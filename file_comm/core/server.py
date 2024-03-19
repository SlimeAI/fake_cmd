import os
import time
import selectors
import subprocess
from subprocess import Popen, TimeoutExpired
from threading import Thread, Event, RLock
from abc import ABCMeta
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.metaclass import (
    Metaclasses,
    _ReadonlyAttrMetaclass,
    InitOnceMetaclass
)
from slime_core.utils.base import BaseList
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Dict,
    Union,
    NOTHING,
    Iterable,
    Missing,
    Literal,
    Callable,
    Any,
    Nothing,
    IO
)
from file_comm.utils.comm import (
    Connection,
    Message,
    create_symbol,
    wait_symbol,
    check_symbol,
    Heartbeat,
    CommandMessage,
    MessageHandler,
    OutputFileHandler
)
from file_comm.utils.file import (
    remove_file_with_retry,
    remove_dir_with_retry
)
from file_comm.utils.parallel import (
    CommandPool,
    LifecycleRun,
    ExitCallbackFunc
)
from file_comm.utils import (
    polling,
    config,
    timestamp_to_str
)
from file_comm.utils.exception import ServerShutdown
from file_comm.utils.logging import logger
from file_comm.utils.system import send_keyboard_interrupt
from . import ServerInfo, SessionInfo, dispatch_action, ActionFunc


class SessionState(ReadonlyAttr):
    
    readonly_attr__ = (
        'destroy_local',
        'unable_to_communicate'
    )
    
    def __init__(self) -> None:
        self.destroy_local = Event()
        # Unable to communicate to client, so in 
        # ``clear_cache``, it will clear the namespace 
        # ignoring whether the client has already 
        # finished reading messages.
        self.unable_to_communicate = Event()


class CommandState(ReadonlyAttr):
    
    readonly_attr__ = (
        'terminate_local',
        'terminate_remote',
        'terminate_disconnect',
        'force_kill',
        'finished',
        'exit',
        'queued',
        'scheduled',
        'scheduled_lock'
    )
    
    def __init__(self) -> None:
        # Terminate from local.
        self.terminate_local = Event()
        # Terminate from remote.
        self.terminate_remote = Event()
        # Terminate because of disconnection.
        self.terminate_disconnect = Event()
        # Force kill.
        self.force_kill = Event()
        # Mark that the command normally finished.
        self.finished = Event()
        # Mark that the command finally exit, whether through itself 
        # or by manual termination (usually because the command is 
        # terminated before being scheduled).
        self.exit = Event()
        # Command is queued. Used only to decide whether to 
        # notify the client that the task is queued.
        self.queued = Event()
        # The command is scheduled. Compared to ``queued``, it 
        # is more safe because of using a ``RLock``.
        self.scheduled = Event()
        self.scheduled_lock = RLock()
    
    @property
    def pending_terminate(self) -> bool:
        return (
            self.terminate_local.is_set() or 
            self.terminate_remote.is_set() or 
            self.terminate_disconnect.is_set() or 
            self.force_kill.is_set()
        )


class Server(
    LifecycleRun,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = (
        'server_info',
        'session_dict',
        'cmd_pool'
    )
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('server_action')
    
    def __init__(
        self,
        address: str,
        max_cmds: Union[int, None] = None
    ) -> None:
        LifecycleRun.__init__(self)
        self.server_info = ServerInfo(address)
        self.session_dict: Dict[str, Session] = {}
        self.cmd_pool = CommandPool(max_cmds)
        self.server_listener: Union[MessageHandler, Nothing] = NOTHING
    
    #
    # Running operations.
    #

    def before_running(self) -> bool:
        listen_fp = self.server_info.server_check_fp
        address = self.server_info.address
        if os.path.exists(listen_fp):
            logger.warning(
                f'server.listen file already exists at {address}, may be another '
                'server is running at the same address. Check the address setting '
                'and if you are sure no other servers are running at the address, '
                f'you may need to manually remove the file: "{listen_fp}".'
            )
            return False
        
        # file initialization
        self.server_info.init_server()
        # Init server listener
        self.server_listener = MessageHandler(self.server_info.server_listen_namespace)
        # NOTE: Open cmd_pool here.
        self.cmd_pool.start()
        logger.info(f'Server initialized. Address {address} created.')
        return True

    def running(self):
        address = self.server_info.address
        
        logger.info(f'Server started. Listening at: {address}')
        for msg in self.server_listener.listen():
            try:
                action = dispatch_action(
                    self.action_registry,
                    msg.type,
                    'Main Server'
                )
                if action is not MISSING:
                    action(self, msg)
            except ServerShutdown:
                return
    
    def after_running(self, *args):
        logger.info('Server shutting down...')
        self.cmd_pool.pool_close.set()
        # Use a new tuple, because the ``exit_callback`` will pop 
        # the dict items.
        sessions = tuple(self.session_dict.values())
        for session in sessions:
            session.session_state.destroy_local.set()
        logger.info('Waiting sessions to destroy...')
        for session in sessions:
            session.join(config.server_shutdown_wait_timeout)
        if any(map(lambda session: session.is_alive(), sessions)):
            logger.warning(
                'Warning: there are still sessions undestroyed. Ignoring and shutdown...'
            )
        else:
            logger.info('Successfully shutdown. Bye.')
        self.server_info.clear_server()
    
    @action_registry(key='new_session')
    def create_new_session(self, msg: Message):
        session_id = msg.session_id
        
        def destroy_session_func(*args):
            """
            Passed to the session to call when destroy.
            """
            self.pop_session_dict(session_id)
        
        session = Session(
            self,
            SessionInfo(self.server_info.address, session_id),
            exit_callbacks=[destroy_session_func]
        )
        if session_id in self.session_dict:
            logger.warning(
                f'Session_id {session_id} already exists and the creation will be ignored.'
            )
            return
        
        self.session_dict[session_id] = session
        session.start()
    
    @action_registry(key='destroy_session')
    def destroy_session(self, msg: Message):
        session_id = msg.content or msg.session_id
        if session_id not in self.session_dict:
            logger.warning(
                f'Session id {session_id} not in the session dict.'
            )
            return
        session = self.session_dict[session_id]
        state = session.session_state
        state.destroy_local.set()
    
    @action_registry(key='server_shutdown')
    def server_shutdown(self, msg: Message):
        logger.info(
            f'Server shutdown received from: "{msg.session_id}".'
        )
        raise ServerShutdown
    
    def pop_session_dict(self, session_id: str):
        return self.session_dict.pop(session_id, None)


class Session(
    LifecycleRun,
    Thread,
    Connection,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    """
    One session can only run one command at the same time.
    """
    readonly_attr__ = (
        'server',
        'session_info',
        'session_state',
        'heartbeat',
        'running_cmd_lock',
        'background_cmds',
        'created_timestamp',
        'session_listener',
        'client_writer'
    )
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('session_action')
    
    def __init__(
        self,
        server: Server,
        session_info: SessionInfo,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ) -> None:
        LifecycleRun.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.server = server
        self.session_info = session_info
        self.session_state = SessionState()
        self.heartbeat = Heartbeat(
            self.session_info.heartbeat_session_fp,
            self.session_info.heartbeat_client_fp
        )
        self.running_cmd: Union[Command, None] = None
        self.running_cmd_lock = RLock()
        self.background_cmds = BackgroundCommandList()
        # Created time.
        self.created_timestamp = time.time()
        # file initialization
        self.session_info.init_session()
        # Message communication
        self.session_listener = MessageHandler(self.session_info.session_queue_namespace)
        self.client_writer = MessageHandler(self.session_info.client_queue_namespace)
        logger.info(f'Session {self.session_id} created.')
    
    @property
    def cmd_pool(self) -> CommandPool:
        return self.server.cmd_pool
    
    @property
    def session_id(self) -> str:
        return self.session_info.session_id
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        return self.connect()
    
    def running(self):
        to_be_destroyed = False
        session_id = self.session_id
        action_registry = self.action_registry
        
        for _ in polling():
            if not to_be_destroyed:
                to_be_destroyed = (not self.check_connection())
            
            msg = self.session_listener.read_one()
            if to_be_destroyed and not msg:
                return
            elif not msg:
                continue
            action = dispatch_action(
                action_registry,
                msg.type,
                f'Server Session {session_id}'
            )
            if action is not MISSING:
                action(self, msg)
    
    def after_running(self, *args):
        # Further confirm the command is terminated.
        self.safely_terminate_cmd(cause='destroy')
        self.clear_cache()
    
    #
    # Actions.
    #
    
    @action_registry(key='cmd')
    def run_new_cmd(self, msg: Message):
        def terminate_command_func(*args):
            self.cmd_pool.cancel(cmd)
            self.reset_running_cmd()
        
        cmd = ShellCommand(
            self,
            msg,
            exit_callbacks=[terminate_command_func]
        )
        self.set_running_cmd(cmd)
        self.cmd_pool.submit(cmd)
    
    @action_registry(key='inner_cmd')
    def run_inner_cmd(self, msg: Message):
        def terminate_command_func(*args):
            self.reset_running_cmd()
        
        cmd = InnerCommand(
            self,
            msg,
            exit_callbacks=[terminate_command_func]
        )
        self.set_running_cmd(cmd)
        cmd.start()
    
    @action_registry(key='terminate_cmd')
    def terminate_cmd(self, msg: Message):
        cmd_id = msg.content
        with self.running_cmd_lock:
            running_cmd = self.running_cmd
            if not running_cmd:
                return
            
            if running_cmd.cmd_id == cmd_id:
                self.safely_terminate_cmd(cause='remote')
            else:
                self.notify_cmd_inconsistency(running_cmd, cmd_id)
    
    @action_registry(key='force_kill_cmd')
    def force_kill_cmd(self, msg: Message):
        cmd_id = msg.content
        with self.running_cmd_lock:
            running_cmd = self.running_cmd
            if not running_cmd:
                return
            
            if running_cmd.cmd_id == cmd_id:
                self.safely_terminate_cmd(cause='force')
            else:
                self.notify_cmd_inconsistency(running_cmd, cmd_id)
    
    @action_registry(key='background_cmd')
    def make_cmd_background(self, msg: Message):
        cmd_id = msg.content
        with self.running_cmd_lock:
            running_cmd = self.running_cmd
            if not running_cmd:
                return
            
            if running_cmd.cmd_id != cmd_id:
                self.notify_cmd_inconsistency(running_cmd, cmd_id)
                return
            self.background_cmds.append(running_cmd)
            self.reset_running_cmd()
    
    def notify_cmd_inconsistency(
        self,
        running_cmd: "Command",
        incoming_cmd_id: str
    ):
        """
        Notify the command id inconsistency.
        """
        self.client_writer.write(
            Message(
                session_id=self.session_id,
                type='info',
                content=(
                    f'Command running inconsistency occurred. '
                    f'Requiring from client: {incoming_cmd_id}. Actual running: '
                    f'{running_cmd.cmd_id}'
                )
            )
        )
    
    #
    # Command operations.
    #
    
    def safely_terminate_cmd(
        self,
        cmd: Union["Command", Missing] = MISSING,
        cause: Literal['remote', 'local', 'destroy', 'disconnect', 'force'] = 'remote'
    ):
        """
        Safely terminate the cmd whether it is running, queued or finished. 
        ``destroy``: called before the session destroyed to further make sure 
        the command is terminated.
        """
        with self.running_cmd_lock:
            if cmd is MISSING:
                cmd = self.running_cmd
            
            if self.running_cmd is not cmd:
                logger.warning(
                    f'Running cmd inconsistency occurred.'
                )
            if not cmd:
                return
            
            cmd_state = cmd.cmd_state
            with cmd_state.scheduled_lock:
                cause_dict = {
                    'remote': cmd_state.terminate_remote,
                    'local': cmd_state.terminate_local,
                    'disconnect': cmd_state.terminate_disconnect,
                    'force': cmd_state.force_kill
                }
                if cause in cause_dict:
                    cause_dict[cause].set()

                # If currently ``scheduled`` is not set, then it will never 
                # be scheduled by the pool (because terminate state is set 
                # under the ``scheduled_lock``).
                scheduled = cmd_state.scheduled.is_set()
            
            if not scheduled:
                # Manually call the exit callbacks and ``after_running``, 
                # because it will never be scheduled.
                cmd.run_exit_callbacks__()
                cmd.after_running()
    
    def set_running_cmd(self, running_cmd: Union["Command", None]) -> bool:
        """
        Return whether the set operation succeeded.
        """
        with self.running_cmd_lock:
            if (
                self.running_cmd and 
                running_cmd
            ):
                # One command is running or has not been terminated, but 
                # another command received.
                self.client_writer.write(
                    Message(
                        session_id=self.session_id,
                        type='info',
                        content=(
                            'Another command is running or has not been '
                            'terminated, inconsistency occurred and the '
                            'incoming command will be ignored.'
                        )
                    )
                )
                return False
            else:
                self.running_cmd = running_cmd
                return True
    
    def reset_running_cmd(self):
        self.set_running_cmd(None)
    
    #
    # Connection operations.
    #
    
    def connect(self) -> bool:
        create_symbol(self.session_info.conn_client_fp)
        if not wait_symbol(
            self.session_info.conn_session_fp
        ):
            logger.warning(
                f'Connection establishment failed. Missing client response. '
                f'Server address: {self.session_info.address}. Session id: {self.session_id}'
            )
            return False
        return True
    
    def disconnect(self, initiator: bool):
        disconn_session_fp = self.session_info.disconn_session_fp
        disconn_confirm_to_session_fp = self.session_info.disconn_confirm_to_session_fp
        disconn_client_fp = self.session_info.disconn_client_fp
        disconn_confirm_to_client_fp = self.session_info.disconn_confirm_to_client_fp
        
        if initiator:
            self.safely_terminate_cmd(cause='disconnect')
            create_symbol(disconn_client_fp)
            if (
                not wait_symbol(disconn_confirm_to_session_fp) or 
                not wait_symbol(disconn_session_fp)
            ):
                logger.warning(
                    'Disconnection from client is not responded, '
                    'ignore and continue...'
                )
                # Set the client is unable to communicate.
                self.session_state.unable_to_communicate.set()
            create_symbol(disconn_confirm_to_client_fp)
        else:
            create_symbol(disconn_confirm_to_client_fp)
            self.safely_terminate_cmd(cause='remote')
            create_symbol(disconn_client_fp)
            if not wait_symbol(disconn_confirm_to_session_fp):
                logger.warning(
                    'Disconnection from client is not responded, '
                    'ignore and continue...'
                )
                # Set the client is unable to communicate.
                self.session_state.unable_to_communicate.set()
        logger.info(
            f'Successfully disconnect session: {self.session_id}'
        )
    
    def check_connection(self) -> bool:
        disconn_server_fp = self.session_info.disconn_session_fp
        session_state = self.session_state
        
        with self.running_cmd_lock:
            if check_symbol(disconn_server_fp):
                self.disconnect(initiator=False)
                return False
            elif (
                session_state.destroy_local.is_set() or 
                not self.heartbeat.beat()
            ):
                self.disconnect(initiator=True)
                return False
        return True
    
    #
    # Other methods.
    #
    
    def clear_cache(self):
        """
        Clear the cached files.
        """
        if self.session_state.unable_to_communicate.is_set():
            self.session_info.clear_session()
            return
        # Wait whether the client has received the final ``disconn_confirm_to_client_fp``
        # when ``self.disconnect(initiator=True)``
        wait_symbol(self.session_info.disconn_confirm_to_client_fp, wait_for_remove=True)
        # Clear anyway.
        self.session_info.clear_session()
    
    def to_str(self) -> str:
        return (
            f'Session(session_id="{self.session_id}", '
            f'created_time={timestamp_to_str(self.created_timestamp)})'
        )


class Command(
    LifecycleRun,
    Thread,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = (
        'session',
        'msg',
        'cmd_state'
    )
    
    def __init__(
        self,
        session: Session,
        msg: Message,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ):
        LifecycleRun.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.session = session
        self.msg = CommandMessage.clone(msg)
        # The running process (if any).
        self.process: Union[Popen, Nothing] = NOTHING
        self.cmd_state = CommandState()
        self.output_writer = OutputFileHandler(self.output_namespace)
    
    @property
    def session_info(self) -> SessionInfo:
        return self.session.session_info
    
    @property
    def cmd_id(self) -> str:
        return self.msg.cmd_id
    
    @property
    def output_namespace(self) -> str:
        return self.session_info.message_output_namespace(self.msg)
    
    @property
    def client_writer(self) -> MessageHandler:
        return self.session.client_writer
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        self.info_running()
        self.cmd_state.queued.clear()
        return True
    
    def after_running(self, __exc_type=None, __exc_value=None, __traceback=None):
        cmd_state = self.cmd_state
        session_info = self.session_info
        # Priority: Remote > Local > Finished
        # Because if terminate remote is set, the client is waiting a 
        # terminate confirm, so its priority should be the first.
        if (
            cmd_state.terminate_remote.is_set() or 
            cmd_state.force_kill.is_set()
        ):
            create_symbol(
                session_info.command_terminate_confirm_fp(self.msg)
            )
        elif cmd_state.terminate_local.is_set():
            self.client_writer.write(
                Message(
                    session_id=session_info.session_id,
                    type='cmd_terminated',
                    content=self.cmd_id
                )
            )
        elif cmd_state.finished.is_set():
            self.client_writer.write(
                Message(
                    session_id=session_info.session_id,
                    type='cmd_finished',
                    content=self.cmd_id
                )
            )
        elif cmd_state.terminate_disconnect.is_set():
            # Doing nothing because the disconnect operations 
            # will automatically notify the client.
            pass
        
        if (
            __exc_type or 
            __exc_value or 
            __traceback
        ):
            self.client_writer.write(
                Message(
                    session_id=self.session_info.session_id,
                    type='info',
                    content=(
                        f'Command terminated with exception: {str(__exc_type)} - '
                        f'{str(__exc_value)}. Command content: {self.msg.content}. '
                        f'Command id: {self.cmd_id}.'
                    )
                )
            )
            self.client_writer.write(
                Message(
                    session_id=self.session_info.session_id,
                    type='cmd_terminated',
                    content=self.cmd_id
                )
            )
        # Mark the command exits.
        cmd_state.exit.set()
    
    #
    # Client info.
    #
    
    def info_running(self):
        """
        Notify the client that the command is running.
        """
        if self.cmd_state.queued.is_set():
            # Notify only when it is queued.
            self.client_writer.write(
                Message(
                    session_id=self.session_info.session_id,
                    type='info',
                    content=f'Command {self.msg.cmd_id} running...'
                )
            )
    
    def info_queued(self):
        """
        Notify the client that the command is queued.
        """
        if self.cmd_state.queued.is_set():
            self.client_writer.write(
                Message(
                    session_id=self.session_info.session_id,
                    type='info',
                    content=f'Command {self.msg.cmd_id} being queued...'
                )
            )
    
    #
    # Other methods.
    #
    
    def to_str(self) -> str:
        return (
            f'Command(cmd="{self.msg.content}", cmd_id="{self.msg.cmd_id}", '
            f'session_id="{self.session_info.session_id}", '
            f'created_time={timestamp_to_str(self.msg.timestamp)}, '
            f'pid={str(self.process.pid)})'
        )


class ShellCommand(Command):
    
    def running(self) -> None:
        msg = self.msg
        state = self.cmd_state
        process = Popen(
            msg.content,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        reader = PipeReader(
            process.stdout,
            config.cmd_pipe_read_timeout
        )
        
        # Set process.
        self.process = process
        for _ in polling(config.cmd_polling_interval):
            # NOTE: DO NOT return after the signal is sent, 
            # and it should always wait until the process 
            # killed.
            if state.force_kill.is_set():
                # Directly kill the process.
                process.kill()
            elif (
                state.terminate_local.is_set() or 
                state.terminate_disconnect.is_set()
            ):
                # Terminate the process.
                process.terminate()
            elif state.terminate_remote.is_set():
                # Reserve a few seconds for ``process.terminate``.
                timeout_for_process_terminate = 5.0
                # Terminate with keyboard interrupt.
                send_keyboard_interrupt(process)
                try:
                    process.wait(
                        config.cmd_terminate_timeout - timeout_for_process_terminate
                    )
                except TimeoutExpired:
                    process.terminate()
            # Write outputs.
            self.output_writer.write(reader.read())
            
            if process.poll() is not None:
                self.output_writer.write(reader.read_all())
                break
        
        if not state.pending_terminate:
            # Normally finished.
            state.finished.set()


class InnerCommand(Command):
    
    inner_cmd_registry = Registry[Callable[[Any], None]]('inner_cmd_registry')
    
    def running(self):
        msg = self.msg
        inner_cmd = msg.content
        action = dispatch_action(
            self.inner_cmd_registry,
            inner_cmd,
            f'Session inner command: {inner_cmd}. Session id: {self.session_info.session_id}'
        )
        if action is not MISSING:
            action(self)
        
        state = self.cmd_state
        if not state.pending_terminate:
            state.finished.set()
    
    @inner_cmd_registry(key='ls-back')
    def list_background(self):
        output_writer = self.output_writer
        background_cmds = tuple(self.session.background_cmds.update_and_get())
        if len(background_cmds) == 0:
            output_writer.print(
                'No background cmds available.'
            )
            return
        output_writer.print('Background commands:')
        for index, cmd in enumerate(background_cmds):
            output_writer.print(
                f'{index}. {cmd.to_str()}'
            )
    
    @inner_cmd_registry(key='ls-session')
    def list_session(self):
        output_writer = self.output_writer
        sessions = tuple(self.session.server.session_dict.values())
        if len(sessions) == 0:
            # This may never be executed.
            output_writer.print('No sessions running.')
            return
        output_writer.print('Sessions:')
        for session in sessions:
            output_writer.print(session.to_str())
    
    @inner_cmd_registry(key='ls-cmd')
    def list_cmd(self):
        output_writer = self.output_writer
        cmd_pool = self.session.cmd_pool
        with cmd_pool.queue_lock, cmd_pool.execute_lock:
            output_writer.print('Executing:')
            if len(cmd_pool.execute) == 0:
                output_writer.print('No executing.')
            else:
                for index, cmd in enumerate(cmd_pool.execute):
                    output_writer.print(
                        f'{index}. {cmd.cmd.to_str()}'
                    )
            output_writer.print('Queued:')
            if len(cmd_pool.queue) == 0:
                output_writer.print('No queued.')
            else:
                for index, cmd in enumerate(cmd_pool.queue):
                    output_writer.print(
                        f'{index}. {cmd.to_str()}'
                    )


class BackgroundCommandList(
    BaseList[Command],
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass, InitOnceMetaclass)
):
    """
    Used to contain the commands that failed to terminate in time.
    """
    readonly_attr__ = ('lock',)
    
    def __init__(self):
        super().__init__()
        # Use a lock to make it safe.
        self.lock = RLock()
    
    def __setitem__(self, __key, __value: Command):
        with self.lock:
            super().__setitem__(__key, __value)
            self.update_command_states__()
    
    def __delitem__(self, __key):
        with self.lock:
            super().__delitem__(__key)
            self.update_command_states__()
    
    def insert(self, __index, __object: Command):
        with self.lock:
            super().insert(__index, __object)
            self.update_command_states__()

    def update_command_states__(self):
        with self.lock:
            self.set_list__(list(filter(
                lambda cmd: not cmd.cmd_state.exit.is_set(),
                self.get_list__()
            )))

    def update_and_get(self) -> "BackgroundCommandList":
        """
        Update command states and return self.
        """
        self.update_command_states__()
        return self


class PipeReader(ReadonlyAttr):
    """
    Non-blocking read stdout pipes within timeout.
    """
    readonly_attr__ = (
        'stdout',
        'selector',
        'timeout'
    )
    
    def __init__(
        self,
        stdout: IO,
        timeout: float
    ) -> None:
        self.stdout = stdout
        # NOTE: Use ``selector`` to get non-blocking outputs.
        self.selector = selectors.DefaultSelector()
        self.selector.register(stdout, selectors.EVENT_READ)
        self.timeout = timeout
    
    def read(self) -> str:
        """
        Read the content within the timeout. This method 
        concat the results of ``_selector_read_one`` within 
        the timeout and return.
        """
        start = time.time()
        content = ''
        while True:
            content += self._selector_read_one()
            stop = time.time()
            if (stop - start) > self.timeout:
                break
        return content
    
    def read_all(self) -> str:
        """
        Read all the content util file end. Make sure the 
        process is end when you call ``read_all``, else the 
        read will be blocked.
        """
        # Non-blocking mode.
        for key, _ in self.selector.select(0):
            if key.fileobj == self.stdout:
                # If ready, read all and return.
                return str(self.stdout.read())
        # If not ready, directly return empty str.
        return ''
    
    def _selector_read_one(self) -> str:
        """
        Read one char if selector is ready.
        """
        # Non-blocking mode.
        for key, _ in self.selector.select(0):
            if key.fileobj == self.stdout:
                # If ready, read one char and return.
                return str(self.stdout.read(1))
        # If no output, directly return empty str.
        return ''
