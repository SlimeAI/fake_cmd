from subprocess import Popen
from threading import Thread, Event, RLock
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Dict,
    Union,
    NOTHING,
    Iterable,
    Missing,
    Literal
)
from file_comm.utils.comm import (
    Connection,
    listen_messages,
    Message,
    create_symbol,
    wait_symbol,
    pop_message,
    check_symbol,
    Heartbeat,
    send_message
)
from file_comm.utils.file import (
    LockedTextIO
)
from file_comm.utils.parallel import (
    CommandPool,
    LifecycleRun,
    ExitCallbackFunc
)
from file_comm.utils import (
    polling
)
from . import ServerInfo, SessionInfo, dispatch_action, ActionFunc


class SessionState:
    
    def __init__(self) -> None:
        self.destroy_local = Event()


class CommandState:
    
    def __init__(self) -> None:
        # Terminate from local.
        self.terminate_local = Event()
        # Terminate from remote.
        self.terminate_remote = Event()
        # Terminate because of disconnection.
        self.terminate_disconnect = Event()
        # Finished.
        self.finished = Event()
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
            self.terminate_disconnect.is_set()
        )


def send_message_to_client(
    session_info: SessionInfo,
    *,
    type: str,
    content: Union[str, None] = None
):
    msg = Message(
        session_id=session_info.session_id,
        target_fp=session_info.client_fp,
        confirm_namespace=session_info.session_namespace,
        type=type,
        content=content
    )
    return (msg, send_message(msg))


class Server(LifecycleRun):
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('server_action')
    
    def __init__(
        self,
        address: str,
        max_commands: Union[int, None] = None
    ) -> None:
        LifecycleRun.__init__(self)
        self.server_info = ServerInfo(address)
        self.session_dict: Dict[str, Session] = {}
        self.cmd_pool = CommandPool(max_commands)
        # file initialization
        self.server_info.init_server()
        print(f'Server initialized. Address {address} created.')

    #
    # Running operations.
    #

    def before_running(self) -> bool:
        return True

    def running(self):
        address = self.server_info.address
        
        print(f'Server started. Listening at: {address}')
        for msg in listen_messages(
            self.server_info.main_fp
        ):
            action = dispatch_action(self.action_registry, msg.type, 'Main Server')
            if action is not MISSING:
                action(self, msg)
    
    def after_running(self):
        print('Server shutting down...')
        for session in self.session_dict.values():
            session.session_state.destroy_local.set()
        self.cmd_pool.pool_close.set()
    
    @action_registry(key='new_session')
    def create_new_session(self, msg: Message):
        session_id = msg.session_id
        
        def destroy_session_func():
            """
            Passed to the session to call when destroy.
            """
            self.pop_session_dict(session_id)
        
        session = Session(
            SessionInfo(self.server_info.address, session_id),
            self.cmd_pool,
            exit_callbacks=[destroy_session_func]
        )
        if session_id in self.session_dict:
            print(
                f'Warning: session_id {session_id} already exists and the creation will be ignored.'
            )
            return
        
        self.session_dict[session_id] = session
        session.start()
    
    @action_registry(key='destroy_session')
    def destroy_session(self, msg: Message):
        session_id = msg.content or msg.session_id
        if session_id not in self.session_dict:
            print(
                f'Warning: Session id {session_id} not in the session dict.'
            )
            return
        session = self.session_dict[session_id]
        state = session.session_state
        state.destroy_local.set()
    
    def pop_session_dict(self, session_id: str):
        return self.session_dict.pop(session_id, None)


class Session(LifecycleRun, Thread, Connection):
    """
    One session can only run one command at the same time.
    """
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('session_action')
    
    def __init__(
        self,
        session_info: SessionInfo,
        cmd_pool: CommandPool,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ) -> None:
        LifecycleRun.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.session_info = session_info
        self.cmd_pool = cmd_pool
        self.session_state = SessionState()
        self.heartbeat = Heartbeat(
            self.session_info.heartbeat_server_fp,
            self.session_info.heartbeat_client_fp
        )
        self.running_cmd: Union[Command, None] = None
        self.running_cmd_lock = RLock()
        
        # file initialization
        self.session_info.init_session()
        print(f'Session {self.session_info.session_id} created.')
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        return self.connect()
    
    def running(self):
        to_be_destroyed = False
        server_fp = self.session_info.server_fp
        session_id = self.session_info.session_id
        action_registry = self.action_registry
        
        for _ in polling():
            if not to_be_destroyed:
                to_be_destroyed = (not self.check_connection())
            
            msg = pop_message(server_fp)
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
    
    def after_running(self):
        # Further confirm the command is terminated.
        self.safely_terminate_cmd(cause='destroy')
    
    #
    # Actions.
    #
    
    @action_registry(key='cmd')
    def run_new_cmd(self, msg: Message):
        def terminate_command_func():
            self.cmd_pool.cancel(cmd)
            self.reset_running_cmd()
        
        cmd = ShellCommand(
            msg,
            self.session_info,
            exit_callbacks=[terminate_command_func]
        )
        self.cmd_pool.submit(cmd)
        with self.running_cmd_lock:
            if self.running_cmd:
                # TODO: inconsistency occurred.
                send_message_to_client(
                    self.session_info,
                    type='info',
                    content='Another command is running.'
                )
            else:
                self.running_cmd = cmd
    
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
                send_message_to_client(
                    self.session_info,
                    type='info',
                    content=(
                        f'Command running inconsistency occurred. '
                        f'Requiring from client: {cmd_id}. Actual running: '
                        f'{running_cmd.cmd_id}'
                    )
                )
    
    def safely_terminate_cmd(
        self,
        cmd: Union["Command", Missing] = MISSING,
        cause: Literal['remote', 'local', 'destroy', 'disconnect'] = 'remote'
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
                print(
                    f'Warning: Running cmd inconsistency occurred.'
                )
            if not cmd:
                return
            
            with cmd.cmd_state.scheduled_lock:
                if cause == 'remote':
                    cmd.cmd_state.terminate_remote.set()
                elif cause == 'local':
                    cmd.cmd_state.terminate_local.set()
                elif cause == 'disconnect':
                    cmd.cmd_state.terminate_disconnect.set()

                # If currently ``scheduled`` is not set, then it will never 
                # be scheduled by the pool (because terminate state is set 
                # under the ``scheduled_lock``).
                scheduled = cmd.cmd_state.scheduled.is_set()
            
            if not scheduled:
                # Manually call the exit callbacks and ``after_running``, 
                # because it will never be scheduled.
                cmd.run_exit_callbacks__()
                cmd.after_running()
    
    def set_running_cmd(self, running_cmd: Union["Command", None]):
        with self.running_cmd_lock:
            self.running_cmd = running_cmd
    
    def reset_running_cmd(self):
        self.set_running_cmd(None)
    
    def clear_cache(self):
        """
        Clear the cached files.
        """
        pass
    
    #
    # Connection operations.
    #
    
    def connect(self) -> bool:
        create_symbol(self.session_info.conn_client_fp)
        if not wait_symbol(
            self.session_info.conn_server_fp,
            remove_lockfile=True
        ):
            print(
                f'Warning: connection establishment failed. Missing client response. '
                f'Server address: {self.session_info.address}. Session id: {self.session_info.session_id}'
            )
            return False
        return True
    
    def disconnect(self, initiator: bool):
        disconn_server_fp = self.session_info.disconn_server_fp
        disconn_confirm_to_server_fp = self.session_info.disconn_confirm_to_server_fp
        disconn_client_fp = self.session_info.disconn_client_fp
        disconn_confirm_to_client_fp = self.session_info.disconn_confirm_to_client_fp
        
        if initiator:
            self.safely_terminate_cmd(cause='disconnect')
            create_symbol(disconn_client_fp)
            if (
                not wait_symbol(disconn_confirm_to_server_fp) or 
                not wait_symbol(disconn_server_fp)
            ):
                print(
                    'Warning: disconnection from client is not responded, '
                    'ignore and continue...'
                )
            create_symbol(disconn_confirm_to_client_fp)
        else:
            create_symbol(disconn_confirm_to_client_fp)
            self.safely_terminate_cmd(cause='remote')
            create_symbol(disconn_client_fp)
            if not wait_symbol(disconn_confirm_to_server_fp):
                print(
                    'Warning: disconnection from client is not responded, '
                    'ignore and continue...'
                )
        print(
            f'Successfully disconnect session: {self.session_info.session_id}'
        )
    
    def check_connection(self) -> bool:
        disconn_server_fp = self.session_info.disconn_server_fp
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


class Command(LifecycleRun, Thread):
    
    def __init__(
        self,
        msg: Message,
        session_info: SessionInfo,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ):
        LifecycleRun.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.msg = msg
        self.session_info = session_info
        self.process = NOTHING
        self.cmd_state = CommandState()
    
    @property
    def cmd_id(self) -> str:
        return self.msg.msg_id
    
    @property
    def output_fp(self) -> str:
        return self.session_info.message_output_fp(self.msg)
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        self.info_running()
        self.cmd_state.queued.clear()
        return True
    
    def after_running(self):
        cmd_state = self.cmd_state
        session_info = self.session_info
        # Priority: Remote > Local > Finished
        # Because if terminate remote is set, the client is waiting a 
        # terminate confirm, so its priority should be the first.
        if cmd_state.terminate_remote.is_set():
            create_symbol(
                self.session_info.command_terminate_confirm_fp(self.msg)
            )
        elif cmd_state.terminate_local.is_set():
            send_message_to_client(
                session_info,
                type='cmd_terminated',
                content=self.cmd_id
            )
        elif cmd_state.finished.is_set():
            send_message_to_client(
                session_info,
                type='cmd_finished',
                content=self.cmd_id
            )
        elif cmd_state.terminate_disconnect.is_set():
            # Doing nothing because the disconnect operations 
            # will automatically notify the client.
            pass
    
    #
    # Client info.
    #
    
    def info_running(self):
        """
        Notify the client that the command is running.
        """
        if self.cmd_state.queued.is_set():
            # Notify only when it is queued.
            send_message_to_client(
                self.session_info,
                type='info',
                content=f'Command {self.msg.msg_id} running...'
            )
    
    def info_queued(self):
        """
        Notify the client that the command is queued.
        """
        if self.cmd_state.queued.is_set():
            send_message_to_client(
                self.session_info,
                type='info',
                content=f'Command {self.msg.msg_id} being queued...'
            )


class ShellCommand(Command):
    
    def running(self) -> None:
        msg = self.msg
        output_fp = self.output_fp
        state = self.cmd_state
        
        with LockedTextIO(
            open(output_fp, 'a'), output_fp
        ) as output_f:
            self.process = Popen(
                msg.content,
                shell=True,
                stdout=output_f,
                stderr=output_f
            )
            while self.process.poll() is None:
                if (
                    state.terminate_local.is_set() or 
                    state.terminate_remote.is_set() or 
                    state.terminate_disconnect.is_set()
                ):
                    self.process.kill()
                    return
        # Normally finished.
        state.finished.set()


class InnerCommand(Command):
    pass
