from subprocess import Popen
from threading import Thread, Event, RLock
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Dict,
    Union,
    NOTHING,
    Iterable
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
        self.destroy = Event()


class CommandState:
    
    def __init__(self) -> None:
        # Terminate from local.
        self.terminate_local = Event()
        # Terminate from remote.
        self.terminate_remote = Event()
        # Finished.
        self.finished = Event()
        # Command is queued.
        self.queued = Event()


def send_message_to_client(
    session_info: SessionInfo,
    *,
    type: str,
    content: str
):
    send_message(
        Message(
            session_id=session_info.session_id,
            target_fp=session_info.client_fp,
            confirm_namespace=session_info.session_namespace,
            type=type,
            content=content
        )
    )


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
        self.command_pool = CommandPool(max_commands)
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
            session.session_state.destroy.set()
        self.command_pool.pool_close.set()
    
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
            self.command_pool,
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
        state.destroy.set()
    
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
        command_pool: CommandPool,
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ) -> None:
        LifecycleRun.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.session_info = session_info
        self.command_pool = command_pool
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
        disconn_server_fp = self.session_info.disconn_server_fp
        action_registry = self.action_registry
        session_state = self.session_state
        
        for _ in polling():
            disconn = check_symbol(disconn_server_fp)
            if (
                session_state.destroy.is_set() or 
                not self.heartbeat.beat() or 
                disconn
            ):
                to_be_destroyed = True
                self.disconnect(initiator=(not disconn))
            
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
        self.destroy()
    
    #
    # Actions.
    #
    
    @action_registry(key='cmd')
    def run_new_cmd(self, msg: Message):
        def terminate_command_func():
            self.command_pool.cancel(cmd)
            self.reset_running_cmd()
        
        cmd = ShellCommand(
            msg,
            self.session_info,
            exit_callbacks=[terminate_command_func]
        )
        self.command_pool.submit(cmd)
    
    @action_registry(key='terminate_cmd')
    def terminate_cmd(self, msg: Message):
        cmd_id = msg.content
        with self.running_cmd_lock:
            running_cmd = self.running_cmd
            if not running_cmd:
                return
            
            if running_cmd.cmd_id == cmd_id:
                running_cmd.command_state.terminate_remote.set()
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
    
    def destroy(self):
        """
        Destroy operations.
        """
        with self.running_cmd_lock:
            running_cmd = self.running_cmd
            if running_cmd:
                command_state = running_cmd.command_state
                # Set the running_cmd to terminate.
                command_state.terminate_local.set()
                if not running_cmd.is_alive():
                    # Manually terminate.
                    running_cmd.terminate()
    
    def set_running_cmd(self, running_cmd: Union["Command", None]):
        with self.running_cmd_lock:
            self.running_cmd = running_cmd
    
    def reset_running_cmd(self):
        self.set_running_cmd(None)
    
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
        disconn_client_fp = self.session_info.disconn_client_fp
        create_symbol(disconn_client_fp)
        if not initiator or wait_symbol(disconn_server_fp):
            print(f'Successfully disconnect session: {self.session_info.session_id}.')
        else:
            print('Session disconnection timeout. Force close.')


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
        self.command_state = CommandState()
    
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
        return True
    
    def after_running(self):
        self.terminate()
    
    def terminate(self):
        self.command_state.terminate_local.set()
    
    #
    # Client info.
    #
    
    def info_running(self):
        """
        Notify the client that the command is running.
        """
        queued = self.command_state.queued
        if queued.is_set():
            # Notify only when it is queued.
            send_message_to_client(
                self.session_info,
                type='info',
                content=f'Command {self.msg.msg_id} running...'
            )
        queued.clear()
    
    def info_queued(self):
        """
        Notify the client that the command is queued.
        """
        if self.command_state.queued.is_set():
            send_message_to_client(
                self.session_info,
                type='info',
                content=f'Command {self.msg.msg_id} being queued...'
            )


class ShellCommand(Command):
    
    def running(self) -> None:
        msg = self.msg
        output_fp = self.output_fp
        state = self.command_state
        
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
                    state.terminate_remote.is_set()
                ):
                    self.process.kill()
                    return


class InnerCommand(Command):
    pass
