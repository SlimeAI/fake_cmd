from subprocess import Popen
from threading import Thread, Event, RLock
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Dict,
    Union,
    NOTHING,
    Callable
)
from file_comm.utils.comm import (
    Connection,
    listen_messages,
    Message,
    create_symbol,
    wait_symbol,
    pop_message,
    check_symbol,
    remove_symbol,
    Heartbeat,
    send_message
)
from file_comm.utils.file import (
    LockedTextIO
)
from file_comm.utils.parallel import (
    CommandPool,
    LifecycleRun
)
from file_comm.utils import (
    polling
)
from . import ServerFiles, SessionFiles, dispatch_action, ActionFunc


class ServerSessionComm:
    
    def __init__(self) -> None:
        self.destroy = Event()


class SessionCommandComm:
    
    def __init__(self) -> None:
        self.terminate = Event()


class Server(LifecycleRun):
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('server_action')
    
    def __init__(
        self,
        address: str,
        max_commands: Union[int, None] = None
    ) -> None:
        self.address = address
        self.server_files = ServerFiles(address)
        self.session_dict: Dict[str, Session] = {}
        self.command_pool = CommandPool(max_commands)
        # file initialization
        self.server_files.create()
        print(f'Server initialized. Address {address} created.')

    #
    # Running operations.
    #

    def before_running(self) -> bool:
        return True

    def running(self):
        print(f'Server started. Listening at: {self.address}')
        for msg in listen_messages(
            self.server_files.main_fp,
            self.address
        ):
            action = dispatch_action(self.action_registry, msg.type, 'Main Server')
            if action is not MISSING:
                action(self, msg)
    
    def after_running(self):
        print('Server shutting down...')
        self.command_pool.pool_close.set()
        for session in self.session_dict.values():
            session.server_session_comm.destroy.set()
    
    @action_registry(key='new_session')
    def create_new_session(self, msg: Message):
        session_id = msg.session_id
        
        def destroy_session_func():
            """
            Passed to the session to call when destroy.
            """
            self.pop_session_dict(session_id)
        
        session = Session(
            self.address,
            session_id,
            destroy_session_func,
            self.command_pool.submit,
            self.command_pool.cancel
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
        comm = session.server_session_comm
        comm.destroy.set()
    
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
        address: str,
        session_id: str,
        destroy_session_func: Callable[[], None],
        submit_func: Callable[["Command"], None],
        cancel_func: Callable[["Command"], bool]
    ) -> None:
        Thread.__init__(self)
        self.address = address
        self.session_id = session_id
        self.session_files = SessionFiles(address, session_id)
        self.destroy_session_func = destroy_session_func
        self.server_session_comm = ServerSessionComm()
        self.heartbeat = Heartbeat(
            self.session_files.heartbeat_server_fp,
            self.session_files.heartbeat_client_fp
        )
        self.submit_func = submit_func
        self.cancel_func = cancel_func
        self.running_cmd: Union[Command, None] = None
        self.running_cmd_lock = RLock()
        # file initialization
        self.session_files.create()
        print(f'Session {session_id} created.')
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        return self.connect()
    
    def running(self):
        to_be_destroyed = False
        server_fp = self.session_files.server_fp
        session_path = self.session_files.session_path
        session_id = self.session_id
        disconn_server_fp = self.session_files.disconn_server_fp
        action_registry = self.action_registry
        comm_server = self.server_session_comm
        
        for _ in polling():
            disconn = check_symbol(disconn_server_fp)
            if (
                comm_server.destroy.is_set() or 
                not self.heartbeat.beat() or 
                disconn
            ):
                to_be_destroyed = True
                self.disconnect(initiator=(not disconn))
            
            msg = pop_message(
                server_fp,
                session_path
            )
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
    
    @action_registry(key='cmd')
    def run_new_cmd(self, msg: Message):
        def terminate_command_func():
            self.cancel_func(cmd)
            self.reset_running_cmd()
        
        cmd = Command(
            msg,
            self.session_id,
            self.session_files,
            terminate_command_func
        )
        self.submit_func(cmd)
    
    def destroy(self):
        """
        Destroy operations.
        """
        with self.running_cmd_lock:
            running_cmd = self.running_cmd
            if running_cmd:
                comm_command = running_cmd.session_command_comm
                # Set the running_cmd to terminate.
                comm_command.terminate.set()
                if not running_cmd.is_alive():
                    # Manually terminate.
                    running_cmd.terminate()
        # Destroy
        self.destroy_session_func()
    
    def set_running_cmd(self, running_cmd: Union["Command", None]):
        with self.running_cmd_lock:
            self.running_cmd = running_cmd
    
    def reset_running_cmd(self):
        self.set_running_cmd(None)
    
    def connect(self) -> bool:
        create_symbol(self.session_files.conn_client_fp)
        if not wait_symbol(
            self.session_files.conn_server_fp,
            remove_lockfile=True
        ):
            print(
                f'Warning: connection establishment failed. Missing client response. '
                f'Server address: {self.address}. Session id: {self.session_id}'
            )
            return False
        return True
    
    def disconnect(self, initiator: bool):
        disconn_server_fp = self.session_files.disconn_server_fp
        disconn_client_fp = self.session_files.disconn_client_fp
        create_symbol(disconn_client_fp)
        if not initiator or wait_symbol(disconn_server_fp):
            print(f'Successfully disconnect session: {self.session_id}.')
        else:
            print('Session disconnection timeout. Force close.')


class Command(LifecycleRun, Thread):
    
    def __init__(
        self,
        msg: Message,
        session_id: str,
        session_files: SessionFiles,
        terminate_command_func: Callable[[], None]
    ):
        Thread.__init__(self)
        self.msg = msg
        self.session_id = session_id
        self.session_files = session_files
        self.process = NOTHING
        self.session_command_comm = SessionCommandComm()
        self.terminate_command_func = terminate_command_func
        self.queued = Event()
    
    @property
    def output_fp(self) -> str:
        return self.session_files.message_output_fp(self.msg)
    
    @property
    def terminate_client_fp(self) -> str:
        return self.session_files.command_terminate_client_fp(self.msg)
    
    @property
    def terminate_server_fp(self) -> str:
        return self.session_files.command_terminate_server_fp(self.msg)
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        self.info_running()
        return True
    
    def running(self) -> None:
        msg = self.msg
        output_fp = self.output_fp
        terminate_server_fp = self.terminate_server_fp
        comm = self.session_command_comm
        
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
                    check_symbol(terminate_server_fp, remove_lockfile=True) or 
                    comm.terminate.is_set()
                ):
                    self.process.kill()
                    return
    
    def after_running(self):
        self.terminate()
    
    def terminate(self):
        self.session_command_comm.terminate.set()
        self.terminate_command_func()
        create_symbol(
            self.terminate_client_fp
        )
        # Remove symbol again to ensure it is removed.
        remove_symbol(self.terminate_server_fp, remove_lockfile=True)
    
    def info_running(self):
        """
        Notify the client that the command is running.
        """
        if self.queued.is_set():
            # Notify only when it is queued.
            send_message(
                self.session_files.client_fp,
                Message(
                    session_id=self.session_id,
                    type='info',
                    content=f'Command {self.msg.msg_id} running...'
                ),
                self.session_files.session_path
            )
        self.queued.clear()
    
    def info_queued(self):
        """
        Notify the client that the command is queued.
        """
        send_message(
            self.session_files.client_fp,
            Message(
                session_id=self.session_id,
                type='info',
                content=f'Command {self.msg.msg_id} being queued...'
            ),
            self.session_files.session_path
        )


class InnerCommand(Command):
    pass
