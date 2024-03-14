from subprocess import Popen
from threading import Thread
from multiprocessing import Pool
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Dict,
    Union,
    NOTHING
)
from file_comm.utils.comm import (
    listen_messages,
    Message,
    create_symbol,
    wait_symbol,
    pop_message,
    check_symbol,
    remove_symbol
)
from file_comm.utils.file import (
    LockedTextIO
)
from file_comm.utils import (
    polling
)
from . import ServerFiles, SessionFiles, dispatch_action, ActionFunc, Connection


class Server:
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('server_action')
    
    def __init__(self, address: str, max_processes: Union[int, None] = None) -> None:
        self.address = address
        self.server_files = ServerFiles(address)
        self.session_dict: Dict[str, Session] = {}
        self.pool = Pool(max_processes)
        # file initialization
        self.server_files.create()
        print(f'Server initialized. Address {address} created.')

    def run(self):
        print(f'Server started. Listening at: {self.address}')
        for msg in listen_messages(
            self.server_files.main_fp,
            self.address
        ):
            action = dispatch_action(self.action_registry, msg.type, 'Main Server')
            if action is not MISSING:
                action(self, msg)
    
    @action_registry(key='new_session')
    def create_new_session(self, msg: Message):
        session_id = msg.session_id
        session = Session(self.address, session_id, self)
        if session_id in self.session_dict:
            print(
                f'Warning: session_id {session_id} already exists and the creation will be ignored.'
            )
            return
        
        self.session_dict[session_id] = session
        session.start()
    
    @action_registry(key='destroy_session')
    def destroy_session(self, msg: Message):
        pass


class Session(Thread, Connection):
    """
    One session can only run one command at the same time.
    """
    
    # Dispatch different message types.
    action_registry = Registry[ActionFunc]('session_action')
    
    def __init__(
        self,
        address: str,
        session_id: str,
        server: Server
    ) -> None:
        Thread.__init__(self)
        self.address = address
        self.session_id = session_id
        self.session_files = SessionFiles(address, session_id)
        self.server = server
        self.running_cmd: Union[Command, None] = None
        # file initialization
        self.session_files.create()
        print(f'Session {session_id} created.')
    
    def run(self):
        if not self.connect():
            return
        for _ in polling():
            self.heartbeat()
            msg = pop_message(
                self.session_files.server_fp,
                self.session_files.session_path
            )
            if not msg:
                continue
            action = dispatch_action(
                self.action_registry,
                msg.type,
                f'Server Session {self.session_id}'
            )
            if action is not MISSING:
                action(self, msg)
    
    @action_registry(key='cmd')
    def run_new_cmd(self, msg: Message):
        cmd = Command(msg, self.session_files)
        cmd.start()
    
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
    
    def disconnect(self):
        return super().disconnect()
    
    def heartbeat(self):
        return super().heartbeat()


class Command(Thread):
    
    def __init__(
        self,
        msg: Message,
        session_files: SessionFiles
    ):
        Thread.__init__(self)
        self.msg = msg
        self.session_files = session_files
        self.process = NOTHING
    
    def run(self) -> None:
        msg = self.msg
        output_fp = self.session_files.message_output_fp(msg)
        terminate_server_fp = self.session_files.command_terminate_server_fp(msg)
        terminate_client_fp = self.session_files.command_terminate_client_fp(msg)
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
                if check_symbol(terminate_server_fp, remove_lockfile=True):
                    self.process.kill()
            create_symbol(
                terminate_client_fp
            )

        # Remove symbol again to ensure it is removed.
        remove_symbol(terminate_server_fp, remove_lockfile=True)
        # TODO: callback
