from subprocess import Popen
from threading import Thread
from multiprocessing import Pool
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Dict,
    Union
)
from utils.comm import listen_messages, Message, create_symbol
from . import ServerFiles, SessionFiles, dispatch_action, ActionFunc


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
        for msg in listen_messages(self.server_files.main_fp):
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


class Session(Thread):
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
        self.running_cmd: Union[Popen, None] = None
        # file initialization
        self.session_files.create()
        print(f'Session {session_id} created.')
    
    def run(self):
        create_symbol(self.session_files.conn_client)
        for msg in listen_messages(self.session_files.server_fp):
            action = dispatch_action(
                self.action_registry,
                msg.type,
                f'Server Session {self.session_id}'
            )
            if action is not MISSING:
                action(self, msg)
    
    @action_registry(key='cmd')
    def run_new_cmd(self, msg: Message):
        # self.server.pool.apply_async(
        #     execute_cmd,
        #     args=(msg.content, self.session_files.output_fp, self.session_files.output_fp),
        #     callback=self.cmd_callback,
        #     error_callback=self.cmd_error_callback
        # )
        Popen(msg.content, shell=True, stdout=open(self.session_files.output_fp, 'a'), stderr=open(self.session_files.output_fp, 'a'))
    
    def cmd_callback(self, res_code: int):
        self.running_cmd = None
        print('cmd finished.')
    
    def cmd_error_callback(self, exception: BaseException):
        print('exception occurred', exception)
