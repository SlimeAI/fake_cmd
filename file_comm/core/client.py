import uuid
from utils.comm import send_message, Message, listen_messages, wait_symbol
from utils.file import wait_file
from . import ServerFiles, SessionFiles


class Client:
    
    def __init__(self, address: str) -> None:
        self.address = address
        self.session_id = str(uuid.uuid1())
        self.server_files = ServerFiles(address)
        self.session_files = SessionFiles(address, self.session_id)
    
    def run(self):
        send_message(
            self.server_files.main_fp,
            Message(type='new_session', session_id=self.session_id)
        )
        if wait_symbol(self.session_files.conn_client):
            return
        send_message(
            self.session_files.server_fp,
            Message(type='cmd', content='ll -a; ls; pwd; echo "hello world"', session_id=self.session_id)
        )
    
    def __del__(self):
        print(self.address)
        print(self.address)
        print('deleted')
