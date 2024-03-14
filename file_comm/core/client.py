import sys
import uuid
import traceback
from threading import Thread, Event
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    Callable,
    MISSING,
    Union,
    Any
)
from file_comm.utils.comm import (
    send_message,
    Message,
    wait_symbol,
    create_symbol,
    pop_message,
    check_symbol,
    remove_symbol
)
from file_comm.utils.file import pop_all, remove_file
from file_comm.utils.exception import ClientShutdown
from file_comm.utils import (
    polling
)
from . import ServerFiles, SessionFiles, Connection

CLIENT_NOTE = """
NOTE: enter ``exit`` to shutdown the client.
NOTE: Use ``Ctrl+C`` AND ``Ctrl+D`` to start a new line.
"""


class ClientCLIComm:
    """
    Communication items between client and cli.
    """
    def __init__(self) -> None:
        # Indicator that whether a command is running.
        self.command_running = Event()
        # Keyboard Interrupt from the main Thread.
        self.command_terminate = Event()


class Client(Connection):
    
    def __init__(self, address: str) -> None:
        self.address = address
        self.session_id = str(uuid.uuid1())
        self.server_files = ServerFiles(address)
        self.session_files = SessionFiles(address, self.session_id)
        self.client_cli_comm = ClientCLIComm()
        self.cli = CLI(
            self.session_id,
            self.server_files,
            self.session_files,
            self.client_cli_comm
        )
    
    def run(self):
        if not self.connect():
            return
        print(f'Connect with server: {self.address}. Session id: {self.session_id}.')
        print(CLIENT_NOTE.strip('\n'))
        # Start the CLI.
        self.cli.start()
        while True:
            try:
                for _ in polling():
                    self.heartbeat()
                    msg = pop_message(
                        self.session_files.client_fp,
                        self.session_files.session_path
                    )
                    if msg:
                        # Currently no msg designed.
                        pass
                    if not self.cli.is_alive():
                        print(f'Client exit.')
                        return
            except ClientShutdown:
                return
            except KeyboardInterrupt:
                print('Keyboard Interrupt.')
                comm = self.client_cli_comm
                if comm.command_running.is_set():
                    comm.command_terminate.set()
            except:
                traceback.print_exc()
    
    def __del__(self):
        print(self.address)
        print('deleted')
    
    def connect(self) -> bool:
        if not send_message(
            self.server_files.main_fp,
            Message(type='new_session', session_id=self.session_id),
            self.address
        ):
            return False
        if not wait_symbol(
            self.session_files.conn_client_fp,
            remove_lockfile=True
        ):
            print(
                f'Warning: server connection establishment failed. Server address: {self.address}.'
                f'Session id: {self.session_id}.'
            )
            return False
        create_symbol(self.session_files.conn_server_fp)
        return True
    
    def disconnect(self):
        return super().disconnect()
    
    def heartbeat(self):
        return super().heartbeat()


class CLI(Thread):
    
    cli_registry = Registry[Callable[[Any, str], None]]('cli_registry')
    
    def __init__(
        self,
        session_id: str,
        server_files: ServerFiles,
        session_files: SessionFiles,
        client_cli_comm: ClientCLIComm
    ):
        Thread.__init__(self)
        self.session_id = session_id
        self.server_files = server_files
        self.session_files = session_files
        self.client_cli_comm = client_cli_comm
    
    def run(self):
        while True:
            try:
                cmd = input('[fake_cmd] ')
                msg = self.process_cmd(cmd)
                if msg:
                    comm = self.client_cli_comm
                    comm.command_running.set()
                    comm.command_terminate.clear()
                    self.wait_server_cmd(msg)
                    comm.command_running.clear()
                    comm.command_terminate.clear()
            except ClientShutdown:
                return
            except EOFError:
                pass
            except:
                traceback.print_exc()
    
    def process_cmd(self, cmd: str) -> Union[Message, bool]:
        """
        Process the input cmd. Return a ``Message`` object if the cmd 
        should be executed in the server and it is successfully received, 
        else return ``False``.
        """
        if not cmd:
            return False
        
        cli_func = self.cli_registry.get(cmd, MISSING)
        if cli_func is MISSING:
            return self.send_server_cmd(cmd)
        else:
            cli_func(self, cmd)
            return False
    
    def send_server_cmd(self, cmd: str) -> Union[Message, bool]:
        """
        Send the command to the server.
        """
        msg = Message(
            session_id=self.session_id,
            type='cmd',
            content=cmd
        )
        res = send_message(
            self.session_files.server_fp,
            msg,
            self.session_files.session_path
        )
        if not res:
            return False
        return msg
    
    def wait_server_cmd(self, msg: Message):
        """
        Wait the server command to finish.
        """
        output_fp = self.session_files.message_output_fp(msg)
        terminate_server_fp = self.session_files.command_terminate_server_fp(msg)
        terminate_client_fp = self.session_files.command_terminate_client_fp(msg)
        
        def redirect_output():
            """
            Redirect the content of ``output_fp`` to the cli.
            """
            content = pop_all(output_fp)
            if not content:
                return
            sys.stderr.write(content)
            sys.stderr.flush()
        
        for _ in polling():
            if check_symbol(
                terminate_client_fp,
                remove_lockfile=True
            ):
                # Clear the output before terminate.
                redirect_output()
                break
            
            redirect_output()
            if self.client_cli_comm.command_terminate.is_set():
                print(
                    f'Terminating server command: {msg.content}. '
                )
                # Notify the server to terminate.
                create_symbol(terminate_server_fp)
        # Remove all the files.
        remove_symbol(terminate_client_fp, remove_lockfile=True)
        remove_file(output_fp, remove_lockfile=True)
    
    @cli_registry(key='exit')
    def cli_exit(self, cmd: str):
        raise ClientShutdown
