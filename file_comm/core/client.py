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
    Connection,
    send_message,
    Message,
    wait_symbol,
    create_symbol,
    pop_message,
    check_symbol,
    remove_symbol,
    Heartbeat
)
from file_comm.utils.file import pop_all, remove_file
from file_comm.utils.exception import CLITerminate
from file_comm.utils import (
    polling
)
from . import ServerFiles, SessionFiles

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
        # CLI terminate indicator.
        self.terminate = Event()


class Client(Connection):
    
    def __init__(self, address: str) -> None:
        self.address = address
        self.session_id = str(uuid.uuid1())
        self.server_files = ServerFiles(address)
        self.session_files = SessionFiles(address, self.session_id)
        self.heartbeat = Heartbeat(
            self.session_files.heartbeat_client_fp,
            self.session_files.heartbeat_server_fp
        )
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
        
        comm = self.client_cli_comm
        disconn_client_fp = self.session_files.disconn_client_fp
        to_be_destroyed = False
        # Start the CLI.
        self.cli.start()
        while True:
            try:
                for _ in polling():
                    disconn = check_symbol(disconn_client_fp)
                    if (
                        not self.heartbeat.beat() or 
                        disconn or 
                        not self.cli.is_alive()
                    ):
                        to_be_destroyed = True
                        self.disconnect(initiator=(not disconn))
                    
                    msg = pop_message(
                        self.session_files.client_fp,
                        self.session_files.session_path
                    )
                    if to_be_destroyed and not msg:
                        self.destroy()
                        # Directly return.
                        return
                    elif not msg:
                        continue
                    # ...
                    # Currently no msg designed.
            except KeyboardInterrupt:
                print('Keyboard Interrupt.')
                if comm.command_running.is_set():
                    comm.command_terminate.set()
            except:
                traceback.print_exc()
    
    def destroy(self):
        """
        Destroy before exit.
        """
        self.client_cli_comm.terminate.set()
        remove_file(self.session_files.client_fp)
    
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
    
    def disconnect(self, initiator: bool):
        disconn_server_fp = self.session_files.disconn_server_fp
        disconn_client_fp = self.session_files.disconn_client_fp
        create_symbol(disconn_server_fp)
        if not initiator or wait_symbol(disconn_client_fp):
            print(f'Successfully disconnect session: {self.session_id}.')
        else:
            print('Session disconnection timeout. Force close.')


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
        comm = self.client_cli_comm
        while True:
            try:
                cmd = input('[fake_cmd] ')
                msg = self.process_cmd(cmd)
                if msg:
                    comm.command_running.set()
                    comm.command_terminate.clear()
                    self.wait_server_cmd(msg)
                    comm.command_running.clear()
                    comm.command_terminate.clear()
                if comm.terminate.is_set():
                    break
            except CLITerminate:
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
    
    def send_server_cmd(self, cmd: str, type: str = 'cmd') -> Union[Message, bool]:
        """
        Send the command to the server.
        """
        msg = Message(
            session_id=self.session_id,
            type=type,
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
        comm = self.client_cli_comm
        
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
            if comm.terminate.is_set():
                break
            
            if comm.command_terminate.is_set():
                print(
                    f'Terminating server command: {msg.content}.'
                )
                # Notify the server to terminate.
                create_symbol(terminate_server_fp)
        # Remove all the files.
        remove_symbol(terminate_client_fp, remove_lockfile=True)
        remove_file(output_fp, remove_lockfile=True)
    
    @cli_registry(key='exit')
    def cli_exit(self, cmd: str):
        raise CLITerminate
