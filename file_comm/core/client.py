import sys
import uuid
import traceback
from threading import Thread, Event
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Union
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
from file_comm.utils.parallel import (
    LifecycleRun
)
from file_comm.utils import (
    polling
)
from . import SessionInfo, ActionFunc, dispatch_action

CLIENT_NOTE = """
NOTE: enter ``exit`` to shutdown the client.
NOTE: Use ``Ctrl+C`` AND ``Ctrl+D`` to start a new line.
"""


class State:
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


class Client(LifecycleRun, Connection):
    
    client_registry = Registry[ActionFunc]('client_registry')
    
    def __init__(self, address: str) -> None:
        LifecycleRun.__init__(self)
        # Create a new session id.
        session_id = str(uuid.uuid1())
        self.session_info = SessionInfo(address, session_id)
        self.heartbeat = Heartbeat(
            self.session_info.heartbeat_client_fp,
            self.session_info.heartbeat_server_fp
        )
        self.state = State()
        self.cli = CLI(
            self.session_info,
            self.state
        )
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        return self.connect()
    
    def running(self):
        address = self.session_info.address
        session_id = self.session_info.session_id
        
        print(f'Connect with server: {address}. Session id: {session_id}.')
        print(CLIENT_NOTE.strip('\n'))
        
        state = self.state
        disconn_client_fp = self.session_info.disconn_client_fp
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
                    
                    msg = pop_message(self.session_info.client_fp)
                    if to_be_destroyed and not msg:
                        # Directly return.
                        return
                    elif not msg:
                        continue
                    action = dispatch_action(
                        self.client_registry,
                        msg.type,
                        f'Client'
                    )
                    if action is not MISSING:
                        action(self, msg)
            except KeyboardInterrupt:
                print('Keyboard Interrupt.')
                if state.command_running.is_set():
                    state.command_terminate.set()
            except:
                traceback.print_exc()
    
    def after_running(self):
        self.destroy()
    
    @client_registry(key='info')
    def process_info(self, msg: Message):
        print(msg.content)
    
    def destroy(self):
        """
        Destroy before exit.
        """
        self.state.terminate.set()
        remove_file(self.session_info.client_fp, remove_lockfile=True)
    
    def connect(self) -> bool:
        address = self.session_info.address
        session_id = self.session_info.session_id
        
        if not send_message(
            Message(
                session_id=session_id,
                target_fp=self.session_info.main_fp,
                confirm_namespace=address,
                type='new_session'
            )
        ):
            return False
        if not wait_symbol(
            self.session_info.conn_client_fp,
            remove_lockfile=True
        ):
            print(
                f'Warning: server connection establishment failed. Server address: {address}.'
                f'Session id: {session_id}.'
            )
            return False
        create_symbol(self.session_info.conn_server_fp)
        return True
    
    def disconnect(self, initiator: bool):
        disconn_server_fp = self.session_info.disconn_server_fp
        disconn_client_fp = self.session_info.disconn_client_fp
        create_symbol(disconn_server_fp)
        if not initiator or wait_symbol(disconn_client_fp):
            print(f'Successfully disconnect session: {self.session_info.session_id}.')
        else:
            print('Session disconnection timeout. Force close.')


class CLI(LifecycleRun, Thread):
    
    cli_registry = Registry[ActionFunc]('cli_registry')
    
    def __init__(
        self,
        session_info: SessionInfo,
        state: State
    ):
        LifecycleRun.__init__(self)
        Thread.__init__(self)
        self.session_info = session_info
        self.state = state
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        return True
    
    def running(self):
        state = self.state
        while True:
            try:
                cmd = input('[fake_cmd] ')
                msg = self.process_cmd(cmd)
                if msg:
                    state.command_running.set()
                    state.command_terminate.clear()
                    self.wait_server_cmd(msg)
                    state.command_running.clear()
                    state.command_terminate.clear()
            except CLITerminate:
                return
            except EOFError:
                pass
            except:
                traceback.print_exc()
            
            if state.terminate.is_set():
                return
    
    def after_running(self):
        return
    
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
        session_id = self.session_info.session_id
        msg = Message(
            session_id=session_id,
            target_fp=self.session_info.server_fp,
            confirm_namespace=self.session_info.session_namespace,
            type=type,
            content=cmd
        )
        res = send_message(msg)
        if not res:
            return False
        return msg
    
    def wait_server_cmd(self, msg: Message):
        """
        Wait the server command to finish.
        """
        output_fp = self.session_info.message_output_fp(msg)
        terminate_server_fp = self.session_info.command_terminate_server_fp(msg)
        terminate_client_fp = self.session_info.command_terminate_client_fp(msg)
        state = self.state
        
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
            if state.terminate.is_set():
                break
            
            if state.command_terminate.is_set():
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
