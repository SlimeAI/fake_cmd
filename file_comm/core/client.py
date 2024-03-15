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
        self.cmd_running = Event()
        # Command terminate from local.
        self.cmd_terminate_local = Event()
        # Command terminated from remote.
        self.cmd_terminate_remote = Event()
        # Command finished.
        self.cmd_finished = Event()
        # CLI terminate indicator.
        self.terminate = Event()
    
    def reset(self) -> None:
        self.cmd_running.clear()
        self.cmd_terminate_local.clear()
        self.cmd_terminate_remote.clear()
        self.cmd_finished.clear()


def send_message_to_server(
    session_info: SessionInfo,
    *,
    type: str,
    content: Union[str, None] = None
):
    msg = Message(
        session_id=session_info.session_id,
        target_fp=session_info.server_fp,
        confirm_namespace=session_info.session_namespace,
        type=type,
        content=content
    )
    return (msg, send_message(msg))


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
        to_be_destroyed = False
        # Start the CLI.
        self.cli.start()
        while True:
            try:
                for _ in polling():
                    if not to_be_destroyed:
                        to_be_destroyed = (not self.check_connection())
                    
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
                if state.cmd_running.is_set():
                    state.cmd_terminate_local.set()
            except:
                traceback.print_exc()
    
    def after_running(self):
        remove_file(self.session_info.client_fp, remove_lockfile=True)
    
    @client_registry(key='info')
    def process_info(self, msg: Message):
        print(msg.content)
    
    @client_registry(key='cmd_finished')
    def process_cmd_finished(self, msg: Message):
        self.state.cmd_finished.set()
    
    @client_registry(key='cmd_terminated')
    def process_cmd_terminated(self, msg: Message):
        self.state.cmd_terminate_remote.set()
    
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
    
    def check_connection(self) -> bool:
        disconn_client_fp = self.session_info.disconn_client_fp
        # Disconnection from remote.
        if check_symbol(disconn_client_fp):
            self.disconnect(initiator=False)
            self.state.cmd_terminate_remote.set()
            self.state.terminate.set()
            return False
        elif (
            not self.heartbeat.beat() or 
            not self.cli.is_alive()
        ):
            self.disconnect(initiator=True)
            self.state.cmd_terminate_local.set()
            self.state.terminate.set()
            return False
        return True


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
                    state.reset()
                    state.cmd_running.set()
                    self.wait_server_cmd(msg)
                    state.reset()
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
        msg, res = send_message_to_server(
            self.session_info,
            type=type,
            content=cmd
        )
        if not res:
            return False
        return msg
    
    def wait_server_cmd(self, msg: Message):
        """
        Wait the server command to finish.
        """
        output_fp = self.session_info.message_output_fp(msg)
        state = self.state
        
        to_be_terminated = False
        for _ in polling():
            if (
                state.cmd_terminate_remote.is_set() or 
                state.terminate.is_set() or 
                state.cmd_finished.is_set()
            ):
                # Clear the output before terminate.
                self.redirect_output(output_fp)
                break
            
            has_output = self.redirect_output(output_fp)
            if to_be_terminated and not has_output:
                # No more output, directly break.
                break
            
            if (
                (not to_be_terminated) and
                state.cmd_terminate_local.is_set()
            ):
                # Terminate from local.
                print(
                    f'Terminating server command: {msg.content}.'
                )
                send_message_to_server(
                    self.session_info,
                    type='terminate_cmd',
                    content=msg.msg_id
                )
                if wait_symbol(
                    self.session_info.command_terminate_confirm_fp(msg)
                ):
                    print(
                        f'Command successfully terminated.'
                    )
                else:
                    # TODO: non-block, but warning.
                    pass
                to_be_terminated = True
        
        # Remove all files.
        remove_file(output_fp, remove_lockfile=True)
    
    @cli_registry(key='exit')
    def cli_exit(self, cmd: str):
        raise CLITerminate
    
    def redirect_output(self, fp: str) -> bool:
        """
        Redirect the content of ``fp`` to the cli. Return whether 
        any content exists.
        """
        content = pop_all(fp)
        if not content:
            return False
        sys.stderr.write(content)
        sys.stderr.flush()
        return True
