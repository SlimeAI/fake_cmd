import sys
import uuid
import traceback
from threading import Thread, Event, RLock
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    MISSING,
    Union,
    Callable,
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
    Heartbeat,
    CommandMessage
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
        self.terminate_lock = RLock()
    
    def reset(self) -> None:
        self.cmd_running.clear()
        self.cmd_terminate_local.clear()
        self.cmd_terminate_remote.clear()
        self.cmd_finished.clear()


def send_message_to_server(
    session_info: SessionInfo,
    *,
    type: str,
    content: Union[str, dict, list, None] = None
):
    msg = Message(
        session_id=session_info.session_id,
        target_fp=session_info.main_fp,
        confirm_namespace=session_info.address,
        type=type,
        content=content
    )
    return (msg, send_message(msg))


def send_message_to_session(
    session_info: SessionInfo,
    *,
    type: str,
    content: Union[str, dict, list, None] = None
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
        self.cli = CLI(self)
    
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
    
    #
    # Actions.
    #
    
    @client_registry(key='info')
    def process_info(self, msg: Message):
        print(msg.content)
    
    @client_registry(key='cmd_finished')
    def process_cmd_finished(self, msg: Message):
        cmd_id = msg.content
        if (
            not self.cli.current_cmd or 
            cmd_id != self.cli.current_cmd.cmd_id
        ):
            print(
                f'Warning: cmd inconsistency occurred.'
            )
        self.state.cmd_finished.set()
    
    @client_registry(key='cmd_terminated')
    def process_cmd_terminated(self, msg: Message):
        cmd_id = msg.content
        if (
            not self.cli.current_cmd or 
            cmd_id != self.cli.current_cmd.cmd_id
        ):
            print(
                f'Warning: cmd inconsistency occurred.'
            )
        self.state.cmd_terminate_remote.set()
    
    #
    # Connection operations.
    #
    
    def connect(self) -> bool:
        address = self.session_info.address
        session_id = self.session_info.session_id
        
        _, res = send_message_to_server(
            self.session_info,
            type='new_session'
        )
        
        if not res:
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
        print(
            f'Disconnecting from server. Server: {self.session_info.address}. '
            f'Session: {self.session_info.session_id}.'
        )
        
        disconn_server_fp = self.session_info.disconn_server_fp
        disconn_confirm_to_server_fp = self.session_info.disconn_confirm_to_server_fp
        disconn_client_fp = self.session_info.disconn_client_fp
        disconn_confirm_to_client_fp = self.session_info.disconn_confirm_to_client_fp
        state = self.state
        if initiator:
            with state.terminate_lock:
                # Use lock to make it consistent.
                # Send remaining messages before disconnect.
                state.cmd_terminate_local.set()
                # Send to server to disconnect.
                create_symbol(disconn_server_fp)
                if (
                    not wait_symbol(disconn_confirm_to_client_fp) or 
                    not wait_symbol(disconn_client_fp)
                ):
                    print(
                        'Warning: disconnection from server is not responded, '
                        'ignore and continue...'
                    )
                create_symbol(disconn_confirm_to_server_fp)
                state.terminate.set()
        else:
            with state.terminate_lock:
                # Use lock to make it consistent.
                create_symbol(disconn_confirm_to_server_fp)
                # Send remaining messages.
                state.cmd_terminate_remote.set()
                create_symbol(disconn_server_fp)
                if not wait_symbol(disconn_confirm_to_client_fp):
                    print(
                        'Warning: disconnection from server is not responded, '
                        'ignore and continue...'
                    )
                state.terminate.set()
        print('Disconnected.')
    
    def check_connection(self) -> bool:
        disconn_client_fp = self.session_info.disconn_client_fp
        # Disconnection from remote.
        if check_symbol(disconn_client_fp):
            self.disconnect(initiator=False)
            return False
        elif (
            not self.heartbeat.beat() or 
            not self.cli.is_alive()
        ):
            self.disconnect(initiator=True)
            return False
        return True


class CLI(LifecycleRun, Thread):
    
    cli_registry = Registry[Callable[[Any, str], Union[CommandMessage, bool]]]('cli_registry')
    
    def __init__(
        self,
        client: Client
    ):
        LifecycleRun.__init__(self)
        Thread.__init__(self)
        self.client = client
        self.current_cmd: Union[CommandMessage, None] = None
        # Use a current cmd lock to make it consistent with 
        # the current waiting server cmd.
        self.current_cmd_lock = RLock()
    
    @property
    def session_info(self) -> SessionInfo:
        return self.client.session_info
    
    @property
    def state(self) -> State:
        return self.client.state
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        return True
    
    def running(self):
        state = self.state
        while True:
            try:
                # Use double check to make it safe.
                if state.terminate.is_set():
                    return
                with state.terminate_lock:
                    if state.terminate.is_set():
                        return
                
                cmd = input('[fake_cmd] ')
                msg = self.process_cmd(cmd)
                if msg:
                    state.reset()
                    state.cmd_running.set()
                    with self.current_cmd_lock:
                        # Use lock to make the current cmd consistent.
                        self.current_cmd = msg
                        self.wait_server_cmd(msg)
                        self.current_cmd = None
                    state.reset()
            except CLITerminate:
                return
            except EOFError:
                pass
            except:
                traceback.print_exc()
    
    def after_running(self):
        return
    
    #
    # Command operations.
    #
    
    def process_cmd(self, cmd: str) -> Union[CommandMessage, bool]:
        """
        Process the input cmd. Return a ``CommandMessage`` object if the 
        cmd should be executed in the server and it is successfully received, 
        else return ``False``.
        """
        if not cmd:
            return False
        
        cli_func = self.cli_registry.get(cmd, MISSING)
        if cli_func is MISSING:
            return self.send_server_cmd(cmd)
        else:
            return cli_func(self, cmd)
    
    def send_server_cmd(self, cmd: str, type: str = 'cmd') -> Union[CommandMessage, bool]:
        """
        Send the command to the server.
        """
        msg, res = send_message_to_session(
            self.session_info,
            type=type,
            content=cmd
        )
        if not res:
            return False
        return CommandMessage.clone(msg)
    
    def wait_server_cmd(self, msg: CommandMessage):
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
                    f'Terminating server command: {msg.cmd_content}.'
                )
                send_message_to_session(
                    self.session_info,
                    type='terminate_cmd',
                    content=msg.cmd_id
                )
                if wait_symbol(
                    self.session_info.command_terminate_confirm_fp(msg)
                ):
                    print(
                        f'Command successfully terminated.'
                    )
                else:
                    print(
                        f'Warning: command may take more time to terminate, and '
                        f'it is now put to the backstage.'
                    )
                    send_message_to_session(
                        self.session_info,
                        type='backstage_cmd',
                        content=msg.cmd_id
                    )
                to_be_terminated = True
        
        # Remove all files.
        remove_file(output_fp, remove_lockfile=True)
    
    @cli_registry(key='exit')
    def cli_exit(self, cmd: str):
        raise CLITerminate
    
    @cli_registry.register_multi([
        'check_backstage'
    ])
    def inner_cmd(self, cmd: str) -> Union[CommandMessage, bool]:
        msg, res = send_message_to_session(
            self.session_info,
            type='inner_cmd',
            content=cmd
        )
        if not res:
            return False
        return CommandMessage.clone(msg)
    
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
