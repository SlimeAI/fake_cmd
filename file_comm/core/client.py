import os
import sys
import uuid
import traceback
from threading import Thread, Event, RLock
from abc import ABCMeta
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.metaclass import (
    Metaclasses,
    _ReadonlyAttrMetaclass
)
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
from file_comm.utils.file import pop_all, remove_file, file_lock
from file_comm.utils.exception import CLITerminate
from file_comm.utils.parallel import (
    LifecycleRun
)
from file_comm.utils import (
    config,
    polling,
    get_server_name
)
from file_comm.utils.logging import logger
from . import SessionInfo, ActionFunc, dispatch_action

CLIENT_HELP = """
NOTE: Use ``Ctrl+C`` AND ``Ctrl+D`` to start a new line.
Inner command help:
``help``: Get the help document.
``exit``: Shutdown the client, disconnect session.
``sid``: Get the sid of the client.
``ls-session``: List all the alive sessions.
``ls-cmd``: List all the commands executing or queued.
``ls-back``: List the background command of the current session.
"""


class State(ReadonlyAttr):
    """
    Communication items between client and cli.
    """
    readonly_attr__ = (
        'cmd_running',
        'cmd_terminate_local',
        'cmd_terminate_remote',
        'cmd_force_kill',
        'cmd_finished',
        'terminate',
        'terminate_lock',
        'unable_to_communicate'
    )
    
    def __init__(self) -> None:
        # Indicator that whether a command is running.
        self.cmd_running = Event()
        # Command terminate from local.
        self.cmd_terminate_local = Event()
        # Command terminated from remote.
        self.cmd_terminate_remote = Event()
        # Command force kill.
        # NOTE: The force kill is always set after 
        # ``cmd_terminate_local``.
        self.cmd_force_kill = Event()
        # Command finished.
        self.cmd_finished = Event()
        # CLI terminate indicator.
        self.terminate = Event()
        self.terminate_lock = RLock()
        # Unable to communicate to server, so in 
        # ``clear_cache``, it will clear the namespace 
        # ignoring whether the server has already 
        # finished reading messages.
        self.unable_to_communicate = Event()
    
    def reset(self) -> None:
        self.cmd_running.clear()
        self.cmd_terminate_local.clear()
        self.cmd_terminate_remote.clear()
        self.cmd_force_kill.clear()
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


class Client(
    LifecycleRun,
    Connection,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = (
        'session_info',
        'heartbeat',
        'state',
        'cli'
    )
    
    client_registry = Registry[ActionFunc]('client_registry')
    
    def __init__(
        self,
        address: str,
        id_prefix: Union[str, None] = None
    ) -> None:
        """
        ``id_prefix``: You can specify your own prefix to make the 
        ``session_id`` more distinguishable.
        """
        LifecycleRun.__init__(self)
        # Create a new session id.
        session_id = str(uuid.uuid1())
        if id_prefix:
            session_id = f'{id_prefix}-{session_id}'
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
        
        logger.info(f'Connect with server: {address}. Session id: {session_id}.')
        print(CLIENT_HELP.strip('\n'))
        
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
                    if state.cmd_terminate_local.is_set():
                        print('Force kill set.')
                        # Double "Ctrl + C" to force kill the command.
                        state.cmd_force_kill.set()
                    state.cmd_terminate_local.set()
            except:
                traceback.print_exc()
    
    def after_running(self, *args):
        self.clear_cache()
    
    #
    # Actions.
    #
    
    @client_registry(key='info')
    def process_info(self, msg: Message):
        logger.info(f'Info from server: {msg.content}')
    
    @client_registry(key='cmd_finished')
    def process_cmd_finished(self, msg: Message):
        cmd_id = msg.content
        if (
            self.cli.current_cmd and 
            cmd_id != self.cli.current_cmd.cmd_id
        ):
            logger.warning(
                f'Cmd inconsistency occurred.'
            )
        self.state.cmd_finished.set()
    
    @client_registry(key='cmd_terminated')
    def process_cmd_terminated(self, msg: Message):
        cmd_id = msg.content
        if (
            self.cli.current_cmd and 
            cmd_id != self.cli.current_cmd.cmd_id
        ):
            logger.warning(
                f'Cmd inconsistency occurred.'
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
            logger.warning(
                f'Server connection establishment failed. Server address: {address}.'
                f'Session id: {session_id}.'
            )
            return False
        create_symbol(self.session_info.conn_server_fp)
        return True
    
    def disconnect(self, initiator: bool):
        logger.info(
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
                state.cmd_force_kill.set()
                # Send to server to disconnect.
                create_symbol(disconn_server_fp)
                if (
                    not wait_symbol(disconn_confirm_to_client_fp) or 
                    not wait_symbol(disconn_client_fp)
                ):
                    logger.warning(
                        'Disconnection from server is not responded, '
                        'ignore and continue...'
                    )
                    # Set that the server is unable to communicate.
                    state.unable_to_communicate.set()
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
                    logger.warning(
                        'Disconnection from server is not responded, '
                        'ignore and continue...'
                    )
                    # Set that the server is unable to communicate.
                    state.unable_to_communicate.set()
                state.terminate.set()
        logger.info('Disconnected.')
    
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
    
    #
    # Other methods
    #
    
    def clear_cache(self):
        """
        Clear cached files.
        """
        with file_lock(self.session_info.clear_lock_fp):
            remove_file(self.session_info.client_fp, remove_lockfile=True)
            session_destroyed = (not os.path.exists(self.session_info.server_fp))
        if (
            session_destroyed or 
            self.state.unable_to_communicate.is_set()
        ):
            # The final clear operation should be done 
            # here if the session is already destroyed 
            # or the server is unable to communicate.
            self.session_info.clear_session()


class CLI(
    LifecycleRun,
    Thread,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = (
        'client',
        'current_cmd_lock',
        'server_name'
    )
    
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
        self.server_name = get_server_name(self.session_info.address)
    
    @property
    def session_info(self) -> SessionInfo:
        return self.client.session_info
    
    @property
    def state(self) -> State:
        return self.client.state
    
    @property
    def input_hint(self) -> str:
        return f'[fake_cmd {self.server_name}] '
    
    def set_current_cmd(self, cmd: Union[CommandMessage, None]) -> None:
        with self.current_cmd_lock:
            self.current_cmd = cmd
    
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
                
                cmd = input(self.input_hint)
                # NOTE: the state reset should be in front of the command sent 
                # to the server.
                state.reset()
                # NOTE: the command has already been sent to the server here.
                msg = self.process_cmd(cmd)
                if msg:
                    state.cmd_running.set()
                    self.wait_server_cmd(msg)
                    self.set_current_cmd(None)
                # No matter what happened, reset the state for the next command.
                state.reset()
            except CLITerminate:
                return
            except EOFError:
                pass
            except:
                traceback.print_exc()
    
    def after_running(self, *args):
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
            return self.send_server_cmd(cmd, type='cmd')
        else:
            return cli_func(self, cmd)
    
    def send_server_cmd(self, cmd: str, type: str = 'cmd') -> Union[CommandMessage, bool]:
        """
        Send the command to the server.
        """
        # NOTE: The message should be manually sent rather than use 
        # ``send_message_to_session`` in order to keep consistent 
        # with ``self.current_cmd``
        session_info = self.session_info
        msg = CommandMessage(
            session_id=session_info.session_id,
            target_fp=session_info.server_fp,
            confirm_namespace=session_info.session_namespace,
            type=type,
            content=cmd
        )
        self.set_current_cmd(msg)
        res = send_message(msg)
        if not res:
            self.set_current_cmd(None)
            return False
        return msg
    
    def wait_server_cmd(self, msg: CommandMessage):
        """
        Wait the server command to finish.
        """
        output_fp = self.session_info.message_output_fp(msg)
        confirm_fp = self.session_info.command_terminate_confirm_fp(msg)
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
                logger.info(
                    f'Terminating server command: {msg.cmd_content}.'
                )
                send_message_to_session(
                    self.session_info,
                    type='terminate_cmd',
                    content=msg.cmd_id
                )
                remote_terminated = wait_symbol(confirm_fp, config.cmd_terminate_timeout)
                
                if (
                    not remote_terminated and 
                    state.cmd_force_kill.is_set()
                ):
                    # After terminate local, check force kill.
                    logger.info('Trying force kill the command...')
                    send_message_to_session(
                        self.session_info,
                        type='force_kill_cmd',
                        content=msg.cmd_id
                    )
                    remote_terminated = wait_symbol(confirm_fp, config.cmd_terminate_timeout)
                
                if remote_terminated:
                    logger.info(
                        f'Command successfully terminated.'
                    )
                else:
                    logger.warning(
                        f'Command may take more time to terminate, and '
                        f'it is now put to the background.'
                    )
                    send_message_to_session(
                        self.session_info,
                        type='background_cmd',
                        content=msg.cmd_id
                    )
                to_be_terminated = True
        
        # Remove all files.
        remove_file(confirm_fp, remove_lockfile=True)
        remove_file(output_fp, remove_lockfile=True)
    
    @cli_registry(key='exit')
    def cli_exit(self, cmd: str):
        raise CLITerminate
    
    @cli_registry(key='sid')
    def cli_sid(self, cmd: str) -> bool:
        print(
            f'Session id: {self.session_info.session_id}'
        )
        return False
    
    @cli_registry(key='help')
    def cli_help(self, cmd: str):
        print(CLIENT_HELP.strip('\n'))
    
    @cli_registry.register_multi([
        'ls-back',
        'ls-session',
        'ls-cmd'
    ])
    def inner_cmd(self, cmd: str) -> Union[CommandMessage, bool]:
        return self.send_server_cmd(cmd, type='inner_cmd')
    
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
