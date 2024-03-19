import os
import sys
import uuid
import traceback
from contextlib import contextmanager
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
    Any,
    Literal,
    Missing
)
from file_comm.utils.comm import (
    Connection,
    Message,
    wait_symbol,
    create_symbol,
    check_symbol,
    Heartbeat,
    CommandMessage,
    MessageHandler,
    OutputFileHandler
)
from file_comm.utils.file import (
    remove_file_with_retry,
    remove_dir_with_retry
)
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
NOTE: ``Ctrl+C`` won't start a new line. Use ``Ctrl+C`` and ``enter`` instead.
Inner command help:
``help``: Get the help document.
``exit``: Shutdown the client, disconnect session (or use ``Ctrl+D`` is also allowed).
``sid``: Get the sid of the client.
``ls-session``: List all the alive sessions.
``ls-cmd``: List all the commands executing or queued.
``ls-back``: List the background command of the current session.
``server_shutdown``: Shutdown the whole server. BE CAREFUL TO USE IT!!!
"""


class State(ReadonlyAttr):
    """
    Communication items between client and cli.
    """
    readonly_attr__ = (
        'cmd_running',
        'cmd_terminate_remote',
        'keyboard_interrupt_lock',
        'keyboard_interrupt_max_cnt',
        'cmd_finished',
        'terminate',
        'terminate_lock',
        'unable_to_communicate'
    )
    
    def __init__(self) -> None:
        # Indicator that whether a command is running.
        self.cmd_running = Event()
        # Command terminated from remote.
        self.cmd_terminate_remote = Event()
        # Counting the "KeyboardInterrupt" Events.
        self.keyboard_interrupt_lock = RLock()
        self.keyboard_interrupt = 0
        # NOTE: Avoid endless Keyboard Interrupt from a 
        # process, which may cause integer overflow.
        self.keyboard_interrupt_max_cnt = 10
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
        self.cmd_terminate_remote.clear()
        # Reset the keyboard interrupt.
        self.reset_keyboard_interrupt()
        self.cmd_finished.clear()
    
    @contextmanager
    def reset_ctx(self):
        """
        State reset context manager.
        """
        self.reset()
        try:
            yield
        finally:
            self.reset()

    def add_keyboard_interrupt(self) -> None:
        with self.keyboard_interrupt_lock:
            if (
                self.keyboard_interrupt >= self.keyboard_interrupt_max_cnt
            ):
                return
            self.keyboard_interrupt += 1
    
    def reset_keyboard_interrupt(self) -> None:
        with self.keyboard_interrupt_lock:
            self.keyboard_interrupt = 0

    def get_keyboard_interrupt(self) -> int:
        with self.keyboard_interrupt_lock:
            return self.keyboard_interrupt


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
        'cli',
        'server_writer',
        'session_writer',
        'client_listener'
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
            self.session_info.heartbeat_session_fp
        )
        self.state = State()
        self.cli = CLI(self)
        self.server_writer = MessageHandler(self.session_info.server_listen_namespace)
        self.session_writer = MessageHandler(self.session_info.session_queue_namespace)
        self.client_listener = MessageHandler(self.session_info.client_queue_namespace)
    
    #
    # Running operations.
    #
    
    def before_running(self) -> bool:
        server_check_fp = self.session_info.server_check_fp
        address = self.session_info.address
        
        # Should check whether the server exists. Otherwise, 
        # ``self.connect`` will create a new ``main_fp`` file 
        # and the real server startup will fail after that.
        if not os.path.exists(server_check_fp):
            logger.warning(
                f'Server address not found: {address}.'
            )
            return False
        
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
                    
                    msg = self.client_listener.read_one()
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
                print(
                    'Keyboard Interrupt. (Use "Ctrl + C" and "enter" '
                    'to start a new input line)'
                )
                state.add_keyboard_interrupt()
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
        
        res = self.server_writer.write(
            Message(
                session_id=self.session_info.session_id,
                type='new_session'
            )
        )
        
        if not res:
            return False
        if not wait_symbol(
            self.session_info.conn_client_fp
        ):
            logger.warning(
                f'Server connection establishment failed. Server address: {address}.'
                f'Session id: {session_id}.'
            )
            return False
        create_symbol(self.session_info.conn_session_fp)
        return True
    
    def disconnect(self, initiator: bool):
        logger.info(
            f'Disconnecting from server. Server: {self.session_info.address}. '
            f'Session: {self.session_info.session_id}.'
        )
        
        disconn_session_fp = self.session_info.disconn_session_fp
        disconn_confirm_to_session_fp = self.session_info.disconn_confirm_to_session_fp
        disconn_client_fp = self.session_info.disconn_client_fp
        disconn_confirm_to_client_fp = self.session_info.disconn_confirm_to_client_fp
        state = self.state
        if initiator:
            with state.terminate_lock:
                # Use lock to make it consistent.
                # Send remaining messages before disconnect.
                # Add twice to force kill.
                state.add_keyboard_interrupt()
                state.add_keyboard_interrupt()
                # Send to server to disconnect.
                create_symbol(disconn_session_fp)
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
                create_symbol(disconn_confirm_to_session_fp)
                state.terminate.set()
        else:
            with state.terminate_lock:
                # Use lock to make it consistent.
                create_symbol(disconn_confirm_to_session_fp)
                # Send remaining messages.
                state.cmd_terminate_remote.set()
                create_symbol(disconn_session_fp)
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
        if self.state.unable_to_communicate.is_set():
            # The final clear operation should be done 
            # here if the server is unable to communicate.
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
        return f'[fake_cmd {self.server_name}]$ '
    
    @property
    def session_writer(self) -> MessageHandler:
        return self.client.session_writer
    
    @property
    def server_writer(self) -> MessageHandler:
        return self.client.server_writer
    
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
        
        def safe_check_terminate() -> bool:
            """
            Safely check whether to terminate.
            """
            # Use double check to make it safe.
            if state.terminate.is_set():
                return True
            with state.terminate_lock:
                if state.terminate.is_set():
                    return True
            return False
        
        while True:
            try:
                # Command input.
                with state.reset_ctx():
                    # The terminate state should be checked before and after the 
                    # cmd input.
                    if safe_check_terminate(): return
                    cmd = input(self.input_hint)
                    if safe_check_terminate(): return
                    if state.get_keyboard_interrupt() > 0:
                        # If keyboard interrupt pressed, directly ignore the cmd.
                        continue
                
                # Process the command.
                with state.reset_ctx():
                    # NOTE: the command has already been sent to the server here.
                    msg = self.process_cmd(cmd)
                    if msg:
                        self.wait_session_cmd(msg)
                        self.set_current_cmd(None)
            except (CLITerminate, EOFError):
                print('CLI exiting...')
                return
            except:
                traceback.print_exc()
    
    def after_running(self, *args):
        return
    
    #
    # Command operations.
    #
    
    def process_cmd(self, cmd: Union[str, None]) -> Union[CommandMessage, bool]:
        """
        Process the input cmd. Return a ``CommandMessage`` object if the 
        cmd should be executed in the server and it is successfully received, 
        else return ``False``.
        """
        if not cmd:
            return False
        
        cli_func = self.cli_registry.get(cmd, MISSING)
        if cli_func is MISSING:
            return self.send_session_cmd(cmd, type='cmd')
        else:
            return cli_func(self, cmd)
    
    def send_session_cmd(self, cmd: str, type: str = 'cmd') -> Union[CommandMessage, bool]:
        """
        Send the command to the session.
        """
        session_info = self.session_info
        msg = CommandMessage(
            session_id=session_info.session_id,
            type=type,
            content=cmd
        )
        self.set_current_cmd(msg)
        res = self.session_writer.write(msg)
        if not res:
            self.set_current_cmd(None)
            return False
        return msg
    
    def wait_session_cmd(self, msg: CommandMessage):
        """
        Wait the session command to finish.
        """
        # Set that the command is running.
        self.state.cmd_running.set()
        
        output_namespace = self.session_info.message_output_namespace(msg)
        output_reader = OutputFileHandler(output_namespace)
        confirm_fp = self.session_info.command_terminate_confirm_fp(msg)
        state = self.state
        
        to_be_terminated = False
        for _ in polling(config.cmd_polling_interval):
            if (
                state.cmd_terminate_remote.is_set() or 
                state.cmd_finished.is_set()
            ):
                # Clear the output before terminate.
                self.redirect_output(output_reader, read_all=True)
                break
            
            has_output = self.redirect_output(output_reader, read_all=False)
            if to_be_terminated and not has_output:
                # No more output, directly break.
                break
            
            if (
                state.terminate.is_set() or 
                (to_be_terminated and state.get_keyboard_interrupt() > 1)
            ):
                # ``to_be_terminated``: the client has already request the session 
                # to terminate the command, no matter what the response is. 
                # NOTE: If cmd_terminate wait over timeout, the process will be put 
                # to the background. However, the process may still endlessly produce 
                # output, causing the cli unable to normally quit, so when keyboard_interrupt
                # is set more than 1, read all the remaining content within a set 
                # timeout, and directly break.
                self.redirect_output(
                    output_reader,
                    read_all=True,
                    read_all_timeout=config.cmd_client_read_timeout
                )
                break
            
            if (
                (not to_be_terminated) and
                state.get_keyboard_interrupt() > 0
            ):
                # Terminate from local.
                logger.info(
                    f'Terminating session command: {msg.cmd_content}.'
                )
                self.session_writer.write(
                    Message(
                        session_id=self.session_info.session_id,
                        type='terminate_cmd',
                        content=msg.cmd_id
                    )
                )
                remote_terminated = wait_symbol(confirm_fp, config.cmd_terminate_timeout)
                
                if (
                    not remote_terminated and 
                    state.get_keyboard_interrupt() > 1
                ):
                    # After terminate local, check force kill.
                    logger.info('Trying force kill the command...')
                    self.session_writer.write(
                        Message(
                            session_id=self.session_info.session_id,
                            type='force_kill_cmd',
                            content=msg.cmd_id
                        )
                    )
                    remote_terminated = wait_symbol(confirm_fp, config.cmd_force_kill_timeout)
                
                if remote_terminated:
                    logger.info(
                        f'Command successfully terminated.'
                    )
                else:
                    logger.warning(
                        f'Command may take more time to terminate, and '
                        f'it is now put to the background.'
                    )
                    self.session_writer.write(
                        Message(
                            session_id=self.session_info.session_id,
                            type='background_cmd',
                            content=msg.cmd_id
                        )
                    )
                to_be_terminated = True
        
        # Remove all files.
        remove_file_with_retry(confirm_fp)
        remove_dir_with_retry(output_namespace)
    
    @cli_registry(key='exit')
    def cli_exit(self, cmd: str):
        raise CLITerminate
    
    @cli_registry(key='sid')
    def cli_sid(self, cmd: str) -> Literal[False]:
        print(
            f'Session id: {self.session_info.session_id}'
        )
        return False
    
    @cli_registry(key='help')
    def cli_help(self, cmd: str) -> Literal[False]:
        print(CLIENT_HELP.strip('\n'))
        return False
    
    @cli_registry.register_multi([
        'ls-back',
        'ls-session',
        'ls-cmd'
    ])
    def inner_cmd(self, cmd: str) -> Union[CommandMessage, bool]:
        return self.send_session_cmd(cmd, type='inner_cmd')
    
    @cli_registry(key='server_shutdown')
    def server_shutdown(self, cmd: str) -> Literal[False]:
        try:
            confirm_key = 'YES'
            res = input(
                'WARNING: You are trying to shutdown the server. '
                'This behavior will terminate all the running '
                'commands and destroy all the sessions. Use ``ls-cmd``'
                'and ``ls-session`` to check them. If you are '
                f'sure, enter exactly "{confirm_key}", else enter '
                f'anything else. ("{confirm_key}" / [Anything else]): '
            )
            if res != confirm_key:
                logger.info(
                    'Server shutdown canceled.'
                )
                return False
            self.server_writer.write(
                Message(
                    session_id=self.session_info.session_id,
                    type='server_shutdown'
                )
            )
        finally:
            return False
    
    def redirect_output(
        self,
        reader: OutputFileHandler,
        read_all: bool,
        read_all_timeout: Union[float, Missing] = MISSING
    ) -> bool:
        """
        Redirect the content of ``fp`` to the cli. Return whether 
        any content exists.
        
        ``read_all_timeout``: only works when ``read_all`` is True.
        """
        content = ''
        if read_all:
            content = reader.read_all(timeout=read_all_timeout)
        else:
            content = reader.read_one()
        # NOTE: If ``read_all`` is ``True``, then we can 
        # directly judge there is no content to read and 
        # return ``False``. However, if ``read_all`` is 
        # ``False``, we just read one file from the 
        # namespace, and if ``content`` is an empty str, 
        # it does not represent that no more content is 
        # available, so we should judge whether ``content``
        # is exactly ``False`` rather than using 
        # ``not content``.
        if (
            (read_all and (not content)) or 
            ((not read_all) and content is False)
        ):
            return False
        sys.stdout.write(content)
        sys.stdout.flush()
        return True
