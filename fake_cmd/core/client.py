import os
import sys
import uuid
import shlex
import argparse
import traceback
from functools import wraps
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
    NOTHING,
    MISSING,
    Union,
    Callable,
    Any,
    Literal,
    Missing,
    List,
    Tuple
)
from fake_cmd.utils.comm import (
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
from fake_cmd.utils.file import (
    remove_file_with_retry,
    remove_dir_with_retry
)
from fake_cmd.utils.exception import CLITerminate
from fake_cmd.utils.parallel import (
    LifecycleRun
)
from fake_cmd.utils.system import platform_open_registry
from fake_cmd.utils import (
    config,
    polling,
    get_server_name,
    version_check
)
from fake_cmd.utils.logging import logger
from . import SessionInfo, ActionFunc, dispatch_action, param_check
from .server import popen_writer_registry

# Keyboard interrupt count corresponding to different meanings.
CMD_INTERRUPT = 1
CMD_TERMINATE = 2
CMD_KILL = 3
CMD_BACKGROUND = 4

CLIENT_HELP = f"""
NOTE: ``Ctrl+C`` won't start a new line. Use ``Ctrl+C`` and ``enter`` instead.

Inner Commands:
``help``: Get the help document.
``exit``: Shutdown the client, disconnect session.
``sid``: Get the sid of the client.
``ls-session``: List all the alive sessions.
``ls-cmd``: List all the commands executing or queued.
``ls-back``: List the background command of the current session.
``ls-server-version``: Show the server version (for compatibility check).
``version_strict_on``: Set the version strict to True.
``version_strict_off``: Set the version strict to False.

Advanced Running:
``cmd``: Run the command with advanced options. Use ``cmd -h`` to get more help.
``inter``: Run the command in the interactive mode. Input is enabled, and if you \
want to quit, use both ``Ctrl+C`` (to terminate the command) and ``Ctrl+D`` (to \
quit the interactive input). Use ``inter -h`` to get more help.

Killing the Running Command:
The number of ``Ctrl+C`` you press represents different actions:
- ({CMD_INTERRUPT}): Send keyboard interrupt to the command.
- ({CMD_TERMINATE}): Terminate the command.
- ({CMD_KILL}): Kill the command.
- (>={CMD_BACKGROUND}): Put the command to the background (and then you can use 
``ls-back`` to check it, or manually use ``kill`` to kill it).

Danger Zone:
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


def cli_action_version_check(
    min_version: Union[Tuple[int, int, int], None] = None,
    max_version: Union[Tuple[int, int, int], None] = None,
    verbose: bool = True
):
    def decorator(func: Callable[..., Union[CommandMessage, bool]]):
        @wraps(func)
        def wrapper(self: "CLI", *args, **kwargs) -> Union[CommandMessage, bool]:
            if (
                self.version_strict and 
                not version_check(
                    version=self.server_version,
                    min_version=min_version,
                    max_version=max_version,
                    verbose=verbose
                )
            ):
                return False
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


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
        self.server_version: Union[Tuple[int, int, int], None] = None
        self.version_strict: bool = True
    
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
        
        logger.info(
            f'Connect with server: {address}. Session id: {session_id}. Client version: "{config.version}".'
        )
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
                        res = action(self, msg)
                        self.check_action_result(res)
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
    @param_check(required=('info',))
    def process_info(self, msg: Message) -> None:
        content = msg.content
        logger.info(f'Info from server: {content["info"]}')
    
    @client_registry(key='cmd_quit')
    @param_check(required=('cmd_id', 'type'))
    def cmd_quit(self, msg: Message) -> None:
        content = msg.content
        cmd_id = content['cmd_id']
        if (
            self.cli.current_cmd and 
            cmd_id != self.cli.current_cmd.cmd_id
        ):
            logger.warning(
                f'Cmd inconsistency occurred.'
            )
        self.terminate_cmd(cause=content['type'])
    
    @client_registry(key='server_version')
    @param_check(required=('version',))
    def process_server_version(self, msg: Message) -> None:
        """
        Get server version for compatibility check.
        """
        content = msg.content
        version = content['version']
        
        try:
            self.server_version = tuple(version)
        except Exception as e:
            logger.error(str(e))
    
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
    # Command operations.
    #
    
    def terminate_cmd(self, cause: Literal['remote', 'finish']):
        """
        Process command terminate messages.
        """
        state = self.state
        state_dict = {
            'remote': state.cmd_terminate_remote,
            'finish': state.cmd_finished
        }
        state_dict[cause].set()
    
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
    
    def check_action_result(self, res: Union[None, Tuple[str, ...]]):
        if res is None: pass
        elif isinstance(res, tuple):
            logger.warning(
                f'Missing args: {res}'
            )


CLIActionFunc = Callable[[Any, List[str]], Union[CommandMessage, bool]]


class CLI(
    LifecycleRun,
    Thread,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = (
        'client',
        'current_cmd_lock',
        'server_name',
        'cmd_parser',
        'inter_cmd_parser'
    )
    
    cli_registry = Registry[CLIActionFunc]('cli_registry')
    
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
        # parsers
        self.cmd_parser = get_cmd_parser()
        self.inter_cmd_parser = get_inter_cmd_parser()
    
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
    
    @property
    def server_version(self) -> Union[Tuple[int, int, int], None]:
        return self.client.server_version
    
    @property
    def version_strict(self) -> bool:
        return self.client.version_strict
    
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
            except CLITerminate:
                print('CLI exiting...')
                return
            except EOFError:
                # NOTE: Ignore EOFError here to avoid CLI exit by mistake 
                # in the interactive mode.
                pass
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
        
        cmd_splits = shlex.split(cmd)
        cli_func = self.cli_registry.get(cmd_splits[0], MISSING)
        if cli_func is MISSING:
            try:
                # All the cmd_splits will be seen as part of the command.
                args = self.cmd_parser.parse_args(
                    ['--'] + cmd_splits
                )
                args.interactive = False
            except:
                return False
            
            return self.send_session_cmd(
                content=self._make_cmd_content_through_args(args),
                type='cmd'
            )
        else:
            return cli_func(self, cmd_splits)
    
    # NOTE: Use version check here. Since (0, 0, 1), the msg 
    # content is either a dict or None, and str is no longer 
    # supported.
    @cli_action_version_check(min_version=(0, 0, 1))
    def send_session_cmd(
        self,
        content: dict,
        type: str = 'cmd'
    ) -> Union[CommandMessage, bool]:
        """
        Send the command to the session.
        """
        session_info = self.session_info
        msg = CommandMessage(
            session_id=session_info.session_id,
            type=type,
            content=content
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
        interactive = msg.interactive
        if interactive:
            cmd_input = InteractiveInput(self, msg)
            cmd_input.start()
        else:
            cmd_input = NOTHING
        
        try:
            # Set that the command is running.
            self.state.cmd_running.set()
            return self._wait_session_cmd(msg)
        except Exception as e:
            logger.error(str(e))
        finally:
            # Set that the command finished (for the interactive).
            self.state.cmd_running.clear()
            if interactive:
                if cmd_input.is_alive():
                    logger.info(
                        'The command has exited, and you need to press '
                        '``Ctrl+D`` or ``Enter`` to further quit the '
                        'interactive mode.'
                    )
                cmd_input.join()
            self.set_current_cmd(None)

    def _wait_session_cmd(self, msg: CommandMessage):
        """
        Wait the session command to finish (wrapped method).
        """
        output_namespace = self.session_info.message_output_namespace(msg)
        output_reader = OutputFileHandler(output_namespace)
        confirm_fp = self.session_info.command_terminate_confirm_fp(msg)
        state = self.state
        # The command content (for logging).
        cmd_content = msg.cmd_content
        # Indicate whether the current command is to be 
        # terminated (and quit the for loop if True).
        to_be_terminated = False
        # flags that indicate whether the corresponding 
        # kill_cmd messages have been sent (to avoid 
        # repeated sending).
        keyboard_interrupt_sent = False
        terminate_sent = False
        kill_sent = False
        
        #
        # For loop functions (to make the for loop clear and more 
        # readable).
        #
        def _check_cmd_state() -> bool:
            """
            Check command state. Return False if the command is ready 
            to quit.
            """
            if state.cmd_finished.is_set():
                # Clear the output before terminate.
                self.redirect_output(output_reader, read_all=True)
                return False
            if (
                state.cmd_terminate_remote.is_set() or 
                state.terminate.is_set() or 
                to_be_terminated
            ):
                # ``to_be_terminated``: the client has already request the session 
                # to terminate the command, no matter what the response is, or the 
                # session has confirmed that the command has been terminated.
                # NOTE: If keyboard_interrupt >= 4, the process will be put to the 
                # background. However, the process may still endlessly produce output, 
                # causing the cli unable to normally quit, so all the remaining content 
                # should be read within a set timeout, and directly break.
                self.redirect_output(
                    output_reader,
                    read_all=True,
                    read_all_timeout=config.cmd_client_read_timeout
                )
                return False
            return True
        
        def _send_kill_cmd_msg(type: str):
            """
            Send the kill_cmd msg with given type.
            """
            if (
                self.version_strict and 
                not version_check(
                    self.server_version,
                    min_version=(0, 0, 2)
                )
            ):
                return
            self.session_writer.write(
                Message(
                    session_id=self.session_info.session_id,
                    type='kill_cmd',
                    content={
                        'cmd_id': msg.cmd_id,
                        'type': type
                    }
                )
            )
        
        def _process_keyboard_interrupt():
            """
            Process the keyboard interrupt.
            """
            nonlocal to_be_terminated
            nonlocal kill_sent, terminate_sent, keyboard_interrupt_sent
            keyboard_interrupt_cnt = state.get_keyboard_interrupt()
            if keyboard_interrupt_cnt >= CMD_BACKGROUND:
                # Put the command to the background.
                self.session_writer.write(
                    Message(
                        session_id=self.session_info.session_id,
                        type='background_cmd',
                        content={'cmd_id': msg.cmd_id}
                    )
                )
                logger.info(
                    'Command is now put to the background, and use '
                    '``ls-back`` to check its state.'
                )
                # Set ``to_be_terminated``
                to_be_terminated = True
            elif (
                keyboard_interrupt_cnt == CMD_KILL and 
                not kill_sent
            ):
                # Kill the command (send SIGKILL)
                logger.info(f'Killing session command: {cmd_content}.')
                _send_kill_cmd_msg(type='force')
                kill_sent = True
            elif (
                keyboard_interrupt_cnt == CMD_TERMINATE and 
                not terminate_sent
            ):
                # Terminate the command (send SIGTERM)
                logger.info(f'Terminating session command: {cmd_content}.')
                _send_kill_cmd_msg(type='remote')
                terminate_sent = True
            elif (
                keyboard_interrupt_cnt == CMD_INTERRUPT and 
                not keyboard_interrupt_sent
            ):
                # Send keyboard interrupt (send SIGINT)
                logger.info(
                    f'Sending keyboard interrupt to session command: {cmd_content}.'
                )
                _send_kill_cmd_msg(type='keyboard')
                keyboard_interrupt_sent = True
        
        for _ in polling(config.cmd_polling_interval):
            # Check the command state.
            if not _check_cmd_state():
                break
            # Print the output content to the fake_cmd.
            self.redirect_output(output_reader, read_all=False)
            # Process keyboard interrupt.
            _process_keyboard_interrupt()
            if check_symbol(confirm_fp):
                logger.info(f'Command successfully terminated.')
                # Set ``to_be_terminated``
                to_be_terminated = True
        
        # Remove all files.
        remove_file_with_retry(confirm_fp)
        remove_dir_with_retry(output_namespace)
    
    #
    # Actions.
    #
    
    @cli_registry(key='exit')
    def cli_exit(self, cmd_splits: List[str]):
        raise CLITerminate
    
    @cli_registry(key='sid')
    def cli_sid(self, cmd_splits: List[str]) -> Literal[False]:
        print(
            f'Session id: {self.session_info.session_id}'
        )
        return False
    
    @cli_registry(key='help')
    def cli_help(self, cmd_splits: List[str]) -> Literal[False]:
        print(CLIENT_HELP.strip('\n'))
        return False
    
    @cli_registry(key='ls-server-version')
    def ls_server_version(self, cmd_splits: List[str]) -> Literal[False]:
        logger.info(
            f'Server version: {self.server_version}'
        )
        return False
    
    @cli_registry(key='version_strict_on')
    def version_strict_on(self, cmd_splits: List[str]) -> Literal[False]:
        self.client.version_strict = True
        logger.info('Set ``version_strict`` to ``True``.')
        return False
    
    @cli_registry(key='version_strict_off')
    def version_strict_off(self, cmd_splits: List[str]) -> Literal[False]:
        self.client.version_strict = False
        logger.info('Set ``version_strict`` to ``False``.')
        return False
    
    def _make_cmd_content_through_args(self, args) -> dict:
        return {
            # NOTE: Not using shlex.join for compatibility 
            # with Python 3.7
            'cmd': ' '.join(args.cmd),
            'encoding': args.encoding,
            'stdin': args.stdin,
            'exec': args.exec,
            'platform': args.platform,
            'interactive': args.interactive
        }
    
    @cli_registry(key='inter')
    @cli_action_version_check(min_version=(0, 0, 2))
    def interactive_cmd(self, cmd_splits: List[str]) -> Union[CommandMessage, bool]:
        """
        Open command in an interactive mode.
        """
        try:
            args = self.inter_cmd_parser.parse_args(cmd_splits[1:])
            args.interactive = True
        except:
            return False
        
        return self.send_session_cmd(
            content=self._make_cmd_content_through_args(args),
            type='cmd'
        )
    
    @cli_registry(key='cmd')
    @cli_action_version_check(min_version=(0, 0, 2))
    def escape_cmd(self, cmd_splits: List[str]) -> Union[CommandMessage, bool]:
        try:
            args = self.cmd_parser.parse_args(cmd_splits[1:])
            args.interactive = False
        except:
            return False
        
        return self.send_session_cmd(
            content=self._make_cmd_content_through_args(args),
            type='cmd'
        )
    
    @cli_registry.register_multi([
        'ls-back',
        'ls-session',
        'ls-cmd'
    ])
    @cli_action_version_check(min_version=(0, 0, 1))
    def inner_cmd(self, cmd_splits: List[str]) -> Union[CommandMessage, bool]:
        return self.send_session_cmd(
            content={
                'cmd': cmd_splits[0],
                'interactive': False
            },
            type='inner_cmd'
        )
    
    @cli_registry(key='server_shutdown')
    def server_shutdown(self, cmd_splits: List[str]) -> Literal[False]:
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
    
    #
    # Other methods.
    #
    
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


class InteractiveInput(
    Thread,
    ReadonlyAttr
):
    readonly_attr__ = (
        'cli',
        'msg'
    )
    
    def __init__(
        self,
        cli: CLI,
        msg: CommandMessage
    ) -> None:
        Thread.__init__(self)
        ReadonlyAttr.__init__(self)
        self.cli = cli
        self.msg = msg
    
    @property
    def cmd_id(self) -> str:
        return self.msg.cmd_id
    
    @property
    def session_writer(self) -> MessageHandler:
        return self.cli.session_writer
    
    @property
    def session_id(self) -> str:
        return self.cli.session_info.session_id
    
    @property
    def cmd_running(self) -> bool:
        return self.cli.state.cmd_running.is_set()
    
    def run(self) -> None:
        logger.info(
            f'You are in the interactive mode. Use ``Ctrl+C`` to '
            'terminate the command as usual, and use ``Ctrl+D`` to '
            'quit the interactive input.'
        )
        while True:
            try:
                input_str = input()
                if not self.cmd_running:
                    raise EOFError
                self.session_writer.write(
                    Message(
                        session_id=self.session_id,
                        type='cmd_input',
                        content={
                            'cmd_id': self.cmd_id,
                            'input_str': input_str
                        }
                    )
                )
            except EOFError:
                if not self.cmd_running:
                    logger.info(
                        f'Interactive mode quitted.'
                    )
                    return
            except Exception as e:
                logger.error(str(e))

#
# Command parsers.
#

def _get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--encoding',
        default=None,
        type=str,
        required=False,
        help=(
            'The encoding method of input and output. Should not '
            'be specified most of the time (and leave it default).'
        )
    )
    parser.add_argument(
        '--exec',
        default=None,
        type=str,
        required=False,
        help=(
            'The executable script path. Should not be specified '
            'most of the time (and leave it default).'
        )
    )
    parser.add_argument(
        '--platform',
        default=None,
        type=str,
        required=False,
        help=(
            'The running platform that decides process operations. '
            'Should not be specified most of the time (and leave it '
            'default).'
        ),
        choices=platform_open_registry.keys()
    )
    # TODO: interrupt can be used commonly rather than kill the command 
    # in the interactive mode.
    parser.add_argument(
        '--interrupt_disabled',
        action='store_true',
        help=(
            'Whether keyboard interrupt is disabled to kill the command.'
        )
    )
    parser.add_argument('cmd', nargs='*')
    return parser


def get_cmd_parser() -> argparse.ArgumentParser:
    parser = _get_parser()
    parser.prog = 'cmd'
    # Set the default to None.
    parser.add_argument(
        '--stdin',
        default=None,
        type=str,
        choices=(),
        required=False,
        help=(
            'The interactive input setting. Should never be '
            'specified in ``cmd``. If you want to start the '
            'command in an interactive mode, use ``inter`` '
            'instead.'
        )
    )
    return parser


def get_inter_cmd_parser() -> argparse.ArgumentParser:
    parser = _get_parser()
    parser.prog = 'inter'
    # Set the default to 'pipe'.
    parser.add_argument(
        '--stdin',
        default='pipe',
        type=str,
        choices=popen_writer_registry.keys(),
        required=False,
        help=(
            'The interactive input setting. Specify the stdin '
            'setting of Popen.'
        )
    )
    return parser
