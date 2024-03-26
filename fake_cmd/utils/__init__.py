import os
import time
from itertools import chain
from argparse import ArgumentParser, Namespace
from slime_core.utils.typing import (
    MISSING,
    Union,
    Any,
    Tuple,
    Sequence,
    Missing
)


class Config:
    
    def __init__(self) -> None:
        # Common polling interval
        self.polling_interval: float = 0.25
        # NOTE: The cmd is responsible for content output, 
        # so the polling interval should be short.
        self.cmd_polling_interval: float = 0.02
        # Read the subprocess pipe output within the timeout 
        # and return.
        # NOTE: This should be slightly larger than the 
        # ``cmd_polling_interval``, because this value determines 
        # how fast a new output file is created, and it should 
        # be slower than the client consuming the output files 
        # to avoid new files piling up in the long term.
        self.cmd_pipe_read_timeout: float = 0.03
        self.cmd_pipe_read_timeout_when_terminate: float = 0.5
        # Limit on the number of files.
        self.max_message_files: int = 1000
        self.max_output_files: int = 1000
        # command pipe output encoding method.
        self.cmd_pipe_encoding: str = 'utf-8'
        self.cmd_executable: Union[str, None] = None
        # When ``kill_cmd`` is sent or client terminated, 
        # read the remaining content util timeout.
        self.cmd_client_read_timeout: float = 1.0
        self.cmd_pool_schedule_interval: float = 0.2
        # Server shutdown wait timeout.
        self.server_shutdown_wait_timeout: float = 5.0
        # Common symbol timeouts.
        self.symbol_wait_timeout: float = 5.0
        self.symbol_remove_timeout: float = 0.01
        # Retries
        self.msg_send_retries: int = 3
        self.msg_confirm_wait_timeout: float = 3.0
        self.exception_retries: int = 3
        self.exception_wait_timeout: float = 0.5
        # Heartbeat settings.
        # Create or check symbol at a random interval between 
        # min and max, in order to reduce file conflicts.
        self.heartbeat_min_interval: float = 10.0
        self.heartbeat_max_interval: float = 20.0
        self.heartbeat_timeout: float = 600.0
        # The version info for compatibility check.
        self.version: Tuple[int, int, int] = (0, 0, 4)
        # For system settings.
        self.platform: str = 'unix'
        self.posix_shlex: bool = True


config = Config()


def version_check(
    version: Union[Tuple[int, int, int], None],
    *,
    min_version: Union[Tuple[int, int, int], None] = None,
    max_version: Union[Tuple[int, int, int], None] = None,
    verbose: bool = True
) -> bool:
    """
    Check whether the server version is compatible with some client
    APIs.
    """
    check_passed = False
    if version is None:
        check_passed = False
    else:
        version = tuple(version)
        check_passed = (
            ((not min_version) or version >= tuple(min_version)) and 
            ((not max_version) or version <= tuple(max_version))
        )
    
    if not check_passed and verbose:
        from .logging import logger
        logger.warning(
            f'Version check failed, expected version: '
            f'min: {min_version}, max: {max_version}, actual '
            f'version: {version}. You may need to get the '
            'latest updates.'
        )
    return check_passed


def polling(
    interval: float = MISSING
):
    """
    Polling with given interval. If not specified, 
    ``config.polling_interval`` is used.
    """
    interval = (
        interval if interval is not MISSING else config.polling_interval
    )
    while True:
        time.sleep(interval)
        yield


def timestamp_to_str(timestamp: float) -> str:
    """
    Parse timestamp to str.
    """
    time_tuple = time.localtime(int(timestamp))
    return time.strftime('%Y/%m/%d %H:%M:%S', time_tuple)


def get_server_name(address: str) -> str:
    dir_path = address
    possible_server_name = ''
    while dir_path and not possible_server_name:
        dir_path, possible_server_name = os.path.split(dir_path)
    
    if possible_server_name:
        return possible_server_name
    else:
        return 'remote'


def xor__(__x: Any, __y: Any) -> bool:
    return bool(
        (__x and not __y) or 
        (not __x and __y)
    )

#
# Argument Parsers
#

class ArgNamespace(Namespace):
    
    def __bool__(self) -> bool:
        return True


def parser_parse(
    parser: ArgumentParser,
    args: Union[Sequence[str], Missing, None] = MISSING,
    strict: bool = True
) -> Union[ArgNamespace, Missing]:
    """
    Argument parse args.
    """
    namespace = MISSING
    try:
        if args is MISSING:
            namespace = parser.parse_args(namespace=ArgNamespace())
        else:
            namespace = parser.parse_args(args=args, namespace=ArgNamespace())
    except SystemExit:
        if strict:
            raise
    return namespace

#
# Number comparison utils.
#

class GreaterThanAnything:
    """
    Used for comparison.
    """
    def __lt__(self, __value: Any) -> bool: return False
    def __le__(self, __value: Any) -> bool: return False
    def __eq__(self, __value: Any) -> bool: return False
    def __gt__(self, __value: Any) -> bool: return True
    def __ge__(self, __value: Any) -> bool: return True


class LessThanAnything:
    """
    Used for comparison.
    """
    def __lt__(self, __value: Any) -> bool: return True
    def __le__(self, __value: Any) -> bool: return True
    def __eq__(self, __value: Any) -> bool: return False
    def __gt__(self, __value: Any) -> bool: return False
    def __ge__(self, __value: Any) -> bool: return False

#
# UUID compression
#

BASE36_CHARS = ''.join(chr(i) for i in chain(range(48, 58), range(97, 123)))
# Number of digits needed to represent a 32-bit hexadecimal number.
NUMBER_DIGITS = 25


def uuid_base36(number: int) -> str:
    """
    Convert a uuid to the base36 format.
    """
    base36_num = ''
    while number > 0:
        number, remainder = divmod(number, 36)
        base36_num = BASE36_CHARS[remainder] + base36_num
    
    return base36_num.rjust(NUMBER_DIGITS, '0')
