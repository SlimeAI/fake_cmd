import os
import time
from functools import partial
from slime_core.utils.base import BaseList
from slime_core.utils.typing import (
    MISSING,
    Type,
    Union,
    Missing,
    Callable,
    TypeVar,
    Any
)
from .logging import logger

_T = TypeVar("_T")
_CallableT = TypeVar("_CallableT", bound=Callable)


class Config:
    
    def __init__(self) -> None:
        # Common polling interval
        self.polling_interval = 0.5
        # NOTE: The cmd is responsible for content output, 
        # so the polling interval should be short.
        self.cmd_polling_interval = 0.01
        # Read the subprocess pipe output within the timeout 
        # and return.
        self.cmd_pipe_read_timeout = 0.01
        # command pipe output encoding method.
        self.cmd_pipe_encoding: Union[str, None] = None
        self.cmd_executable: Union[str, None] = None
        # When ``cmd_force_kill`` is set or client terminated, 
        # read the remaining content util timeout.
        self.cmd_client_read_timeout = 1.0
        self.cmd_pool_schedule_interval = 0.5
        # Server shutdown wait timeout.
        self.server_shutdown_wait_timeout = 5.0
        # Common symbol wait timeout.
        self.wait_timeout = 10.0
        # Command terminate wait timeout (used by 
        # client to decide how long to wait when 
        # kill a command).
        self.cmd_terminate_timeout = 15.0
        self.cmd_force_kill_timeout = 5.0
        # Retries
        self.send_msg_retries: int = 3
        self.msg_confirm_wait_timeout: float = 3.0
        self.exception_retries: int = 3
        self.exception_wait_timeout: float = 0.5
        # Heartbeat settings.
        self.heartbeat_interval = 10.0
        self.heartbeat_timeout = 600.0


config = Config()


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

#
# Exception handling.
#

class ExceptionInfo(BaseList[BaseException]):
    """
    Used to check the exception information.
    """
    pass


def retry(
    func: Callable[[], _T],
    suppress_exc: Type[BaseException] = Exception,
    max_retries: Union[int, Missing] = MISSING,
    exception_wait_timeout: Union[float, Missing] = MISSING
) -> Union[_T, ExceptionInfo]:
    """
    Try to call the ``func`` and retry.
    """
    max_retries = (
        max_retries if 
        max_retries is not MISSING else 
        config.exception_retries
    )
    exception_wait_timeout = (
        exception_wait_timeout if 
        exception_wait_timeout is not MISSING else 
        config.exception_wait_timeout
    )
    
    info = ExceptionInfo()
    attempt = 0
    while True:
        try:
            return func()
        except suppress_exc as e:
            info.append(e)
        
        if attempt >= max_retries:
            logger.warning(
                f'Func {str(func)} run {attempt} times, but '
                'still failed, returning the ExceptionInfo.'
            )
            return info
        
        attempt += 1
        logger.warning(
            f'Exception occurred in {str(func)}, retry after '
            f'{exception_wait_timeout} seconds. Exception: {str(info)}.'
        )
        time.sleep(exception_wait_timeout)


def retry_deco(
    suppress_exc: Type[BaseException] = Exception,
    max_retries: Union[int, Missing] = MISSING,
    exception_wait_timeout: Union[float, Missing] = MISSING
) -> Callable[[_CallableT], _CallableT]:
    """
    Decorator version of retry.
    """
    def decorator(func: _CallableT) -> _CallableT:
        def wrapper(*args, **kwds):
            return retry(
                partial(func, *args, **kwds),
                suppress_exc=suppress_exc,
                max_retries=max_retries,
                exception_wait_timeout=exception_wait_timeout
            )
        return wrapper
    return decorator


def xor__(__x: Any, __y: Any) -> bool:
    return bool(
        (__x and not __y) or 
        (not __x and __y)
    )
