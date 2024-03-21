import os
import time
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.typing import (
    MISSING,
    Union,
    Any,
    Tuple
)


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
        self.cmd_pipe_encoding: str = 'utf-8'
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
        self.cmd_terminate_timeout = 20.0
        self.cmd_force_kill_timeout = 5.0
        # Retries
        self.send_msg_retries: int = 3
        self.msg_confirm_wait_timeout: float = 3.0
        self.exception_retries: int = 3
        self.exception_wait_timeout: float = 0.5
        # Heartbeat settings.
        self.heartbeat_interval = 10.0
        self.heartbeat_timeout = 600.0
        # The version info for compatibility check.
        self.version: Tuple[int, int, int] = (0, 0, 1)


config = Config()


def version_check(
    version: Union[Tuple[int, int, int], None],
    *,
    min_version: Union[Tuple[int, int, int], None] = None,
    max_version: Union[Tuple[int, int, int], None] = None
) -> bool:
    """
    Check whether the server version is compatible with some client
    APIs.
    """
    if version is None:
        return False
    
    version = tuple(version)
    return (
        ((not min_version) or version >= tuple(min_version)) and 
        ((not max_version) or version <= tuple(max_version))
    )


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
# Stream bytes service.
#

class StreamBytesParser(ReadonlyAttr):
    """
    Parse bytes stream eagerly.
    """
    readonly_attr__ = ('encoding',)
    
    def __init__(
        self,
        encoding: str = 'utf-8'
    ) -> None:
        self.buffer = b''
        self.encoding = encoding
    
    def parse(self, data: bytes) -> str:
        """
        Try to parse new data with previous stream buffer.
        """
        if self.buffer:
            data = self.buffer + data
        
        parsed = ''
        try:
            parsed = data.decode(encoding=self.encoding)
        except UnicodeDecodeError as e:
            if e.start != 0:
                # Parse the previous right data.
                parsed = data[:e.start].decode(encoding=self.encoding)
                self.buffer = data[e.start:]
            elif e.end != len(data):
                # This means there is some error in the middle, 
                # then directly parse the decoded str with error 
                # replace (to explicitly show that there is an 
                # error).
                parsed = data.decode(encoding=self.encoding, errors='replace')
                self.buffer = b''
            else:
                # The total bytes are decoded with error, should wait 
                # the following bytes.
                self.buffer = data
        else:
            # Successfully decode, clear the buffer.
            self.buffer = b''
        return parsed


def xor__(__x: Any, __y: Any) -> bool:
    return bool(
        (__x and not __y) or 
        (not __x and __y)
    )
