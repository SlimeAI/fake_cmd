import os
import time
from slime_core.utils.typing import (
    MISSING
)


class Config:
    
    def __init__(self) -> None:
        self.polling_interval = 0.1
        self.cmd_pool_schedule_interval = 0.5
        # Server-Client response timeout.
        self.wait_timeout = 30.0
        # Command terminate wait timeout.
        self.cmd_terminate_timeout = 10.0
        self.heart_beat_interval = 3.0
        self.heart_beat_timeout = 10.0


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
