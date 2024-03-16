import time
from slime_core.utils.typing import (
    MISSING
)


class Config:
    
    def __init__(self) -> None:
        self.polling_interval = 0.1
        self.cmd_pool_schedule_interval = 0.5
        self.wait_timeout = 5.0
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
