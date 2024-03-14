import time
from slime_core.utils.typing import (
    MISSING
)


class Config:
    
    def __init__(self) -> None:
        self.polling_interval = 0.01


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
