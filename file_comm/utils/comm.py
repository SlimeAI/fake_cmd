"""
Communication utils.
"""
import os
import json
import time
import uuid
from abc import ABC, abstractmethod
from slime_core.utils.metabase import (
    ReadonlyAttr
)
from slime_core.utils.typing import (
    Dict,
    Any,
    Union
)
from . import polling, config
from .file import (
    pop_first_line,
    append_line,
    wait_file,
    create_empty_file,
    remove_file
)


class Message(ReadonlyAttr):
    """
    A message object.
    """
    readonly_attr__ = (
        'session_id',
        'msg_id'
    )
    
    json_attrs = (
        'session_id',
        'type',
        'content',
        'timestamp',
        'msg_id'
    )
    
    def __init__(
        self,
        *,
        session_id: str,
        type: str,
        content: Union[str, None] = None,
        timestamp: Union[float, None] = None,
        msg_id: Union[str, None] = None
    ) -> None:
        self.session_id = session_id
        self.type = type
        self.content = content
        self.timestamp = timestamp or time.time()
        self.msg_id = msg_id or str(uuid.uuid4())
    
    @property
    def confirm_file_name(self) -> str:
        """
        Message confirmation file name.
        """
        return f'{self.msg_id}.confirm'
    
    @property
    def output_file_name(self) -> str:
        """
        System output redirect file name.
        """
        return f'{self.msg_id}.output'
    
    def to_json(self) -> str:
        kwds = {k:getattr(self, k, None) for k in self.json_attrs}
        return json.dumps(kwds)
    
    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        kwds: Dict[str, Any] = json.loads(json_str)
        return cls(**{k:kwds.get(k, None) for k in cls.json_attrs})

    def __bool__(self) -> bool:
        return True


def listen_messages(fp: str, confirm_path: str):
    """
    Endlessly listen new messages. If no new messages, then block.
    
    ``confirm_path``: the path to send the confirm symbol.
    """
    for _ in polling():
        msg = pop_message(fp, confirm_path)
        if msg:
            yield msg


def pop_message(fp: str, confirm_path: str) -> Union[Message, bool]:
    """
    Pop a new message (if any). Return ``False`` if no new messages.
    """
    message = pop_first_line(fp)
    if not message:
        return False
    msg = Message.from_json(message)
    # Create a confirmation symbol.
    create_symbol(os.path.join(confirm_path, msg.confirm_file_name))
    return msg


def send_message(
    fp: str,
    msg: Message,
    confirm_path: str,
    max_retries: int = 3
) -> bool:
    """
    Send a new message to ``fp``. Retry when the confirm symbol is not received, 
    until ``max_retries`` times. Return whether the message is successfully received.
    """
    attempt = 0
    msg_confirm_fp = os.path.join(confirm_path, msg.confirm_file_name)
    msg_json = msg.to_json()
    while True:
        attempt += 1
        if attempt > 1:
            print(
                f'Retrying sending the message: {msg_json}'
            )
        
        append_line(fp, msg_json)
        if wait_symbol(msg_confirm_fp, remove_lockfile=True):
            return True
        
        if attempt >= max_retries:
            print(
                f'Message sent {attempt} times, but not responded. Expected confirm file: '
                f'{msg_confirm_fp}. Message content: {msg_json}.'
            )
            return False


def create_symbol(fp: str):
    """
    Create a symbol file.
    """
    return create_empty_file(fp, True)


def wait_symbol(
    fp: str,
    timeout: int = 5,
    remove_lockfile: bool = True
) -> bool:
    """
    Wait a symbol file. Return ``True`` if the symbol is created 
    before timeout, otherwise ``False``.
    
    If the symbol file is a one-time symbol, then set ``remove_lockfile`` 
    to ``True`` to clean the corresponding lockfile.
    """
    if wait_file(fp, timeout):
        remove_symbol(fp, remove_lockfile)
        return True
    return False


def remove_symbol(fp: str, remove_lockfile: bool = True):
    """
    Remove a symbol file.
    
    If ``remove_lockfile`` is ``True``, then clean the corresponding lockfile 
    at the same time.
    """
    remove_file(fp, remove_lockfile)


def check_symbol(fp: str, remove_lockfile: bool = True) -> bool:
    """
    Check if the symbol exists. Non-block form of ``wait_symbol``.
    """
    if os.path.exists(fp):
        remove_symbol(fp, remove_lockfile)
        return True
    else:
        return False


class Connection(ABC):

    @abstractmethod
    def connect(self) -> bool: pass

    @abstractmethod
    def disconnect(self, initiator: bool): pass


class Heartbeat:
    
    def __init__(
        self,
        receive_fp: str,
        send_fp: str
    ) -> None:
        self.receive_fp = receive_fp
        self.last_receive = None
        self.send_fp = send_fp
        self.last_send = None
        self.interval = config.heart_beat_interval
        self.timeout = config.heart_beat_timeout
    
    def beat(self) -> bool:
        now = time.time()
        received = check_symbol(self.receive_fp)
        if (
            not received and 
            self.last_receive is not None and 
            (now - self.last_receive) > self.timeout
        ):
            print(
                f'Heartbeat time out at {self.receive_fp}.'
            )
            return False
        
        if received:
            self.last_receive = now
        
        if (
            self.last_send is None or 
            (now - self.last_send) >= self.interval
        ):
            create_symbol(self.send_fp)
            self.last_send = now
        
        return True
