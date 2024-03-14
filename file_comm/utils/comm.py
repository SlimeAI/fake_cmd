"""
Communication utils.
"""
import os
import json
import time
import uuid
from slime_core.utils.metabase import (
    ReadonlyAttr
)
from slime_core.utils.typing import (
    Dict,
    Any,
    Union
)
from . import polling
from .file import (
    pop_first_line,
    append_line,
    wait_file,
    create_empty_file,
    remove_file,
    remove_file_and_lock
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
    
    def to_json(self) -> str:
        kwds = {k:getattr(self, k, None) for k in self.json_attrs}
        return json.dumps(kwds)
    
    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        kwds: Dict[str, Any] = json.loads(json_str)
        return cls(**{k:kwds.get(k, None) for k in cls.json_attrs})


def listen_messages(fp: str, confirm_path: str):
    """
    Endlessly listen new messages. If no new messages, then block.
    
    ``confirm_path``: the path to send the confirm symbol.
    """
    for _ in polling():
        message = pop_first_line(fp)
        if message:
            msg = Message.from_json(message)
            # Create a confirmation symbol.
            create_symbol(os.path.join(confirm_path, msg.confirm_file_name))
            yield 


def send_message(fp: str, msg: Message, confirm_path: str, max_retries: int = 3):
    """
    Send a new message to ``fp``. Retry when the confirm symbol is not received, 
    until ``max_retries`` times.
    """
    attempt = 0
    msg_confirm_fp = os.path.join(confirm_path, msg.confirm_file_name)
    msg_json = msg.to_json()
    while True:
        attempt += 1
        append_line(fp, msg_json)
        if wait_symbol(msg_confirm_fp, remove_lockfile=True):
            break
        if attempt >= max_retries:
            print(
                f'Message sent {attempt} times, but not responded. Expected confirm file: '
                f'{msg_confirm_fp}. Message content: {msg_json}.'
            )
            break


def create_symbol(fp: str):
    """
    Create a symbol file.
    """
    return create_empty_file(fp, True)


def wait_symbol(
    fp: str,
    timeout: int = 5,
    remove_lockfile: bool = False
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


def remove_symbol(fp: str, remove_lockfile: bool = False):
    """
    Remove a symbol file.
    
    If ``remove_lockfile`` is ``True``, then clean the corresponding lockfile 
    at the same time.
    """
    if remove_lockfile:
        remove_file_and_lock(fp)
    else:
        remove_file(fp)
