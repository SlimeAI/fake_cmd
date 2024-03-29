"""
Communication utils.
"""
import os
import json
import time
import uuid
import random
from itertools import filterfalse
from threading import RLock
from abc import ABC, abstractmethod, ABCMeta
from slime_core.utils.base import (
    BaseList
)
from slime_core.utils.metabase import (
    ReadonlyAttr,
    _ReadonlyAttrMetaclass
)
from slime_core.utils.metaclass import (
    Metaclasses
)
from slime_core.utils.typing import (
    Dict,
    Any,
    Union,
    Missing,
    MISSING,
    List,
    Literal,
    Iterable,
    Stop,
    STOP
)
from . import config
from .common import LessThanAnything, polling, timeout_loop, uuid_base36, xor__
from .exception import retry_deco
from .logging import logger
from .file import (
    remove_file_with_retry,
    check_single_writer_lock,
    single_writer_lock,
    SINGLE_WRITER_LOCK_FILE_EXTENSION
)

#
# Messages.
#

class Message(ReadonlyAttr):
    """
    A message object.
    """
    readonly_attr__ = (
        'session_id',
        'msg_id',
        'timestamp',
        'content',
        'type'
    )
    
    json_attrs = (
        'session_id',
        'type',
        'content',
        'timestamp',
        'msg_id'
    )
    # Separator to sep the send fname components. Used 
    # for file sorting.
    send_fname_sep = '__'
    
    def __init__(
        self,
        *,
        session_id: str,
        type: str,
        content: Union[dict, None] = None,
        timestamp: Union[float, None] = None,
        msg_id: Union[str, None] = None
    ) -> None:
        """
        - ``session_id``: The connection session id.
        - ``type``: The message type (for different actions).
        - ``content``: The message content.
        - ``timestamp``: Time when the message is created.
        - ``msg_id``: A unique message id.
        """
        self.session_id = session_id
        self.type = type
        if (
            content is not None and 
            not isinstance(content, dict)
        ):
            logger.warning(
                f'Message content can only be ``dict`` or ``None``, not {str(content)}.'
            )
            content = {}
        self.content = content
        self.timestamp = timestamp or time.time()
        self.msg_id = msg_id or uuid_base36(uuid.uuid4().int)
    
    @property
    def confirm_fname(self) -> str:
        """
        Message confirmation file name.
        """
        return f'{self.msg_id}.confirm'
    
    @property
    def send_fname(self) -> str:
        """
        Return the message send file name.
        """
        return (
            f'{str(self.timestamp)}{Message.send_fname_sep}'
            f'{self.msg_id}.msg'
        )
    
    @property
    def output_namespace(self) -> str:
        """
        System output redirect namespace.
        """
        return f'{self.msg_id}_output'
    
    def to_json(self) -> str:
        """
        Transfer to json str.
        """
        kwargs = {k:getattr(self, k, None) for k in self.json_attrs}
        return json.dumps(kwargs)
    
    @classmethod
    def from_json(cls, json_str: str):
        """
        Create a message object from json str.
        """
        kwargs: Dict[str, Any] = json.loads(json_str)
        return cls(**{k:kwargs.get(k, None) for k in cls.json_attrs})

    @classmethod
    def clone(cls, msg: "Message"):
        return cls.from_json(msg.to_json())

    def get_content_item(self, key: str, default: Any):
        """
        Get message content item with given key and default value.
        """
        if not self.content:
            return default
        return self.content.get(key, default)

    def __bool__(self) -> bool:
        return True


class CommandMessage(Message):
    """
    Create alias names of the message attributes for better understanding.
    """
    readonly_attr__ = ('interactive',)
    
    def __init__(
        self,
        *,
        session_id: str,
        type: str,
        content: Union[str, dict, list, None] = None,
        timestamp: Union[float, None] = None,
        msg_id: Union[str, None] = None
    ) -> None:
        super().__init__(
            session_id=session_id,
            type=type,
            content=content,
            timestamp=timestamp,
            msg_id=msg_id
        )
    
    @property
    def cmd_content(self) -> Union[str, None]:
        if (
            self.content is None or 
            'cmd' not in self.content
        ):
            return None
        return self.content['cmd']
    
    @property
    def cmd_id(self) -> str:
        return self.msg_id

    @property
    def interactive(self) -> bool:
        """
        Whether the command is executed in an interactive mode.
        """
        return self.get_content_item('interactive', False)
    
    @property
    def kill_disabled(self) -> bool:
        """
        Whether the command is disabled to kill using keyboard interrupt.
        """
        return self.get_content_item('kill_disabled', False)

#
# File Handlers.
#

class SequenceFileHandler(
    ABC,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    """
    Process files in a namespace with specified sorting method. 
    Best compatible with single-writer.
    """
    readonly_attr__ = (
        'namespace',
        'max_files'
    )
    
    def __init__(
        self,
        namespace: str,
        max_files: int
    ):
        self.namespace = namespace
        self.max_files = max_files
        # file path queue, for sequence read.
        self.fp_queue = BaseList[str]()
        self.fp_queue_lock = RLock()
        # read queue
        # Contain files that have been read to avoid 
        # repeated files.
        self.read_queue = BaseList[str]()
        self.read_queue_lock = RLock()
        self.read_queue_max_size = 100
        os.makedirs(namespace, exist_ok=True)
    
    def read_one(
        self,
        detect_new_files: bool = True
    ) -> Union[str, Literal[False]]:
        """
        Read one sequence file (if any).
        
        ``detect_new_files``: Whether to detect new files if 
        ``fp_queue`` is empty. If set to ``False``, then directly 
        return ``False`` if ``fp_queue`` is empty.
        """
        if not self.check_namespace():
            return False
        
        with self.fp_queue_lock, self.read_queue_lock:
            if len(self.fp_queue) == 0:
                if not detect_new_files:
                    # Directly return.
                    return False
                
                try:
                    self.detect_files()
                except Exception as e:
                    logger.error(str(e), stack_info=True)
                    return False
            
            if len(self.fp_queue) == 0:
                return False
            
            fp = self.fp_queue.pop(0)
            if not os.path.exists(fp):
                logger.warning(
                    f'Message file removed after sent: {fp}'
                )
                return False
            # Check repeated messages.
            if fp in self.read_queue:
                remove_file_with_retry(fp)
                return False
            
            with open(fp, 'r') as f:
                content = f.read()
                remove_file_with_retry(fp)
            
            if len(self.read_queue) >= self.read_queue_max_size:
                self.read_queue.pop(0)
            self.read_queue.append(fp)
            return content
    
    def read_all(
        self,
        timeout: Union[float, Missing] = MISSING
    ) -> str:
        """
        Read all the remaining content (until timeout).
        """
        content = ''
        detect_new_files = True
        start = time.time()
        while True:
            c = self.read_one(detect_new_files=detect_new_files)
            # NOTE: Use ``c is False`` rather than 
            # ``not c`` here, because some sequence 
            # files may contain empty content.
            if c is False:
                return content
            content += c
            stop = time.time()
            if (
                timeout is not MISSING and 
                (stop - start) > timeout
            ):
                # Set ``detect_new_files`` to ``False`` 
                # and only read from existing ``fp_queue``.
                detect_new_files = False
    
    def write(
        self,
        fname: str,
        content: str,
        exist_ok: bool = False,
        empty_ok: bool = False
    ) -> Union[bool, Missing, Stop]:
        """
        Safely write a file with single writer lock. 
        Return whether the writing operation succeeded.
        """
        if not self.check_namespace():
            return False
        
        # Optimization: Avoid writing empty content.
        if (
            not content and 
            not empty_ok
        ):
            return MISSING
        
        try:
            # Optimization: Avoid piling up too many files in 
            # the folder.
            num_files = len(os.listdir(self.namespace))
            if num_files >= self.max_files:
                return STOP
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        
        fp = self.get_fp(fname)
        if (
            os.path.exists(fp) and 
            not exist_ok
        ):
            return False
        
        try:
            with single_writer_lock(fp), open(fp, 'w') as f:
                f.write(content)
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        
        return True
    
    def get_fp(self, fname: str) -> str:
        """
        Get full file path given ``fname``.
        """
        return os.path.join(self.namespace, fname)
    
    def detect_files(self) -> None:
        """
        Detect and sort new files in the namespace, and add them 
        to ``fp_queue``.
        """
        if not self.check_namespace():
            return
        
        with self.fp_queue_lock:
            # Filter writer lock files.
            fname_iter = filter(
                lambda fname: not fname.endswith(SINGLE_WRITER_LOCK_FILE_EXTENSION),
                os.listdir(self.namespace)
            )
            # Sort the file names.
            fname_list = self.sort(fname_iter)
            # Get the full file paths and filter files with writer lock.
            self.fp_queue.extend(
                filterfalse(check_single_writer_lock, map(self.get_fp, fname_list))
            )
    
    @abstractmethod
    def sort(self, fname_iter: Iterable[str]) -> List[str]:
        """
        Return the sorted sequence of ``fname_iter``.
        """
        pass
    
    def check_namespace(self, silent: bool = True) -> bool:
        """
        Check whether the namespace exists.
        """
        namespace_exists = os.path.exists(self.namespace)
        if not namespace_exists and not silent:
            logger.warning(
                f'Namespace "{self.namespace}" does not exists.'
            )
        return namespace_exists


class OutputFileHandler(SequenceFileHandler):
    """
    OutputFileHandler is used for stdout and stderr content 
    writing/reading.
    """
    # Separator to separate the fname components. Used 
    # for file sorting.
    fname_sep = '__'
    
    def __init__(self, namespace: str):
        SequenceFileHandler.__init__(
            self,
            namespace=namespace,
            max_files=config.max_output_files
        )
    
    def write(self, content: str, exist_ok: bool = False):
        """
        Write a new file with timestamp name.
        """
        return super().write(
            fname=self.gen_fname(),
            content=content,
            exist_ok=exist_ok
        )
    
    def print(self, content: str, exist_ok: bool = False):
        return self.write(f'{content}\n', exist_ok=exist_ok)
    
    def sort(self, fname_iter: Iterable[str]) -> List[str]:
        """
        Sort the output file names by timestamps.
        """
        def get_timestamp(fname: str) -> float:
            """
            Get timestamp from the file path.
            """
            return float(fname.split(OutputFileHandler.fname_sep)[0])
        
        def filter_valid_fname(fname: str) -> bool:
            """
            Only keep the valid fname.
            """
            try:
                get_timestamp(fname)
            except Exception:
                remove_file_with_retry(self.get_fp(fname))
                return False
            else:
                return True
        
        return sorted(
            filter(filter_valid_fname, fname_iter),
            key=lambda fp: get_timestamp(fp)
        )
    
    def gen_fname(self) -> str:
        """
        Generate a unique file name with timestamp for sorting.
        """
        # The order of output files strongly rely on the accurate time, 
        # so we use ``time.time_ns`` here.
        return (
            f'{time.time_ns()}{OutputFileHandler.fname_sep}'
            f'{uuid_base36(uuid.uuid4().int)}.out'
        )


class MessageHandler(SequenceFileHandler):
    """
    MessageHandler that is responsible for safe message reading and 
    sending.
    """
    
    def __init__(
        self,
        namespace: str,
        max_retries: Union[int, Missing] = MISSING,
        wait_timeout: Union[float, Missing] = MISSING
    ) -> None:
        SequenceFileHandler.__init__(
            self,
            namespace=namespace,
            max_files=config.max_message_files
        )
        self.max_retries = (
            max_retries if 
            max_retries is not MISSING else 
            config.msg_send_retries
        )
        self.wait_timeout = (
            wait_timeout if 
            wait_timeout is not MISSING else 
            config.msg_confirm_wait_timeout
        )
    
    def listen(self):
        """
        Endlessly listen messages in blocking mode.
        """
        for _ in polling():
            msg = self.read_one()
            if msg:
                yield msg
    
    def read_one(self) -> Union[Message, Literal[False]]:
        """
        Pop a new message (if any). Return ``False`` if no new messages.
        """
        content = super().read_one()
        if not content:
            return False
        try:
            # If the json decode fails (mostly because of 
            # file read and written at the same time, causing 
            # file inconsistency), directly return ``False``. 
            # Because the message will be re-sent if no confirm 
            # file is created, the consistency is ensured.
            msg = Message.from_json(content)
        except Exception as e:
            logger.error(str(e), stack_info=True)
            return False
        # Create a confirmation symbol.
        create_symbol(self.get_fp(msg.confirm_fname))
        return msg
    
    def write(self, msg: Message) -> bool:
        """
        Send msg to namespace. Retry when the confirm symbol is not received, 
        until ``max_retries`` times. Return whether the message is successfully 
        received.
        """
        if not self.check_namespace(silent=False):
            return False
        
        attempt = 0
        msg_json = msg.to_json()
        max_retries = self.max_retries
        send_fp = self.get_fp(msg.send_fname)
        confirm_fp = self.get_fp(msg.confirm_fname)
        while True:
            # Avoid sending the same message.
            if not os.path.exists(send_fp):
                super().write(
                    msg.send_fname,
                    msg_json,
                    exist_ok=False
                )
            
            if wait_symbol(
                confirm_fp,
                timeout=self.wait_timeout
            ):
                return True
            
            # If wait symbol is False, then retry.
            if attempt >= max_retries:
                logger.warning(
                    f'Message sent {attempt} times, but not responded. Expected confirm file: '
                    f'{confirm_fp}. Message content: {msg_json}.'
                )
                return False
            
            attempt += 1
            logger.warning(
                f'Retrying sending the message: {msg_json}'
            )
    
    def sort(self, fname_iter: Iterable[str]) -> List[str]:
        """
        Sort the message file names by timestamps.
        """
        def get_timestamp(fname: str) -> float:
            """
            Get timestamp from the file path.
            """
            return float(fname.split(Message.send_fname_sep)[0])
        
        def filter_valid_fname(fname: str) -> bool:
            """
            Only keep the valid fname.
            """
            try:
                get_timestamp(fname)
            except Exception:
                remove_file_with_retry(self.get_fp(fname))
                return False
            else:
                return True
        
        return sorted(
            filter(filter_valid_fname, fname_iter),
            key=lambda fp: get_timestamp(fp)
        )

#
# Symbol operations.
#

@retry_deco(suppress_exc=Exception)
def create_symbol(fp: str):
    """
    Create a symbol file. If ``fp`` exists, then do nothing.
    """
    if os.path.exists(fp):
        return
    
    with single_writer_lock(fp), open(fp, 'a'):
        pass


def wait_symbol(
    fp: str,
    timeout: Union[float, Missing] = MISSING,
    wait_for_remove: bool = False
) -> bool:
    """
    Wait a symbol file. Return ``True`` if the symbol is created 
    before timeout, otherwise ``False``.
    
    ``wait_for_remove``: whether the function is used to wait for 
    removing the symbol or creating the symbol.
    """
    timeout = config.symbol_wait_timeout if timeout is MISSING else timeout
    
    for _ in timeout_loop(
        timeout,
        interval=config.polling_interval
    ):
        if xor__(
            os.path.exists(fp),
            wait_for_remove
        ):
            remove_symbol(fp)
            return True
    return False


def remove_symbol(fp: str) -> None:
    """
    Remove a symbol file.
    """
    if check_single_writer_lock(fp):
        # If the file has a single writer lock, sleep a little 
        # amount of time and then remove.
        time.sleep(config.symbol_remove_timeout)
    remove_file_with_retry(fp)


def check_symbol(fp: str) -> bool:
    """
    Check if the symbol exists. Non-blocking form of ``wait_symbol``.
    """
    if os.path.exists(fp):
        remove_symbol(fp)
        return True
    else:
        return False

#
# Connection API.
#

class Connection(ABC):

    @abstractmethod
    def connect(self) -> bool:
        """
        Three-way handshake to connect.
        """
        pass

    @abstractmethod
    def disconnect(self, initiator: bool):
        """
        Four-way handshake to disconnect. ``initiator``: whether 
        the disconnection is initiated locally.
        """
        pass
    
    @abstractmethod
    def check_connection(self) -> bool:
        """
        Check the connection state, decide whether to exit 
        and perform corresponding exit operations.
        """
        pass

#
# Heartbeat services.
#

class Heartbeat(ReadonlyAttr):
    """
    Used heartbeat to confirm the connection is still alive.
    """
    readonly_attr__ = (
        'receive_fp',
        'send_fp',
        'min_interval',
        'max_interval',
        'timeout'
    )
    
    def __init__(
        self,
        receive_fp: str,
        send_fp: str
    ) -> None:
        self.receive_fp: str = receive_fp
        self.last_receive: Union[float, None] = None
        self.send_fp: str = send_fp
        # Initialized to ``LessThanAnything``.
        self.last_beat: Union[float, LessThanAnything] = LessThanAnything()
        self.min_interval: float = config.heartbeat_min_interval
        self.max_interval: float = config.heartbeat_max_interval
        self.timeout: float = config.heartbeat_timeout
        self.set_interval()
    
    def beat(self) -> bool:
        now = time.time()
        if self.last_receive is None:
            # Set ``last_receive`` to now if it is None, because 
            # if the heartbeat never receives and ``last_receive`` 
            # is always None, we will never know when the timeout 
            # reaches. Set here rather than in the ``__init__``, 
            # because there may be a period of time between the 
            # Heartbeat object creation and the first beat (although 
            # most of the time it is short).
            self.last_receive = now
        
        if self.last_beat > (now - self.interval):
            # If within the interval, do not check and directly 
            # return True.
            return True
        
        create_symbol(self.send_fp)
        received = check_symbol(self.receive_fp)
        if received:
            self.last_receive = now
        elif (now - self.last_receive) > self.timeout:
            logger.warning(
                f'Heartbeat time out at {self.receive_fp}.'
            )
            return False

        self.last_beat = now
        # Set a new random interval after beat.
        self.set_interval()
        return True
    
    def set_interval(self) -> float:
        """
        Set a random interval between min and max.
        """
        self.interval = random.uniform(
            self.min_interval,
            self.max_interval
        )
