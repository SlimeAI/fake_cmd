import time
import selectors
import subprocess
from slime_core.utils.registry import Registry
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.typing import (
    Type,
    Union,
    Missing,
    MISSING
)
from fake_cmd.utils.logging import logger
from . import ExecutorComponent, PlatformPopenExecutor

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

#
# Popen Readers
#

popen_reader_registry = Registry[Type["PopenReader"]]('popen_reader_registry')


@popen_reader_registry(key='none')
class PopenReader(ExecutorComponent[PlatformPopenExecutor]):
    """
    Interface to read output of the popen process.
    """
    
    readonly_attr__ = ('executor',)
    
    def __init__(self) -> None:
        ExecutorComponent.__init__(self)
    
    @property
    def encoding(self) -> str:
        return self.executor.encoding
    
    def read_init(self) -> None:
        """
        This method should be called when the executor starts, 
        and it notifies the reader that the Popen object has 
        been created and the stdout as well as stderr can be 
        accessed.
        """
        pass
    
    def read(self, timeout: float) -> str:
        """
        Read the content within the timeout. This method 
        concat the results of ``_selector_read_one`` within 
        the timeout and return.
        """
        return ''
    
    def read_all(self) -> str:
        """
        Read all the content util file end. Make sure the 
        process is end when you call ``read_all``, else the 
        read will be blocked.
        """
        return ''
    
    def get_popen_stdout(self):
        """
        Get the stdout argument passed to ``Popen``.
        """
        return None
    
    def get_popen_stderr(self):
        """
        Get the stderr argument passed to ``Popen``.
        """
        return None


@popen_reader_registry(key='default')
class PipePopenReader(PopenReader):
    """
    Non-blocking read stdout pipes within timeout.
    """
    readonly_attr__ = (
        'selector',
        'bytes_parser'
    )

    def __init__(self) -> None:
        PopenReader.__init__(self)
        # NOTE: Use ``selector`` to get non-blocking outputs.
        self.selector = selectors.DefaultSelector()
        self.bytes_parser: Union[StreamBytesParser, Missing] = MISSING
    
    def bind(self, executor: PlatformPopenExecutor) -> None:
        super().bind(executor)
        # The encoding attribute will be available after binding, so the 
        # ``bytes_parser`` object should be created here.
        self.bytes_parser = StreamBytesParser(encoding=self.encoding)

    def read_init(self) -> None:
        self.selector.register(self.executor.stdout, selectors.EVENT_READ)

    def read(self, timeout: float) -> str:
        start = time.time()
        content = ''
        while True:
            content += self._selector_read_one()
            stop = time.time()
            if (stop - start) > timeout:
                break
        return content

    def read_all(self) -> str:
        # Non-blocking mode.
        stdout = self.executor.stdout
        for key, _ in self.selector.select(0):
            if key.fileobj == stdout:
                # If ready, read all and return.
                try:
                    stdout.flush()
                except Exception as e:
                    logger.error(str(e), stack_info=True)
                return self.bytes_parser.parse(stdout.read())
        # If not ready, directly return empty str.
        return ''

    def _selector_read_one(self) -> str:
        """
        Read one char if selector is ready.
        """
        # Non-blocking mode.
        stdout = self.executor.stdout
        for key, _ in self.selector.select(0):
            if key.fileobj == stdout:
                # If ready, read one char and return.
                return self.bytes_parser.parse(stdout.read(1))
        # If no output, directly return empty str.
        return ''
    
    def get_popen_stdout(self):
        return subprocess.PIPE
    
    def get_popen_stderr(self):
        return subprocess.STDOUT

#
# Pexpect Readers
#

class PexpectReader(ExecutorComponent):
    pass

