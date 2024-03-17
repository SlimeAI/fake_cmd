"""
File operations.
"""
import os
import time
import fcntl
from contextlib import contextmanager
from slime_core.utils.typing import (
    TextIO,
    Any,
    Missing,
    MISSING,
    Union,
    is_slime_naming,
    is_magic_naming
)
from . import polling, config


def get_lockfile_path(fp: str) -> str:
    """
    Get the lockfile name with given ``fp``
    """
    return f'{fp}.lockfile'


@contextmanager
def file_lock(fp: str, remove_lockfile: bool = False):
    """
    Acquire a file lock. If ``fp`` does not exist, then create a new 
    file. Remove the lock file if ``remove_lockfile`` is ``True``.
    
    NOTE: ``file_lock`` acquired by different functions will be blocked, 
    even the functions are called in the same process and the same thread.
    """
    lockfile_path = get_lockfile_path(fp)
    # The lock file should not be deleted to avoid inconsistency.
    lock_f = open(lockfile_path, 'a')
    fcntl.flock(lock_f, fcntl.LOCK_EX)
    # Use ``open(fp, 'a')`` to create a new file if ``fp`` does not exist.
    with lock_f, open(fp, 'a'):
        try:
            yield
        finally:
            if remove_lockfile:
                try:
                    os.remove(lockfile_path)
                except FileNotFoundError:
                    pass
            fcntl.flock(lock_f, fcntl.LOCK_UN)


def pop_first_line(fp: str) -> str:
    """
    Pop the first line of the file. Return an empty str if the line 
    is empty.
    """
    first_line: str = ''
    
    with file_lock(fp), open(fp, 'r') as f:
        first_line = f.readline().strip().strip('\n')
        if not first_line:
            # Empty content.
            return first_line

        temp_fp = f'{fp}.tmp'
        with file_lock(temp_fp, remove_lockfile=True):
            with open(temp_fp, 'w') as temp_f:
                for line in f:
                    # Read the remaining lines and write them to 
                    # the temp file.
                    temp_f.write(line)

            # Remove and rename. The remove and rename operation 
            # should be done under the two locks.
            try:
                os.remove(fp)
            except FileNotFoundError:
                pass
            
            try:
                os.rename(temp_fp, fp)
            except FileNotFoundError:
                pass
    
    return first_line


def pop_all(fp: str, remove: bool = False) -> str:
    """
    Pop all the items. If ``remove`` is ``True``, then remove the 
    file finally.
    """
    with file_lock(fp), open(fp, 'r') as f:
        content = f.read()
        if remove:
            try:
                os.remove(fp)
            except FileNotFoundError:
                pass
        else:
            # Clear the file.
            with open(fp, 'w'): pass
        return content


def append_line(fp: str, content: str):
    """
    Append a new line to the file.
    """
    with file_lock(fp), open(fp, 'a') as f:
        f.write(f'{content}\n')


def create_empty_file(fp: str, exist_ok: bool = False):
    """
    Create an empty file.
    """
    if os.path.exists(fp) and not exist_ok:
        return
    # Create a new empty file.
    with file_lock(fp), open(fp, 'w'): pass


def wait_file(fp: str, timeout: Union[float, Missing] = MISSING) -> bool:
    """
    Wait the file to be created until timeout. Return ``True`` if 
    the file is successfully created before timeout, else ``False``.
    """
    timeout = config.wait_timeout if timeout is MISSING else timeout
    
    start = time.time()
    for _ in polling():
        if os.path.exists(fp):
            return True
        
        stop = time.time()
        if (stop - start) > timeout:
            return False


def remove_file(fp: str, remove_lockfile: bool = False):
    """
    Remove a file.
    """
    with file_lock(fp, remove_lockfile):
        try:
            os.remove(fp)
        except FileNotFoundError:
            pass


class LockedTextIO:
    """
    TextIO with file lock.
    """
    
    def __init__(
        self,
        f: TextIO,
        fp: str
    ) -> None:
        self.f__ = f
        self.fp__ = fp
        self.attrs__ = {
            'write',
            'writelines',
            'flush'
        }
    
    def print__(self, content: str):
        """
        Print content with automatic new line.
        """
        return self.write(f'{content}\n')
    
    def write(self, *args, **kwds):
        with file_lock(self.fp__):
            res = self.f__.write(*args, **kwds)
            # Use flush to make it consistent.
            self.f__.flush()
            return res
    
    def writelines(self, *args, **kwds):
        with file_lock(self.fp__):
            res = self.f__.writelines(*args, **kwds)
            # Use flush to make it consistent.
            self.f__.flush()
            return res
    
    def flush(self, *args, **kwds):
        with file_lock(self.fp__):
            return self.f__.flush(*args, **kwds)
    
    def __enter__(self):
        self.f__.__enter__()
        # NOTE: should return ``self`` rather than 
        # ``self.f__`` for outside usage.
        return self
    
    def __exit__(self, *args, **kwds):
        return self.f__.__exit__(*args, **kwds)
    
    def __getattribute__(self, __name: str) -> Any:
        if (
            is_slime_naming(__name) or 
            is_magic_naming(__name)
        ):
            return super().__getattribute__(__name)
        if __name in self.attrs__:
            return super().__getattribute__(__name)
        return getattr(self.f__, __name)
