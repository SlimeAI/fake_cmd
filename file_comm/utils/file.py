"""
File operations.
"""
import os
import shutil
from contextlib import contextmanager, suppress
from .exception import retry_deco


@retry_deco(suppress_exc=Exception)
def remove_file_with_retry(fp: str):
    try:
        os.remove(fp)
    except FileNotFoundError:
        pass


@retry_deco(suppress_exc=Exception)
def remove_dir_with_retry(namespace: str):
    try:
        shutil.rmtree(namespace)
    except FileNotFoundError:
        pass


SINGLE_WRITER_LOCK_FILE_EXTENSION = 'writing'


def get_single_writer_lock_fp(fp: str) -> str:
    """
    Return the writing lock file path.
    """
    return f'{fp}.{SINGLE_WRITER_LOCK_FILE_EXTENSION}'


@contextmanager
def single_writer_lock(fp: str):
    """
    Manually implementing lockfile with single-writer and 
    multi-reader.
    
    NOTE: ``fp`` should be a file path rather than a directory.
    """
    lock_fp = get_single_writer_lock_fp(fp)
    # Create writing lock.
    with suppress(Exception), open(lock_fp, 'a'): pass
    try:
        yield
    finally:
        # Remove writing lock.
        remove_file_with_retry(lock_fp)


def check_single_writer_lock(fp: str) -> bool:
    """
    Check whether the writing lock of ``fp`` exists. 
    Return ``True`` if the lock exists.
    """
    return os.path.exists(get_single_writer_lock_fp(fp))
