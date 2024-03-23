import os
import signal
from subprocess import Popen
from abc import ABC, abstractmethod, ABCMeta
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.metaclass import (
    Metaclasses,
    _ReadonlyAttrMetaclass
)
from slime_core.utils.registry import Registry
from slime_core.utils.common import FuncParams
from slime_core.utils.typing import (
    Type
)


class PlatformPopen(
    ABC,
    ReadonlyAttr,
    metaclass=Metaclasses(ABCMeta, _ReadonlyAttrMetaclass)
):
    readonly_attr__ = ('popen_obj',)
    
    def __init__(self, popen_params: FuncParams) -> None:
        self.popen_obj = Popen(*popen_params.args, **popen_params.kwargs)
    
    @property
    def stdout(self):
        return self.popen_obj.stdout
    
    @property
    def stdin(self):
        return self.popen_obj.stdin
    
    @property
    def stderr(self):
        return self.popen_obj.stderr
    
    @property
    def pid(self):
        return self.popen_obj.pid
    
    @property
    def poll(self):
        return self.popen_obj.poll
    
    @property
    def returncode(self):
        return self.popen_obj.returncode
    
    def keyboard_interrupt(self):
        """
        Send keyboard interrupt.
        """
        self.send_signal(signal.SIGINT)
    
    def terminate(self):
        """
        Terminate the process.
        """
        self.send_signal(signal.SIGTERM)
    
    def kill(self):
        """
        Kill the process.
        """
        if hasattr(signal, 'SIGKILL'):
            self.send_signal(signal.SIGKILL)
        else:
            self.terminate()
    
    @abstractmethod
    def send_signal(self, sig):
        """
        Send signal to the process.
        """
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args, **kwargs):
        return self.popen_obj.__exit__(*args, **kwargs)


platform_open_registry = Registry[Type[PlatformPopen]]('platform_open_registry')


@platform_open_registry(key='default')
class DefaultPopen(PlatformPopen):
    
    def send_signal(self, sig):
        """
        Safely send signal according to subprocess.Popen.send_signal.
        """
        self.poll()
        if self.returncode is not None:
            return
        
        # Try to kill the progress.
        try:
            os.kill(self.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass


@platform_open_registry(key='unix')
class UnixPopen(PlatformPopen):
    
    def __init__(self, popen_params: FuncParams) -> None:
        # Use ``start_new_session`` to create a new process group.
        platform_kwargs = {
            'start_new_session': True
        }
        platform_kwargs.update(
            popen_params.kwargs
        )
        popen_params.kwargs = platform_kwargs
        super().__init__(popen_params)

    def send_signal(self, sig):
        """
        Safely send signal according to subprocess.Popen.send_signal.
        """
        self.poll()
        if self.returncode is not None:
            return
        
        # Try to kill both progress and progress group in Unix.
        try:
            os.kill(self.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass
        
        try:
            os.killpg(self.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass
