"""
Parallel utils.
"""
import os
from threading import RLock, Thread, Event
from abc import ABC, abstractmethod
from slime_core.utils.base import BaseList
from slime_core.utils.typing import (
    List,
    Union,
    TYPE_CHECKING,
    Callable,
    Iterable
)
from . import polling
if TYPE_CHECKING:
    from file_comm.core.server import Command


ExitCallbackFunc = Callable[[], None]


class ExitCallbacks:
    
    def __init__(
        self,
        callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ) -> None:
        self.exit_callbacks__ = BaseList(callbacks)
    
    def run_exit_callbacks__(self):
        for callback in self.exit_callbacks__:
            callback()


class CommandWatchdog(ExitCallbacks, Thread):
    """
    Run ``terminate_func`` after the ``command`` terminates.
    """
    def __init__(
        self,
        cmd: "Command",
        exit_callbacks: Union[Iterable[ExitCallbackFunc], None] = None
    ) -> None:
        ExitCallbacks.__init__(self, exit_callbacks)
        Thread.__init__(self)
        self.cmd = cmd
    
    def run(self) -> None:
        self.cmd.start()
        self.cmd.join()
        self.run_exit_callbacks__()


class CommandPool:
    
    def __init__(
        self,
        max_threads: Union[int, None] = None
    ) -> None:
        self.queue: List["Command"] = []
        self.queue_lock = RLock()
        self.execute: List[CommandWatchdog] = []
        self.execute_lock = RLock()
        self.max_threads = max_threads or os.cpu_count() or 1
        self.pool_close = Event()
        self.polling_thread = Thread(target=self.run)
        self.polling_thread.start()
    
    def run(self):
        for _ in polling(1.0):
            if self.pool_close.is_set():
                break
            
            self.schedule()
        
        for t in self.execute:
            t.join()
    
    def schedule(self):
        """
        Schedule new jobs (in batch).
        """
        with (
            self.queue_lock,
            self.execute_lock
        ):
            while (
                self.queue and 
                len(self.execute) < self.max_threads
            ):
                cmd = self.queue.pop(0)
                if cmd.cmd_state.pending_terminate:
                    continue
                
                def terminate_func():
                    with self.execute_lock:
                        try:
                            self.execute.remove(watch)
                        except ValueError:
                            pass
                
                watch = CommandWatchdog(
                    cmd,
                    exit_callbacks=[terminate_func]
                )
                self.execute.append(watch)
                watch.start()
    
    def close(self):
        self.pool_close.set()
    
    def submit(self, cmd: "Command") -> bool:
        """
        Submit a command and return whether the command will be executed 
        immediately.
        """
        with (
            self.queue_lock,
            self.execute_lock
        ):
            self.queue.append(cmd)
            queued = (len(self.queue) + len(self.execute)) > self.max_threads
            if queued:
                cmd.cmd_state.queued.set()
                cmd.info_queued()
            return (not queued)
    
    def cancel(self, command: "Command") -> bool:
        with self.queue_lock:
            if command not in self.queue:
                return False
            try:
                self.queue.remove(command)
            except ValueError:
                pass
            return True


class LifecycleRun(ExitCallbacks, ABC):
    """
    Separate the running process into ``before_running``, 
    ``running`` and ``after_running``.
    """
    def run(self):
        """
        This method may not be overridden.
        """
        if self.before_running():
            try:
                self.running()
            finally:
                self.run_exit_callbacks__()
                self.after_running()
    
    @abstractmethod
    def running(self): pass

    @abstractmethod
    def before_running(self) -> bool:
        """
        Execute before running. Return ``False`` to stop the 
        following ``running`` operation.
        """
        return True
    
    @abstractmethod
    def after_running(self): pass
