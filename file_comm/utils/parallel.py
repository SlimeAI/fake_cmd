"""
Parallel utils.
"""
import os
from threading import RLock, Thread, Event
from abc import ABC, abstractmethod
from slime_core.utils.typing import (
    List,
    Union,
    TYPE_CHECKING,
    Callable
)
from . import polling
if TYPE_CHECKING:
    from file_comm.core.server import Command


class CommandWatchdog(Thread):
    """
    Run ``terminate_func`` after the ``command`` terminates.
    """
    def __init__(
        self,
        cmd: "Command",
        terminate_func: Callable[[], None]
    ) -> None:
        Thread.__init__(self)
        self.cmd = cmd
        self.terminate_func = terminate_func
    
    def run(self) -> None:
        self.cmd.start()
        self.cmd.join()
        self.terminate_func()


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
        Schedule new jobs.
        """
        with (
            self.queue_lock,
            self.execute_lock
        ):
            if (
                self.queue and 
                len(self.execute) < self.max_threads
            ):
                cmd = self.queue.pop(0)
                
                def terminate_func():
                    with self.execute_lock:
                        try:
                            self.execute.remove(watch)
                        except ValueError:
                            pass
                
                watch = CommandWatchdog(cmd, terminate_func)
                self.execute.append(watch)
                watch.start()
    
    def close(self):
        self.pool_close.set()
    
    def submit(self, command: "Command") -> bool:
        """
        Submit a command and return whether the command will be executed 
        immediately.
        """
        with (
            self.queue_lock,
            self.execute_lock
        ):
            self.queue.append(command)
            queued = (len(self.queue) + len(self.execute)) > self.max_threads
            if queued:
                command.queued.set()
                command.info_queued()
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


class LifecycleRun(ABC):
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
