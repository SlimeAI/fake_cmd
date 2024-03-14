import os
from abc import ABC, abstractmethod
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    Missing,
    MISSING,
    Union,
    Callable,
    Any
)
from file_comm.utils.comm import Message
from file_comm.utils.file import create_empty_file


class ServerFiles:
    """
    Parse server files with given address.
    """
    
    def __init__(
        self,
        address: str
    ) -> None:
        self.address = address
        self.main_fp = self.concat_fp('main.listen')

    def create(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.address, exist_ok=True)
        create_empty_file(self.main_fp)
    
    def concat_fp(self, fp: str) -> str:
        return os.path.join(self.address, fp)


class SessionFiles:
    """
    Parse session files with given address and session id.
    """
    def __init__(
        self,
        address: str,
        session_id: str
    ) -> None:
        self.session_path = os.path.join(address, session_id)
        # Message transfer.
        self.server_fp = self.concat_fp('server.queue')
        self.client_fp = self.concat_fp('client.queue')
        # Connect creation.
        self.conn_server_fp = self.concat_fp('server.conn')
        self.conn_client_fp = self.concat_fp('client.conn')
        # Disconnect.
        self.disconn_server_fp = self.concat_fp('server.disconn')
        self.disconn_client_fp = self.concat_fp('client.disconn')
        # Heartbeat.
    
    def create(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.session_path, exist_ok=True)
        create_empty_file(self.server_fp)
        create_empty_file(self.client_fp)
    
    def concat_fp(self, fp: str) -> str:
        return os.path.join(self.session_path, fp)


ActionFunc = Callable[[Any, Message], None]


def dispatch_action(
    registry: Registry[ActionFunc],
    type: str,
    caller: str
) -> Union[ActionFunc, Missing]:
    """
    Dispatch the cations with given msg types.
    """
    action = registry.get(type, MISSING)
    if action is MISSING:
        print(
            f'Warning: unsupported message type received in {caller}: {type}, '
            f'and it is ignored. Supported types: {tuple(registry.keys())}'
        )
    return action


class Connection(ABC):

    @abstractmethod
    def connect(self) -> bool: pass
    
    @abstractmethod
    def disconnect(self): pass
    
    @abstractmethod
    def heartbeat(self): pass
