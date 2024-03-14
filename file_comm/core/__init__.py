import os
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    Missing,
    MISSING,
    Union,
    Callable,
    Any
)
from utils.comm import Message
from utils.file import create_empty_file


class ServerFiles:
    """
    Parse server files with given address.
    """
    
    def __init__(
        self,
        address: str
    ) -> None:
        self.address = address
        self.main_fp = os.path.join(address, 'main.listen')

    def create(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.address, exist_ok=True)
        create_empty_file(self.main_fp)


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
        self.server_fp = os.path.join(self.session_path, 'server.queue')
        self.client_fp = os.path.join(self.session_path, 'client.queue')
        self.output_fp = os.path.join(self.session_path, 'output.content')
        # Connect creation.
        self.conn_server = os.path.join(self.session_path, 'server.conn')
        self.conn_client = os.path.join(self.session_path, 'client.conn')
        # Disconnect.
        self.disconn_server = os
    
    def create(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.session_path, exist_ok=True)
        create_empty_file(self.server_fp)
        create_empty_file(self.client_fp)
        create_empty_file(self.output_fp)


ActionFunc = Callable[[Any, Message], None]


def dispatch_action(
    registry: Registry[ActionFunc],
    type: str,
    caller: str
) -> Union[ActionFunc, Missing]:
    action = registry.get(type, MISSING)
    if action is MISSING:
        print(
            f'Warning: unsupported message type received in {caller}: {type}, '
            f'and it is ignored. Supported types: {tuple(registry.keys())}'
        )
    return action
