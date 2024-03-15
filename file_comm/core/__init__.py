import os
from slime_core.utils.metabase import ReadonlyAttr
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


class ServerInfo(ReadonlyAttr):
    """
    Server information.
    """
    readonly_attr__ = (
        'address'
    )
    
    def __init__(
        self,
        address: str
    ) -> None:
        self.address = address
        self.main_fname = 'main.listen'

    @property
    def main_fp(self) -> str:
        return self.concat_server_fp(self.main_fname)

    def init_server(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.address, exist_ok=True)
        create_empty_file(self.main_fp)
    
    def concat_server_fp(self, fname: str) -> str:
        return os.path.join(self.address, fname)


class SessionInfo(ServerInfo, ReadonlyAttr):
    """
    Parse session file names.
    """
    readonly_attr__ = (
        'address',
        'session_id'
    )
    
    def __init__(
        self,
        address: str,
        session_id: str
    ) -> None:
        ServerInfo.__init__(self, address)
        self.session_id = session_id
        # Message transfer.
        self.server_fp = self.concat_session_fp('server.queue')
        self.client_fp = self.concat_session_fp('client.queue')
        # Connect creation.
        self.conn_server_fp = self.concat_session_fp('server.conn')
        self.conn_client_fp = self.concat_session_fp('client.conn')
        # Disconnect.
        self.disconn_server_fp = self.concat_session_fp('server.disconn')
        self.disconn_client_fp = self.concat_session_fp('client.disconn')
        # Heartbeat.
        self.heartbeat_server_fp = self.concat_session_fp('server.heartbeat')
        self.heartbeat_client_fp = self.concat_session_fp('client.heartbeat')
    
    @property
    def session_namespace(self) -> str:
        return os.path.join(self.address, self.session_id)
    
    def init_server(self):
        print(
            f'Warning: ``init_server`` should not be called in the ``SessionInfo``.'
        )
    
    def init_session(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.session_namespace, exist_ok=True)
        create_empty_file(self.server_fp)
        create_empty_file(self.client_fp)
    
    def concat_session_fp(self, fname: str) -> str:
        return os.path.join(self.session_namespace, fname)
    
    def message_output_fp(self, msg: Message) -> str:
        """
        Get the message output file path.
        """
        return self.concat_session_fp(msg.output_fname)
    
    def command_terminate_confirm_fp(self, msg: Message) -> str:
        """
        Get the command terminate confirm fp.
        """
        return self.concat_session_fp(msg.msg_id)


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
