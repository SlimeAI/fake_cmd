import os
import shutil
from slime_core.utils.metabase import ReadonlyAttr
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    Missing,
    MISSING,
    Union,
    Callable,
    Any
)
from file_comm.utils.comm import Message, CommandMessage
from file_comm.utils.file import create_empty_file, remove_file, file_lock
from file_comm.utils.logging import logger


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
        # Used as a file lock to check if another server is listening 
        # the same address.
        self.main_check = 'main.check'

    @property
    def main_fp(self) -> str:
        return self.concat_server_fp(self.main_fname)

    @property
    def main_check_fp(self) -> str:
        return self.concat_server_fp(self.main_check)

    def init_server(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.address, exist_ok=True)
        create_empty_file(self.main_fp)
    
    def clear_server(self):
        """
        Clear server listening files.
        """
        with file_lock(self.main_check_fp):
            remove_file(self.main_fp, remove_lockfile=True)
        remove_file(self.main_check_fp, remove_lockfile=True)
        try:
            # If empty, then remove.
            os.rmdir(self.address)
        except (OSError, FileNotFoundError):
            pass
    
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
        self.disconn_confirm_to_server_fp = self.concat_session_fp('server.disconn.confirm')
        self.disconn_client_fp = self.concat_session_fp('client.disconn')
        self.disconn_confirm_to_client_fp = self.concat_session_fp('client.disconn.confirm')
        # Heartbeat.
        self.heartbeat_server_fp = self.concat_session_fp('server.heartbeat')
        self.heartbeat_client_fp = self.concat_session_fp('client.heartbeat')
        # Clear lock.
        # This lock is used to ensure the corresponding namespace will be 
        # completely cleared after the session is destroyed, and the 
        # destruction won't clear the reading files.
        self.clear_lock_fp = self.concat_session_fp('clear.lock')
    
    @property
    def session_namespace(self) -> str:
        return os.path.join(self.address, self.session_id)
    
    def init_server(self):
        logger.warning(
            f'``init_server`` should not be called in the ``SessionInfo``.'
        )
    
    def clear_server(self):
        logger.warning(
            f'``clear_server`` should not be called in the ``SessionInfo``.'
        )
    
    def init_session(self):
        """
        Create corresponding files. Should only be called once by server.
        """
        os.makedirs(self.session_namespace, exist_ok=True)
        create_empty_file(self.server_fp)
        create_empty_file(self.client_fp)
    
    def clear_session(self):
        """
        Clear the session files.
        """
        try:
            shutil.rmtree(self.session_namespace)
        except FileNotFoundError:
            pass
    
    def concat_session_fp(self, fname: str) -> str:
        return os.path.join(self.session_namespace, fname)
    
    def message_output_fp(self, msg: Message) -> str:
        """
        Get the message output file path.
        """
        return self.concat_session_fp(msg.output_fname)
    
    def command_terminate_confirm_fp(self, msg: CommandMessage) -> str:
        """
        Get the command terminate confirm fp.
        """
        return self.concat_session_fp(msg.cmd_id)


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
        logger.warning(
            f'Unsupported message type received in {caller}: {type}, '
            f'and it is ignored. Supported types: {tuple(registry.keys())}'
        )
    return action
