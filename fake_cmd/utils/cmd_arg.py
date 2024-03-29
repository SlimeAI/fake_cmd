"""
Parse client command arguments.
"""
import shlex
from argparse import ArgumentParser
from abc import ABC, abstractmethod
from slime_core.utils.registry import Registry
from slime_core.utils.typing import (
    Union,
    Tuple,
    List,
    Missing,
    Literal,
    MISSING,
    Sequence,
    Type
)
from . import config, ArgNamespace, parser_parse
from .logging import logger
from .executors import platform_open_executor_registry
from .executors.writer import popen_writer_registry
from .executors.reader import popen_reader_registry

#
# Split command for further analysis.
#

# Separator to separate between command options and session command.
CMD_SEPARATOR = ' -- '
# Command split type.
CMDSplit = Tuple[Union[List[str], Missing], Union[str, Missing]]


def split_cmd(cmd_str: str) -> Union[CMDSplit, Literal[False]]:
    """
    Split the command str into possible command options and possible 
    session command.
    
    Example:
    ```Python
    # (['inter'], 'python -i')
    split_cmd('inter -- python -i')
    # (['inter', '--writer', 'pty'], MISSING<0x7fa1282a4310>)
    split_cmd('inter --writer pty')
    # (['inter', '--exec', '/path/with white space', '--writer', 'pty'], 'python -i')
    split_cmd('inter --exec "/path/with white space" --writer pty -- python -i')
    # (MISSING<id>, 'a')
    split_cmd(' -- a')
    # (['a'], '')
    split_cmd('a -- ')
    # (MISSING<id>, '')
    split_cmd(' -- ')
    ```
    """
    cmd_splits = cmd_str.split(CMD_SEPARATOR, 1)
    split_len = len(cmd_splits)
    if split_len == 0:
        return False
    elif split_len > 2:
        logger.warning(
            'Expect the length of ``cmd_splits`` to be no more than 2, '
            f'but {split_len} found. Split result: {cmd_splits}'
        )
        return False
    
    # Split the possible command options for further analyzing.
    shlex_splits = shlex.split(cmd_splits[0], posix=config.posix_shlex)
    if len(shlex_splits) == 0:
        shlex_splits = MISSING
    if split_len == 1:
        return shlex_splits, MISSING
    else:
        return shlex_splits, cmd_splits[1]

#
# ArgumentParsers.
#

class CMDParser(ABC):
    
    @abstractmethod
    def get_parser(self) -> ArgumentParser:
        """
        Return an ArgumentParser object.
        """
        pass
    
    @abstractmethod
    def set_additional_args(self, args: ArgNamespace) -> None:
        """
        Set additional command args to the namespace.
        """
        pass
    
    @abstractmethod
    def make_cmd_content(self, args: ArgNamespace) -> dict:
        """
        Make command content dict using args.
        """
        pass
    
    def parse_args(
        self,
        args: Union[Sequence[str], Missing, None] = MISSING,
        strict: bool = False
    ) -> Union[ArgNamespace, Missing]:
        """
        Parse arguments using ArgumentParser, and set additional 
        args.
        """
        parser = self.get_parser()
        args = parser_parse(parser, args, strict)
        if args is MISSING:
            # If ``args`` is ``MISSING``, directly return.
            return args
        # Set additional args.
        self.set_additional_args(args)
        return args
    
    def parse_cmd(
        self,
        cmd_splits: CMDSplit,
        strict: bool
    ) -> Union[ArgNamespace, Literal[False]]:
        possible_options, possible_cmd = cmd_splits
        if possible_options is MISSING:
            return False
        
        args = self.parse_args(
            args=possible_options[1:],
            strict=strict
        )
        if (
            args is MISSING or 
            possible_cmd is MISSING
        ):
            return False
        
        args.cmd = possible_cmd
        return args


cmd_parser_registry = Registry[Type[CMDParser]]('cmd_parser_registry')


class PopenCMDParser(CMDParser):
    
    @abstractmethod
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser()
        parser.add_argument(
            '--encoding',
            default=None,
            type=str,
            required=False,
            help=(
                'The encoding method of input and output. Should not '
                'be specified most of the time (and leave it default).'
            )
        )
        parser.add_argument(
            '--exec',
            default=None,
            type=str,
            required=False,
            help=(
                'The executable script path. Should not be specified '
                'most of the time (and leave it default).'
            )
        )
        parser.add_argument(
            '--platform',
            default=None,
            type=str,
            required=False,
            help=(
                'The running platform that decides process operations. '
                'Should not be specified most of the time (and leave it '
                'default).'
            ),
            choices=platform_open_executor_registry.keys()
        )
        parser.add_argument(
            '--reader',
            default='default',
            type=str,
            choices=popen_reader_registry.keys(),
            required=False,
            help=(
                'The output redirection setting. Specify the reader '
                'setting of Popen.'
            )
        )
        return parser
    
    def make_cmd_content(self, args: ArgNamespace) -> dict:
        return {
            'cmd': args.cmd,
            'encoding': args.encoding,
            'reader': args.reader,
            'writer': args.writer,
            'exec': args.exec,
            'platform': args.platform,
            'interactive': args.interactive,
            'kill_disabled': args.kill_disabled
        }


@cmd_parser_registry(key='cmd')
class PlainPopenParser(PopenCMDParser):
    
    def get_parser(self) -> ArgumentParser:
        parser = super().get_parser()
        parser.prog = 'cmd'
        return parser
    
    def set_additional_args(self, args: ArgNamespace) -> None:
        args.interactive = False
        args.kill_disabled = False
        args.writer = None


@cmd_parser_registry(key='inter')
class InterPopenParser(PopenCMDParser):
    
    def get_parser(self) -> ArgumentParser:
        parser = super().get_parser()
        parser.prog = 'inter'
         # Set the default to 'pipe'.
        parser.add_argument(
            '--writer',
            default='pipe',
            type=str,
            choices=popen_writer_registry.keys(),
            required=False,
            help=(
                'The interactive input setting. Specify the writer '
                'setting of Popen.'
            )
        )
        parser.add_argument(
            '--kill_disabled',
            action='store_true',
            help=(
                'Whether keyboard interrupt is disabled to kill the command.'
            )
        )
        return parser

    def set_additional_args(self, args: ArgNamespace) -> None:
        args.interactive = True
