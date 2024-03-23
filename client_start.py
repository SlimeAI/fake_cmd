import argparse
from fake_cmd.core.client import Client
from fake_cmd.utils import parser_parse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--address', type=str, required=True)
    parser.add_argument('--id_prefix', type=str, default=None, required=False)
    return parser_parse(parser)


if __name__ == '__main__':
    args = get_args()
    Client(args.address, args.id_prefix).run()
