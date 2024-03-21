import argparse
from fake_cmd.core.server import Server


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--address', type=str, required=True)
    parser.add_argument('--max_cmds', type=int, default=100, required=False)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    Server(args.address, args.max_cmds).run()
