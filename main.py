import asyncio
import argparse
import logging
import threading

from fuse import FUSE

import fs
from storage import Storage

def main(args):
    storage = Storage()

    operations = fs.Filesystem(args.token, storage)
    fuse = FUSE(operations, args.mountpoint[0], foreground=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()

    parser.add_argument('mountpoint', type=str, nargs=1)
    parser.add_argument('-t', '--token', type=str, required=True, help="Flespi MQTT Token.")

    args = parser.parse_args()

    main(args)