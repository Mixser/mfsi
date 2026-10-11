import argparse
import logging
import os
import sys

from .device import MqttDevice
from .storage import Storage

DEFAULT_BROKER = 'tcp://mqtt.flespi.io'


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog='mfsi',
        description="Mount a filesystem which keeps files as retained MQTT messages.",
    )

    parser.add_argument('mountpoint', type=str, help="Directory to mount the filesystem to.")
    parser.add_argument('-t', '--token', type=str, default=os.environ.get('FLESPI_TOKEN'),
                        help="Flespi MQTT token, FLESPI_TOKEN environment variable by default.")
    parser.add_argument('-b', '--broker', type=str, default=DEFAULT_BROKER,
                        help="Broker URL: tcp://host[:port] or mqtts://host[:port] (default: %(default)s).")
    parser.add_argument('-v', '--verbose', action='store_true', help="Log every filesystem operation.")

    args = parser.parse_args(argv)

    if not args.token:
        parser.error("the token is required: pass --token or set FLESPI_TOKEN")

    return args


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    try:
        from fuse import FUSE

        from .fs import Filesystem
    except OSError as e:
        sys.exit("mfsi: {}. Install macFUSE (macOS) or libfuse (Linux).".format(e))

    storage = Storage()

    device = MqttDevice(args.token, storage, args.broker)
    storage.set_device(device)

    FUSE(Filesystem(storage, device), args.mountpoint, foreground=True)


if __name__ == "__main__":
    main()
