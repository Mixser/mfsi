# mfsi

A filesystem in userspace (FUSE) which keeps files as retained MQTT messages.
Mount it on several machines connected to the same broker, and a file written
on one of them shows up on the others.

By default it works with [flespi](https://flespi.com) MQTT broker
(`mqtt.flespi.io`), but any MQTT 5 broker with retained messages will do. The
MQTT client is [zenmqtt](https://github.com/Mixser/zenmqtt).

## How it works

- Every file is a retained message: the topic is `fs/<path>`, the payload is the
  content of the file. Writing to `/notes/todo.txt` publishes
  `fs/notes/todo.txt`.
- On mount the client subscribes to `fs/#`; the broker sends all retained
  messages, and the tree of files is built from them. Changes from other mounts
  arrive the same way.
- Removing a file publishes an empty retained message, which removes it from
  the broker; an empty message from the broker removes the file locally.
  Renaming republishes the files under the new topics.

### Limitations

- The files are kept in memory; the broker is the only persistent storage.
- **Empty files and empty folders exist only locally**: an empty payload means
  "removed", so they aren't stored on the broker. A file truncated to zero size
  disappears from other mounts until something is written to it.
- Permissions, owners, timestamps and extended attributes are local too.
- File names with `+` or `#` can't be published: these characters aren't
  allowed in MQTT topics.
- Changes are last-writer-wins: concurrent writes of the same file on different
  mounts aren't merged.

## Requirements

- Python 3.12 or newer
- FUSE: [macFUSE](https://osxfuse.github.io) on macOS, `libfuse` on Linux
  (e.g. `apt install fuse`)
- An MQTT 5 broker; for flespi, a token with access to the `fs/#` topics

## Installation

```sh
git clone <repository url> mfsi
cd mfsi
python3.12 -m venv .venv
. .venv/bin/activate
pip install .
```

## Usage

```sh
mkdir -p ~/mfsi
export FLESPI_TOKEN=<your flespi token>
mfsi ~/mfsi
```

The filesystem runs in the foreground; stop it with `Ctrl+C` or unmount the
directory (`umount ~/mfsi`).

Options:

| Option | Description |
|---|---|
| `-t`, `--token` | flespi token, the `FLESPI_TOKEN` environment variable by default. Prefer the variable: command line arguments are visible to other users in `ps`. |
| `-b`, `--broker` | broker URL, `tcp://host[:port]` or `mqtts://host[:port]` for TLS; `tcp://mqtt.flespi.io` by default |
| `-v`, `--verbose` | log every filesystem operation |

`python -m mfsi` works the same way as `mfsi`.

## Development

```sh
pip install -e '.[dev]'
pytest
```

The tests don't mount anything and don't need a broker: the MQTT client is
replaced with a fake, and when libfuse isn't installed the `fuse` module is
replaced with a stub.

Layout:

| Module | |
|---|---|
| `mfsi/cli.py` | command line, wires everything together |
| `mfsi/fs.py` | FUSE operations, delegate to the storage |
| `mfsi/storage.py` | the tree of files in memory; decides what is published |
| `mfsi/device.py` | runs the MQTT client in a background thread with its own event loop |
| `mfsi/mqtt.py` | the MQTT client: connect, subscribe, publish |

## License

[MIT](LICENSE)
