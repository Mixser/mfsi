import asyncio
import errno
import threading

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from mqtt import MqttWrapper


async def sync(storage, token):
    client = MqttWrapper(storage)

    storage.set_device(client)

    await client.initialize(token)

def main_loop(storage, token):
    loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(sync(storage, token))
    finally:
        loop.close()


class Filesystem(LoggingMixIn, Operations):
    def __init__(self, token, storage):
        self._storage = storage
        self._token = token

        self.fd = 0
        self.files = {}
        self._thread = None

    def init(self, path):
        self._thread = threading.Thread(target=lambda: main_loop(self._storage, self._token), args=())
        self._thread.start()

    def destroy(self, path):
        self._storage._device.disconnect()
        self._thread.join()

    def chmod(self, path, mode):

        self.files[path]['st_mode'] &= 0o770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
        self._storage.create_file(path, mode)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        file = self._storage.get(path)

        if not file:
            raise FuseOSError(errno.ENOENT)

        return file.meta

    def getxattr(self, path, name, position=0):
        file = self._storage.get(path)

        if not file:
            raise FuseOSError(errno.ENOATTR)

        attr = file.meta.attrs.get(name, None)
        print(attr)

        if not attr:
            raise FuseOSError(errno.ENOATTR)

        return attr

    def listxattr(self, path):
        file = self._storage.get(path)

        if not file:
            return []

        return file.meta.attrs.keys()

    def mkdir(self, path, mode):
        self._storage.create_folder(path, mode)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        file = self._storage.get(path)

        return file.read(offset, size)

    def readdir(self, path, fh):
        return self._storage.list_dir(path)

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        file = self._storage.get(path)

        if not file:
            return

        file.meta.attrs.pop(name, None)

    def rename(self, old, new):
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        file = self._storage.get(path)
        file.meta.attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096 * 1000, f_bavail=2048 * 1000)

    def truncate(self, path, length, fh=None):
        file = self._storage.get(path)

        file.data = file.data[:length]
        file.meta.st_size = length

    def unlink(self, path):
        self.files.pop(path)

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        file = self._storage.get(path)

        nbytes = file.write(data, offset)

        topic = 'fs' + path

        self._storage._device.publish(topic, file.data)

        return nbytes
