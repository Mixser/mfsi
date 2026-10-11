import errno
import itertools

from fuse import FuseOSError, Operations, LoggingMixIn

# Linux has no ENOATTR, ENODATA is used instead
ENOATTR = getattr(errno, 'ENOATTR', errno.ENODATA)


class Filesystem(LoggingMixIn, Operations):
    def __init__(self, storage, device):
        self._storage = storage
        self._device = device

        self._fds = itertools.count(1)

    def init(self, path):
        self._device.mount()

    def destroy(self, path):
        self._device.unmount()

    def _get(self, path):
        node = self._storage.get(path)

        if node is None:
            raise FuseOSError(errno.ENOENT)

        return node

    def chmod(self, path, mode):
        self._storage.chmod(path, mode)

    def chown(self, path, uid, gid):
        self._storage.chown(path, uid, gid)

    def create(self, path, mode, fi=None):
        self._storage.create_file(path, mode)
        return next(self._fds)

    def getattr(self, path, fh=None):
        return self._get(path).meta.as_dict()

    def getxattr(self, path, name, position=0):
        attr = self._get(path).meta.attrs.get(name)

        if attr is None:
            raise FuseOSError(ENOATTR)

        return attr

    def listxattr(self, path):
        return list(self._get(path).meta.attrs)

    def mkdir(self, path, mode):
        self._storage.create_folder(path, mode)

    def open(self, path, flags):
        self._get(path)
        return next(self._fds)

    def read(self, path, size, offset, fh):
        return self._storage.read(path, size, offset)

    def readdir(self, path, fh):
        return self._storage.list_dir(path)

    def removexattr(self, path, name):
        if self._get(path).meta.attrs.pop(name, None) is None:
            raise FuseOSError(ENOATTR)

    def rename(self, old, new):
        self._storage.rename(old, new)

    def rmdir(self, path):
        self._storage.remove_folder(path)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        self._get(path).meta.attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096 * 1000, f_bavail=2048 * 1000)

    def truncate(self, path, length, fh=None):
        self._storage.truncate(path, length)

    def unlink(self, path):
        self._storage.remove_file(path)

    def utimens(self, path, times=None):
        self._storage.utimens(path, times)

    def write(self, path, data, offset, fh):
        return self._storage.write(path, data, offset)
