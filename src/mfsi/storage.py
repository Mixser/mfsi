import errno
import logging
import os
import stat
import threading
import time

logger = logging.getLogger(__name__)

# Every file is a retained message: topic "fs/<path>", payload is the content.
# An empty payload removes the retained message, so empty files and empty
# folders exist only locally, and an empty payload from the broker means
# that the file was removed.
TOPIC_PREFIX = 'fs'

FILE_MODE = 0o644
FOLDER_MODE = 0o755


def _error(code, path):
    return OSError(code, os.strerror(code), path)


class INodeMeta:
    __slots__ = ('st_mode',
                 'st_ctime',
                 'st_mtime',
                 'st_atime',
                 'st_nlink',
                 'st_size',
                 'st_uid',
                 'st_gid',
                 'attrs')

    def __init__(self, st_mode, st_ctime, st_mtime, st_atime, st_nlink, st_size=0, st_uid=None, st_gid=None):
        self.st_mode = st_mode
        self.st_ctime = st_ctime
        self.st_mtime = st_mtime
        self.st_atime = st_atime

        self.st_nlink = st_nlink

        self.st_size = st_size
        self.st_uid = os.getuid() if st_uid is None else st_uid
        self.st_gid = os.getgid() if st_gid is None else st_gid

        self.attrs = {}

    def is_file(self):
        return stat.S_ISREG(self.st_mode)

    def is_directory(self):
        return stat.S_ISDIR(self.st_mode)

    def as_dict(self):
        return {field: getattr(self, field) for field in self.__slots__ if field != 'attrs'}


class INode:
    __slots__ = ('name', 'meta', 'sub_nodes', 'parent', 'data')

    def __init__(self, name: str, meta: INodeMeta, data=None):
        self.name = name
        self.meta = meta
        self.sub_nodes = {}

        self.parent = None
        self.data = data or b''

    def is_directory(self):
        return self.meta.is_directory()

    def is_file(self):
        return self.meta.is_file()

    def append(self, node: 'INode'):
        if not self.is_directory():
            raise ValueError("Can't add child to file")

        if node.name in self.sub_nodes:
            raise ValueError("Already exists")

        self.sub_nodes[node.name] = node
        node.parent = self

    def detach(self):
        del self.parent.sub_nodes[self.name]
        self.parent = None

    def path(self):
        names = []
        node = self

        while node.parent is not None:
            names.append(node.name)
            node = node.parent

        return '/' + '/'.join(reversed(names))

    def walk_files(self):
        if self.is_file():
            yield self
            return

        for node in list(self.sub_nodes.values()):
            yield from node.walk_files()

    def read(self, offset, size):
        return self.data[offset: offset + size]

    def write(self, data, offset=0):
        if offset > len(self.data):
            self.data += b'\0' * (offset - len(self.data))

        self.data = self.data[:offset] + data + self.data[offset + len(data):]
        self._touch()

        return len(data)

    def truncate(self, length):
        self.data = self.data[:length] + b'\0' * (length - len(self.data))
        self._touch()

    def replace(self, data):
        self.data = data
        self._touch()

    def _touch(self):
        self.meta.st_size = len(self.data)
        self.meta.st_mtime = time.time()


class Storage:
    def __init__(self):
        now = time.time()

        meta = INodeMeta(st_mode=(stat.S_IFDIR | FOLDER_MODE),
                         st_ctime=now, st_mtime=now, st_atime=now,
                         st_nlink=2)

        self._root = INode('', meta)

        self._device = None

        # FUSE threads and the MQTT thread change the tree concurrently
        self._lock = threading.RLock()

    def set_device(self, device):
        self._device = device

    @staticmethod
    def _split(path):
        return [name for name in path.split('/') if name]

    def _lookup(self, names):
        node = self._root

        for name in names:
            if not node.is_directory() or name not in node.sub_nodes:
                return None

            node = node.sub_nodes[name]

        return node

    def _get(self, path):
        node = self._lookup(self._split(path))

        if node is None:
            raise _error(errno.ENOENT, path)

        return node

    def _get_file(self, path):
        node = self._get(path)

        if node.is_directory():
            raise _error(errno.EISDIR, path)

        return node

    def _get_parent(self, path):
        *names, name = self._split(path) or ['']

        if not name:
            raise _error(errno.EEXIST, path)

        parent = self._lookup(names)

        if parent is None:
            raise _error(errno.ENOENT, path)

        if not parent.is_directory():
            raise _error(errno.ENOTDIR, path)

        return parent, name

    def get(self, path):
        with self._lock:
            return self._lookup(self._split(path))

    def list_dir(self, path):
        with self._lock:
            node = self._get(path)

            if not node.is_directory():
                raise _error(errno.ENOTDIR, path)

            return ['.', '..'] + list(node.sub_nodes)

    @staticmethod
    def _create_file(parent_node, filename, mode):
        now = time.time()

        meta = INodeMeta(st_mode=(stat.S_IFREG | mode),
                         st_ctime=now, st_mtime=now, st_atime=now,
                         st_nlink=1)

        new_node = INode(filename, meta)
        parent_node.append(new_node)

        return new_node

    @staticmethod
    def _create_folder(parent_node, dirname, mode):
        now = time.time()

        meta = INodeMeta(st_mode=(stat.S_IFDIR | mode),
                         st_ctime=now, st_mtime=now, st_atime=now,
                         st_nlink=2)

        new_node = INode(dirname, meta)
        parent_node.append(new_node)

        return new_node

    def create_file(self, path, mode):
        with self._lock:
            parent, filename = self._get_parent(path)

            if filename in parent.sub_nodes:
                raise _error(errno.EEXIST, path)

            # nothing to publish until the file has content
            self._create_file(parent, filename, mode)

    def create_folder(self, path, mode):
        with self._lock:
            parent, dirname = self._get_parent(path)

            if dirname in parent.sub_nodes:
                raise _error(errno.EEXIST, path)

            self._create_folder(parent, dirname, mode)

    def read(self, path, size, offset):
        with self._lock:
            return self._get_file(path).read(offset, size)

    def write(self, path, data, offset):
        with self._lock:
            node = self._get_file(path)
            nbytes = node.write(data, offset)

            self._publish(path, node.data)

            return nbytes

    def truncate(self, path, length):
        with self._lock:
            node = self._get_file(path)
            node.truncate(length)

            self._publish(path, node.data)

    def remove_file(self, path):
        with self._lock:
            node = self._get_file(path)
            node.detach()

            self._publish(path, b'')

    def remove_folder(self, path):
        with self._lock:
            node = self._get(path)

            if not node.is_directory():
                raise _error(errno.ENOTDIR, path)

            if node is self._root:
                raise _error(errno.EBUSY, path)

            if node.sub_nodes:
                raise _error(errno.ENOTEMPTY, path)

            node.detach()

    def rename(self, old, new):
        with self._lock:
            node = self._get(old)
            parent, name = self._get_parent(new)

            if node is self._root:
                raise _error(errno.EBUSY, old)

            # a folder can't be moved into itself
            ancestor = parent
            while ancestor is not None:
                if ancestor is node:
                    raise _error(errno.EINVAL, new)
                ancestor = ancestor.parent

            target = parent.sub_nodes.get(name)

            if target is node:
                return

            if target is not None:
                if node.is_directory() and not target.is_directory():
                    raise _error(errno.ENOTDIR, new)

                if not node.is_directory() and target.is_directory():
                    raise _error(errno.EISDIR, new)

                if target.is_directory() and target.sub_nodes:
                    raise _error(errno.ENOTEMPTY, new)

                # the new content of the target is published below
                target.detach()

            old_paths = [file.path() for file in node.walk_files()]

            node.detach()
            node.name = name
            parent.append(node)

            for old_path, file in zip(old_paths, node.walk_files()):
                self._publish(old_path, b'')
                self._publish(file.path(), file.data)

    def chmod(self, path, mode):
        with self._lock:
            node = self._get(path)
            node.meta.st_mode = stat.S_IFMT(node.meta.st_mode) | stat.S_IMODE(mode)

    def chown(self, path, uid, gid):
        with self._lock:
            node = self._get(path)

            # -1 means "don't change"
            if uid != -1:
                node.meta.st_uid = uid
            if gid != -1:
                node.meta.st_gid = gid

    def utimens(self, path, times=None):
        with self._lock:
            node = self._get(path)
            atime, mtime = times or (time.time(), time.time())

            node.meta.st_atime = atime
            node.meta.st_mtime = mtime

    def _publish(self, path, data):
        if self._device is None:
            logger.warning("No device, %s isn't published", path)
            return

        self._device.publish(TOPIC_PREFIX + path, data)

    def process_fs_event(self, topic, payload):
        if isinstance(topic, (bytes, bytearray)):
            topic = topic.decode()

        prefix, *names = topic.split('/')
        names = [name for name in names if name]

        if prefix != TOPIC_PREFIX or not names:
            return

        *folders, filename = names

        with self._lock:
            if not payload:
                node = self._lookup(names)

                if node is not None and node.is_file():
                    node.detach()

                return

            node = self._root

            for folder in folders:
                child = node.sub_nodes.get(folder)

                if child is None:
                    child = self._create_folder(node, folder, FOLDER_MODE)
                elif not child.is_directory():
                    logger.warning("Skip %s: %s is a file", topic, child.path())
                    return

                node = child

            file = node.sub_nodes.get(filename)

            if file is None:
                file = self._create_file(node, filename, FILE_MODE)
            elif not file.is_file():
                logger.warning("Skip %s: %s is a folder", topic, file.path())
                return

            file.replace(bytes(payload))
