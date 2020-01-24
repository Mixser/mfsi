import stat
import time

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

    def __init__(self, st_mode, st_ctime, st_mtime, st_atime, st_nlink, st_size=None, st_uid=None, st_gid=None):
        self.st_mode = st_mode
        self.st_ctime = st_ctime
        self.st_mtime = st_mtime
        self.st_atime = st_atime

        self.st_nlink = st_nlink

        self.st_size = st_size
        self.st_uid = st_uid
        self.st_gid = st_gid

        self.attrs = {}

    def is_file(self):
        return stat.S_ISREG(self.st_mode)

    def is_directory(self):
        return stat.S_ISDIR(self.st_mode)

    def items(self):
        return [(field, getattr(self, field)) for field in self.__slots__ if getattr(self, field, None) is not None]

class INode:
    __slots__ = ('name', 'meta', 'sub_nodes', 'parent', 'data')

    def __init__(self, name: str, meta: INodeMeta, data=None):
        self.name = name
        self.meta = meta
        self.sub_nodes = {}

        self.parent = None
        self.data = data or b''

    def set_parent(self, node: 'INode'):
        self.parent = node

    def is_directory(self):
        return self.meta.is_directory()

    def is_file(self):
        return self.meta.is_file()

    def append(self, node: 'INode'):
        node.set_parent(self)

        if not self.is_directory():
            raise ValueError("Can't add child to file")

        if node.name in self.sub_nodes:
            if stat.S_IFMT(node.meta.st_mode) == stat.S_IFMT(self.sub_nodes[node.name].meta.st_mode):
                raise ValueError("Already exists")

        self.sub_nodes[node.name] = node
        node.set_parent(self)

    def read(self, offset, size):
        return self.data[offset: offset + size]

    def write(self, data, offset=0):
        self.data = self.data[:offset] + data
        self.meta.st_size = len(self.data)

        return self.meta.st_size



class Storage:
    def __init__(self):

        now = time.time()

        meta = INodeMeta(st_mode=(stat.S_IFDIR | 0o755), 
                         st_ctime=now, st_mtime=now, st_atime=now,
                         st_nlink=2)

        self._root = INode('', meta)

        self._device = None

    def set_device(self, device):
        self._device = device

    
    @staticmethod
    def _recursive_get(path, current_node):
        if not path:
            return current_node

        name, path = path[0], path[1: ]

        if name == '':
            return Storage._recursive_get(path, current_node)

        if name == current_node.name:
            return current_node

        if name not in current_node.sub_nodes:
            return None

        next_node = current_node.sub_nodes[name]
        return Storage._recursive_get(path, next_node)        

    def get(self, path):
        if path == '/':
            return self._root

        if path.startswith('/'):
            path = path[1:]

        path = path.split('/')
        return self._recursive_get(path, self._root)

    def list_dir(self, path):
        node = self.get(path)

        if not node:
            return ['.', '..']

        return ['.', '..'] + [n for n in node.sub_nodes]

    def _create_file(self, parent_node, filename, mode):
        now = time.time()

        meta = INodeMeta(st_mode=(stat.S_IFREG | mode), 
                         st_ctime=now, st_mtime=now, st_atime=now,
                         st_nlink=1, st_size=0)

        new_node = INode(filename, meta, data='')

        parent_node.append(new_node)

        return new_node

    def create_file(self, path, mode):
        *path, filename = path.split('/')

        node = self._recursive_get(path, self._root)

        if not node:
            raise ValueError("Invalid path")
        
        self._create_file(node, filename, mode)

        topic = 'fs' + '/'.join(path) + '/' + filename
        payload = ''
        self._device.publish(topic, payload)
        
    def _create_folder(self, parent_node, dirname, mode):
        now = time.time()

        meta = INodeMeta(st_mode=(stat.S_IFDIR | mode), 
                         st_ctime=now, st_mtime=now, st_atime=now,
                         st_nlink=2, st_size=0)

        new_node = INode(dirname, meta)
        parent_node.append(new_node)

        return new_node

    def create_folder(self, path, mode):
        *path, dirname = path.split('/')
        node = self._recursive_get(path, self._root)

        if not node:
            raise ValueError("Invalid path")
        
        self._create_folder(node, dirname, mode)


    def process_fs_event(self, topic, payload):
        if isinstance(topic, (bytes, bytearray)):
            topic = topic.decode()

        prefix, *path, filename = topic.split('/')
        # import pdb; pdb.set_trace()

        node = self._root

        for folder in path:
            if folder not in node.sub_nodes:
                self._create_folder(node, folder, mode=0o755)

            node = node.sub_nodes[folder]

        node = self._create_file(node, filename, 0b0110100100)
        node.write(payload)

        



        
