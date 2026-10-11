import errno

import pytest

from mfsi.fs import Filesystem


@pytest.fixture
def filesystem(storage, device):
    return Filesystem(storage, device)


def test_getattr(filesystem):
    filesystem.create('/a', 0o644)
    filesystem.write('/a', b'abc', 0, 1)

    attrs = filesystem.getattr('/a')
    assert attrs['st_size'] == 3
    assert 'attrs' not in attrs


def test_getattr_missing(filesystem):
    with pytest.raises(OSError) as info:
        filesystem.getattr('/missing')
    assert info.value.errno == errno.ENOENT


def test_xattrs(filesystem):
    filesystem.create('/a', 0o644)
    filesystem.setxattr('/a', 'user.k', b'v', 0)

    assert filesystem.getxattr('/a', 'user.k') == b'v'
    assert filesystem.listxattr('/a') == ['user.k']

    filesystem.removexattr('/a', 'user.k')
    with pytest.raises(OSError):
        filesystem.getxattr('/a', 'user.k')


def test_file_handles_are_unique(filesystem):
    filesystem.create('/a', 0o644)
    assert filesystem.open('/a', 0) != filesystem.open('/a', 0)
