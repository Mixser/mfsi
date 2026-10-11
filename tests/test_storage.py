import errno
import stat

import pytest


def errno_of(fn, *args):
    with pytest.raises(OSError) as info:
        fn(*args)
    return info.value.errno


def test_write_publishes_content(storage, device):
    storage.create_file("/a.txt", 0o644)
    storage.write("/a.txt", b"hello", 0)

    assert storage.read("/a.txt", 100, 0) == b"hello"
    assert device.published == [("fs/a.txt", b"hello")]


def test_write_in_the_middle_keeps_the_tail(storage):
    storage.create_file("/a.txt", 0o644)
    storage.write("/a.txt", b"hello world", 0)
    storage.write("/a.txt", b"HE", 0)

    assert storage.read("/a.txt", 100, 0) == b"HEllo world"


def test_write_after_the_end_pads_with_zeros(storage):
    storage.create_file("/a.txt", 0o644)
    storage.write("/a.txt", b"ab", 4)

    assert storage.read("/a.txt", 100, 0) == b"\0\0\0\0ab"
    assert storage.get("/a.txt").meta.st_size == 6


def test_truncate(storage, device):
    storage.create_file("/a.txt", 0o644)
    storage.write("/a.txt", b"hello", 0)
    storage.truncate("/a.txt", 2)

    assert storage.read("/a.txt", 100, 0) == b"he"
    assert device.published[-1] == ("fs/a.txt", b"he")


def test_remove_file_clears_retained_message(storage, device):
    storage.create_file("/a.txt", 0o644)
    storage.remove_file("/a.txt")

    assert storage.get("/a.txt") is None
    assert device.published == [("fs/a.txt", b"")]


def test_rename_folder_republishes_files(storage, device):
    storage.create_folder("/dir", 0o755)
    storage.create_folder("/dir/sub", 0o755)
    storage.create_file("/dir/sub/b", 0o644)
    storage.write("/dir/sub/b", b"bb", 0)
    device.published.clear()

    storage.rename("/dir", "/moved")

    assert storage.list_dir("/") == [".", "..", "moved"]
    assert storage.read("/moved/sub/b", 100, 0) == b"bb"
    assert device.published == [("fs/dir/sub/b", b""), ("fs/moved/sub/b", b"bb")]


def test_rename_replaces_existing_file(storage):
    for name, data in (("/a", b"a"), ("/b", b"b")):
        storage.create_file(name, 0o644)
        storage.write(name, data, 0)

    storage.rename("/a", "/b")

    assert storage.list_dir("/") == [".", "..", "b"]
    assert storage.read("/b", 100, 0) == b"a"


def test_errors(storage):
    storage.create_folder("/dir", 0o755)
    storage.create_file("/dir/a", 0o644)

    assert errno_of(storage.read, "/missing", 1, 0) == errno.ENOENT
    assert errno_of(storage.read, "/dir", 1, 0) == errno.EISDIR
    assert errno_of(storage.create_folder, "/dir", 0o755) == errno.EEXIST
    assert errno_of(storage.create_file, "/missing/a", 0o644) == errno.ENOENT
    assert errno_of(storage.create_file, "/dir/a/b", 0o644) == errno.ENOTDIR
    assert errno_of(storage.remove_folder, "/dir") == errno.ENOTEMPTY
    assert errno_of(storage.remove_folder, "/") == errno.EBUSY
    assert errno_of(storage.rename, "/dir", "/dir/sub") == errno.EINVAL


def test_chmod_keeps_file_type(storage):
    storage.create_file("/a", 0o644)
    storage.chmod("/a", 0o600)

    mode = storage.get("/a").meta.st_mode
    assert stat.S_ISREG(mode) and stat.S_IMODE(mode) == 0o600


def test_remote_event_creates_and_updates_file(storage):
    storage.process_fs_event("fs/dir/a.txt", b"hello")
    storage.process_fs_event("fs/dir/a.txt", b"again")

    assert storage.list_dir("/dir") == [".", "..", "a.txt"]
    assert storage.read("/dir/a.txt", 100, 0) == b"again"


def test_remote_empty_payload_removes_file(storage):
    storage.process_fs_event("fs/dir/a.txt", b"hello")
    storage.process_fs_event("fs/dir/a.txt", b"")

    assert storage.list_dir("/dir") == [".", ".."]


def test_remote_event_outside_prefix_is_ignored(storage):
    storage.process_fs_event("flespi/state/x", b"zzz")

    assert storage.list_dir("/") == [".", ".."]


def test_remote_event_conflicting_with_folder_is_skipped(storage):
    storage.create_folder("/a", 0o755)
    storage.process_fs_event("fs/a", b"data")

    assert storage.get("/a").is_directory()
