import sys
import types

import pytest

from mfsi.storage import Storage

try:
    import fuse  # noqa: F401
except OSError:
    # libfuse isn't installed: the tests don't mount anything, a stub is enough
    stub = types.ModuleType("fuse")

    class FuseOSError(OSError):
        def __init__(self, errno):
            super().__init__(errno, "")

    stub.FuseOSError = FuseOSError
    stub.Operations = type("Operations", (), {})
    stub.LoggingMixIn = type("LoggingMixIn", (), {})

    sys.modules["fuse"] = stub


class RecordingDevice:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload):
        self.published.append((topic, payload))


@pytest.fixture
def device():
    return RecordingDevice()


@pytest.fixture
def storage(device):
    storage = Storage()
    storage.set_device(device)
    return storage
