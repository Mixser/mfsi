import asyncio
import threading

import pytest

from mfsi.device import MqttDevice


class Message:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload
        self.qos = 0


class FakeClient:
    """Replaces zenmqtt.MQTTClient: messages are put to a queue by the test."""

    def __init__(self):
        self.queue = asyncio.Queue()
        self.published = []
        self.connected = threading.Event()
        self.processed = threading.Event()

    def authorize(self, username, password):
        self.username = username

    async def connect(self, url, **kwargs):
        self.url = url

    async def subscribe(self, topics):
        self.topics = topics
        self.connected.set()

    async def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, retain))

    async def ack(self, message):
        self.processed.set()

    async def disconnect(self):
        await self.queue.put(None)

    @property
    def messages(self):
        return self._iterate()

    async def _iterate(self):
        while (message := await self.queue.get()) is not None:
            yield message


@pytest.fixture
def mounted(storage):
    device = MqttDevice("token", storage, "tcp://broker")
    storage.set_device(device)

    client = FakeClient()
    device._client._client = client

    device.mount()
    assert client.connected.wait(2)

    yield device, client

    device.unmount()


def test_connects_and_subscribes(mounted):
    device, client = mounted

    assert client.url == "tcp://broker"
    assert client.username == "token"
    assert [(s.topic, s.no_local) for s in client.topics] == [("fs/#", True)]


def test_incoming_message_is_applied_to_storage(mounted, storage):
    device, client = mounted

    asyncio.run_coroutine_threadsafe(
        client.queue.put(Message("fs/dir/a.txt", b"hello")), device._loop
    ).result()
    assert client.processed.wait(2)

    assert storage.read("/dir/a.txt", 100, 0) == b"hello"


def test_publish_from_another_thread(mounted, storage):
    device, client = mounted

    storage.create_file("/a.txt", 0o644)
    storage.write("/a.txt", b"data", 0)

    device.unmount()

    assert client.published == [("fs/a.txt", b"data", True)]
    assert not device._thread.is_alive()


def test_publish_after_unmount_does_not_raise(mounted, storage):
    device, client = mounted
    device.unmount()

    storage.create_file("/a.txt", 0o644)
    storage.write("/a.txt", b"data", 0)
