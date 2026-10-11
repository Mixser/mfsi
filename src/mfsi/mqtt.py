import logging
import time

from zenmqtt import MQTTClient, Subscription

from .storage import TOPIC_PREFIX

logger = logging.getLogger(__name__)


class MqttWrapper:
    def __init__(self, storage, broker):
        self._storage = storage
        self._broker = broker

        self._client = MQTTClient("mitu-fs-{}".format(time.time()))

    async def initialize(self, token):
        self._client.authorize(token, None)

        await self._client.connect(self._broker, clean_session=False)

        # no_local: our own publishes are already applied to the storage
        await self._client.subscribe([Subscription(TOPIC_PREFIX + '/#', qos=0, no_local=True)])

        async for message in self._client.messages:
            try:
                self._storage.process_fs_event(message.topic, message.payload)
            except Exception:
                logger.exception("Failed to process message from %s", message.topic)

            await self._client.ack(message)

    async def disconnect(self):
        await self._client.disconnect()

    async def publish(self, topic, payload):
        await self._client.publish(topic, payload, retain=True)
