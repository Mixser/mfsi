import asyncio
import time

from gmqtt import Client as MqttClient

class MqttWrapper(MqttClient):
    def __init__(self, storage):
        self._storage = storage

        client = MqttClient("mitu-fs-{}".format(time.time()), clean_session=False)

        self._client = self.__init_client_hooks__(client)
        self._disconnect = asyncio.Event()

        self._loop = asyncio.get_event_loop()

    def __init_client_hooks__(self, client):
        client.on_message = self._on_message
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect

        return client

    def _on_disconnect(self, client, *args, **kwargs):
        self._disconnect.set()

    def _on_connect(self, client, flags, rc, properties):
        client.subscribe('#', qos=0)

    def _on_message(self, client, topic, payload, *args, **kwargs):
        self._storage.process_fs_event(topic, payload)

    async def initialize(self, token):
        self._client.set_auth_credentials(token, None)

        await self._client.connect('mqtt.flespi.io')

        await self._disconnect.wait()

    def disconnect(self):
        asyncio.ensure_future(self._client.disconnect(), loop=self._loop)
        

    def publish(self, topic, payload):
        self._client.publish(topic, payload, retain=True)
