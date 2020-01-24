import threading

class MqttDevice:
    def __init__(self, token):
        self._token = token
        self._thread = None

    def mount(self):
        self._thread = threading.Thread(target=self._daemon)

        self._thread.start()

    async def _sync(self):
        client = MqttWrapper(storage)
        await client.initialize()

    def _daemon(self):
        loop = asyncio.new_event_loop()

        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self._sync())
        finally:
            loop.close()



