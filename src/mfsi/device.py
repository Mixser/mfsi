import asyncio
import logging
import threading

from .mqtt import MqttWrapper

logger = logging.getLogger(__name__)


class MqttDevice:
    """
    Runs the MQTT client in a background thread with its own event loop;
    the methods are called from FUSE threads.
    """

    def __init__(self, token, storage, broker):
        self._token = token
        self._client = MqttWrapper(storage, broker)

        self._loop = asyncio.new_event_loop()
        self._thread = None

    def mount(self):
        self._thread = threading.Thread(target=self._daemon, name='mqtt', daemon=True)
        self._thread.start()

    def unmount(self):
        if self._thread is None:
            return

        if self._thread.is_alive():
            try:
                self._run(self._client.disconnect()).result(timeout=5)
            except Exception:
                logger.exception("Failed to disconnect")

        self._thread.join(timeout=5)

    def publish(self, topic, payload):
        coro = self._client.publish(topic, payload)

        try:
            future = self._run(coro)
        except RuntimeError:
            coro.close()
            logger.error("Can't publish %s: the device is stopped", topic)
            return

        future.add_done_callback(lambda f: self._log_error(f, "Failed to publish " + topic))

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    @staticmethod
    def _log_error(future, message):
        if not future.cancelled() and future.exception() is not None:
            logger.error(message, exc_info=future.exception())

    def _daemon(self):
        asyncio.set_event_loop(self._loop)

        try:
            self._loop.run_until_complete(self._client.initialize(self._token))
        except Exception:
            logger.exception("MQTT client stopped")
        finally:
            # publishes which wait for a connection that never comes
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

            self._loop.close()
