import asyncio
import threading
from concurrent.futures import Future
from typing import Coroutine


class AsyncRunner:
    _instance = None

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self._run_loop,
            daemon=True
        )
        self.thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run_async(self, coro: Coroutine, wait_until_finished=True):
        future = Future()

        async def wrapper():
            try:
                result = await coro
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

        self.loop.call_soon_threadsafe(
            asyncio.create_task, wrapper()
        )
        if wait_until_finished:
            return future.result()  # blocks until result
        else:
            return future


    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = AsyncRunner()
        return cls._instance


def run_async(coro: Coroutine, wait_until_finished=True):
    return AsyncRunner.get_instance().run_async(coro, wait_until_finished)
