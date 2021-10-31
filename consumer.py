"""A RabbitMQ consumer"""

import asyncio
from asyncio import Event
import json
import platform
import signal
from typing import AsyncIterator, Optional, TypeVar

import aio_pika

T = TypeVar('T')


async def _windows_signal_support(
        cancellation_event: asyncio.Event
) -> None:
    # See https://bugs.python.org/issue23057, to catch signals on
    # Windows it is necessary for an IO event to happen periodically.
    while not cancellation_event.is_set():
        try:
            await asyncio.wait_for(cancellation_event.wait(), 1)
        except asyncio.TimeoutError:
            pass


async def cancellable_aiter(
        async_iterator: AsyncIterator[T],
        cancellation_event: Event
) -> AsyncIterator[T]:
    """Wraps an async iterator such that it exits when the cancellation event is set"""
    cancellation_task = asyncio.create_task(cancellation_event.wait())
    result_iter = async_iterator.__aiter__()
    while not cancellation_event.is_set():
        done, pending = await asyncio.wait(
            [cancellation_task, result_iter.__anext__()],
            return_when=asyncio.FIRST_COMPLETED
        )
        for done_task in done:
            if done_task == cancellation_task:
                for pending_task in pending:
                    try:
                        pending_task.cancel()
                        await pending_task
                    except asyncio.CancelledError:
                        pass
                break
            else:
                yield done_task.result()


async def main_async():
    print("Press CTRL-C to quit")
    cancellation_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    windows_task: Optional[Task] = None
    if platform.system() == "Windows":
        windows_task = asyncio.create_task(
            _windows_signal_support(cancellation_event)
        )

    def _signal_handler(*args, **kwargs):
        print("Received signal")
        cancellation_event.set()

    for signal_name in {"SIGINT", "SIGTERM", "SIGBREAK"}:
        if hasattr(signal, signal_name):
            sig_value = getattr(signal, signal_name)
            try:
                loop.add_signal_handler(sig_value, _signal_handler)
            except NotImplementedError:
                # Add signal handler may not be implemented on Windows
                signal.signal(sig_value, _signal_handler)

    async with await aio_pika.connect("amqp://guest:guest@127.0.0.1/") as connection:

        channel = await connection.channel()
        queue = await channel.declare_queue('producer', passive=True)

        async with queue.iterator() as queue_iter:
            async for message in cancellable_aiter(queue_iter, cancellation_event):
                async with message.process():
                    obj = json.loads(message.body.decode())
                    print(obj)

    if windows_task:
        await windows_task

if __name__ == '__main__':
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore

    asyncio.run(main_async())
