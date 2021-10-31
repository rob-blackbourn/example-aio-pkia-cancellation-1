"""A RabbitMQ consumer"""

import asyncio
from asyncio import Event
import json
import signal
from typing import AsyncIterator, TypeVar

import aio_pika

T = TypeVar('T')


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
                    await pending_task
                break
            else:
                yield done_task.result()


async def main_async():
    print("Press CTRL-C to quit")
    cancellation_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: cancellation_event.set())

    async with await aio_pika.connect("amqp://guest:guest@127.0.0.1/") as connection:

        channel = await connection.channel()
        queue = await channel.declare_queue('producer', passive=True)

        async with queue.iterator() as queue_iter:
            async for message in cancellable_aiter(queue_iter, cancellation_event):
                async with message.process():
                    obj = json.loads(message.body.decode())
                    print(obj)

if __name__ == '__main__':
    asyncio.run(main_async())
