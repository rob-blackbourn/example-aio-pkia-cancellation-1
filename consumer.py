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
    """Wrap an async iterator such that it exits when the cancellation event is
    set.
    """
    cancellation_task = asyncio.create_task(cancellation_event.wait())
    result_iter = async_iterator.__aiter__()
    while not cancellation_event.is_set():
        done, pending = await asyncio.wait(
            [cancellation_task, result_iter.__anext__()],
            return_when=asyncio.FIRST_COMPLETED
        )
        for done_task in done:
            if done_task != cancellation_task:
                # We have a result from the async iterator.
                yield done_task.result()
            else:
                # The cancellation token has been set, and we should exit.
                # Cancel any pending tasks. This is safe as there is no await
                # between the completion of the wait on the cancellation event
                # and the pending tasks being cancelled. This means that the
                # pending tasks cannot have done any work.
                for pending_task in pending:
                    pending_task.cancel()
                # Now the tasks are cancelled we can await the cancellation
                # error, knowing they have done no work.
                for pending_task in pending:
                    try:
                        await pending_task
                    except asyncio.CancelledError:
                        pass


async def main_async():
    print("Press CTRL-C to quit")
    cancellation_event = Event()

    def _signal_handler(*args, **kwargs):
        print('Setting the cancellation event')
        cancellation_event.set()

    loop = asyncio.get_event_loop()
    for signal_value in {signal.SIGINT, signal.SIGTERM}:
        loop.add_signal_handler(signal_value, _signal_handler)

    url = "amqp://guest:guest@127.0.0.1/"
    async with await aio_pika.connect(url) as connection:

        channel = await connection.channel()
        queue = await channel.declare_queue('producer', passive=True)

        async with queue.iterator() as queue_iter:
            async for message in cancellable_aiter(queue_iter, cancellation_event):
                async with message.process():
                    obj = json.loads(message.body.decode())
                    print(obj)

if __name__ == '__main__':
    asyncio.run(main_async())
