"""The producer queues messages"""

import asyncio
import signal

import aio_pika


async def main_async():
    print("Press CTRL-C to quit")
    cancellation_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: cancellation_event.set())

    async with await aio_pika.connect("amqp://guest:guest@127.0.0.1/") as connection:

        channel = await connection.channel()
        exchange = await channel.declare_exchange('test', aio_pika.ExchangeType.TOPIC, durable=True)
        queue = await channel.declare_queue('producer', durable=True)
        await queue.bind(exchange, routing_key="producer")

        while not cancellation_event.is_set():
            try:
                await asyncio.wait_for(cancellation_event.wait(), 2)
                print('Cancelled')
            except asyncio.TimeoutError:
                print("Enqueue")
                await exchange.publish(
                    aio_pika.Message(
                        body=b'[1,2,3]',
                        content_type='application/json',
                        app_id='producer'
                    ),
                    routing_key='producer',
                )
if __name__ == '__main__':
    asyncio.run(main_async())
