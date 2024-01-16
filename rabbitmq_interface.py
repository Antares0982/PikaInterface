import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Union

from aio_pika import ExchangeType, Message, connect_robust


if TYPE_CHECKING:
    from aio_pika.message import AbstractIncomingMessage


# <-------------------- listen -------------------->
def listen_to(loop: asyncio.AbstractEventLoop, exchange_name: str, handler: Callable[["AbstractIncomingMessage"], Awaitable[Any]]):
    """
    Returns: a stop handle to stop listening.
    """
    condition = asyncio.Condition()
    loop.run_until_complete(condition.acquire())

    async def listen_task():
        connection = await connect_robust()
        async with connection:
            channel = await connection.channel()

            exchange = await channel.declare_exchange(exchange_name, ExchangeType.TOPIC)
            queue = await channel.declare_queue(exchange_name, exclusive=True)
            await queue.bind(exchange, routing_key=f'{exchange_name}.#')
            await queue.consume(handler)
            print(f"Start listening: {exchange_name}.#")
            await condition.wait()
            print(f"Stopped listening: {exchange_name}.#")
            await connection.close()

    async def stop_handle():
        async with condition:
            condition.notify()

    loop.create_task(listen_task())
    return stop_handle
# <-------------------- listen end -------------------->


# <-------------------- send -------------------->
__connection = None


async def get_sustained_connection():
    global __connection
    if __connection is None:
        __connection = await connect_robust()
    return __connection


async def close_sustained_connection():
    global __connection
    if __connection is not None:
        await __connection.close()
        __connection = None


async def send_message(routing_key: str, message: Union[str, bytes]):
    connection = await get_sustained_connection()
    chan = await connection.channel()
    exchange_name = routing_key.split('.')[0]
    exchange = await chan.declare_exchange(name=exchange_name, type=ExchangeType.TOPIC)
    await exchange.publish(
        Message(message.encode() if isinstance(message, str) else message),
        routing_key=routing_key
    )


def send_message_nowait(routing_key: str, message: Union[str, bytes]):
    loop = asyncio.get_event_loop()
    loop.create_task(send_message(routing_key, message))
# <-------------------- send end -------------------->
