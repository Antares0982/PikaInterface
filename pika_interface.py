import asyncio
import threading
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional, Union

from aio_pika import ExchangeType, Message, connect_robust


if TYPE_CHECKING:
    from aio_pika.channel import AbstractChannel
    from aio_pika.connection import AbstractConnection
    from aio_pika.message import AbstractIncomingMessage

# <-------------------- listen -------------------->


def listen_to(
    loop: asyncio.AbstractEventLoop,
    routing_key_prefix: str,
    handler: Callable[["AbstractIncomingMessage"], Awaitable[Any]],
    **connection_kwargs
):
    """
    Returns: a stop handle to stop listening.
    """
    condition = asyncio.Condition()
    loop.run_until_complete(condition.acquire())

    async def listen_task():
        connection = await connect_robust(**connection_kwargs)
        async with connection:
            channel = await connection.channel()

            exchange_name = routing_key_prefix.split('.')[0]

            exchange = await channel.declare_exchange(exchange_name, ExchangeType.TOPIC)
            queue = await channel.declare_queue('', exclusive=True)
            await queue.bind(exchange, routing_key=f'{routing_key_prefix}.#')

            async def consume(msg: "AbstractIncomingMessage"):
                async with msg.process():
                    await handler(msg)
            await queue.consume(consume)
            print(f"Start listening: {routing_key_prefix}.#")
            await condition.wait()
            print(f"Stopped listening: {routing_key_prefix}.#")
            await connection.close()

    async def stop_handle():
        async with condition:
            condition.notify()

    loop.create_task(listen_task())
    return stop_handle
# <-------------------- listen end -------------------->


class SustainedChannel:
    conn: "AbstractConnection"
    channel: "AbstractChannel"
    MAIN_CONNECTION: Optional["SustainedChannel"] = None
    CONNECTIONS: Dict[int, "SustainedChannel"] = dict()
    LOCK = threading.Lock()
# <-------------------- send -------------------->


async def create_sustained_connection(**kwargs):
    if SustainedChannel.MAIN_CONNECTION is not None:
        raise RuntimeError("Sustained connection already exists.")
    ret = SustainedChannel()
    ret.conn = await connect_robust(**kwargs)
    ret.channel = await ret.conn.channel()
    SustainedChannel.MAIN_CONNECTION = ret
    return ret


async def get_sustained_connection():
    ret = SustainedChannel.MAIN_CONNECTION
    if ret is None:
        ret = SustainedChannel()
        ret.conn = await connect_robust()
        ret.channel = await ret.conn.channel()
        SustainedChannel.MAIN_CONNECTION = ret
    return ret


async def close_sustained_connection():
    if SustainedChannel.MAIN_CONNECTION is not None:
        conn = SustainedChannel.MAIN_CONNECTION
        await conn.channel.close()
        await conn.conn.close()
        SustainedChannel.MAIN_CONNECTION = None


async def send_message(routing_key: str, message: Union[str, bytes]):
    chan = (await get_sustained_connection()).channel
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


# <-------------------- send multi-thread -------------------->
async def create_cur_sustained_connection_thread_safe(**kwargs):
    _id = threading.get_ident()
    with SustainedChannel.LOCK:
        connections = SustainedChannel.CONNECTIONS
        ret = connections.get(_id)
        if ret is not None:
            raise RuntimeError("Sustained connection already exists.")
        ret = SustainedChannel()
        ret.conn = await connect_robust(**kwargs)
        ret.channel = await ret.conn.channel()
        connections[_id] = ret
    return ret


async def get_cur_sustained_connection_thread_safe():
    _id = threading.get_ident()
    with SustainedChannel.LOCK:
        connections = SustainedChannel.CONNECTIONS
        _new = False
        ret = connections.get(_id)
        if ret is None:
            _new = True
            ret = SustainedChannel()
            connections[_id] = ret
    if _new:
        ret.conn = await connect_robust()
        ret.channel = await ret.conn.channel()
    return ret


async def close_sustained_connections_thread_safe():
    with SustainedChannel.LOCK:
        connections = SustainedChannel.CONNECTIONS
        SustainedChannel.CONNECTIONS = dict()
    for conn in connections.values():
        await conn.channel.close()
        await conn.conn.close()
    connections.clear()


async def close_cur_sustained_connection_thread_safe():
    _id = threading.get_ident()
    with SustainedChannel.LOCK:
        ret = SustainedChannel.CONNECTIONS.pop(_id, None)
    if ret is not None:
        await ret.channel.close()
        await ret.conn.close()


async def send_message_thread_safe(routing_key: str, message: Union[str, bytes]):
    chan = (await get_cur_sustained_connection_thread_safe()).channel
    exchange_name = routing_key.split('.')[0]
    exchange = await chan.declare_exchange(name=exchange_name, type=ExchangeType.TOPIC)
    await exchange.publish(
        Message(message.encode() if isinstance(message, str) else message),
        routing_key=routing_key
    )


def send_message_nowait_thread_safe(routing_key: str, message: Union[str, bytes]):
    loop = asyncio.get_event_loop()
    loop.create_task(send_message_thread_safe(routing_key, message))
# <-------------------- send multi-thread end -------------------->
