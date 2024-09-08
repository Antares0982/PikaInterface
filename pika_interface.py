__VERSION__ = "1.0.0"

import asyncio
import threading
from typing import Any, Awaitable, Callable, Union

from aio_pika import ExchangeType, Message, connect_robust
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
    loop.create_task(condition.acquire())

    async def listen_task():
        while not condition.locked():
            await asyncio.sleep(0.1)
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
        if not condition.locked():  # in case the listen task crashed
            return
        async with condition:
            condition.notify()

    loop.create_task(listen_task())
    return stop_handle
# <-------------------- listen end -------------------->


class SustainedChannel:
    conn: AbstractConnection
    channel: AbstractChannel

    @classmethod
    async def create(cls, **kwargs):
        ret = cls()
        cls.conn = await connect_robust(**kwargs)
        cls.channel = await cls.conn.channel()
        return ret


class _ConnectionHolder:
    main_connection: SustainedChannel | None = None
    connections: dict[int, SustainedChannel] = dict()
    lock = threading.Lock()


_connection_holder = _ConnectionHolder()
# <-------------------- send -------------------->


async def create_sustained_connection(**kwargs) -> SustainedChannel:
    """
    Create a global connection object.
    This global connection will be retrieved by `get_sustained_connection`.
    """
    if _connection_holder.main_connection is not None:
        raise RuntimeError("Sustained connection already exists.")
    ret = await SustainedChannel.create(**kwargs)
    _connection_holder.main_connection = ret
    return ret


async def get_sustained_connection() -> SustainedChannel:
    """
    Get the global connection object.

    Note:
        If called before `create_sustained_connection`,
        it will create a new connection without any argument.
    """
    ret = _connection_holder.main_connection
    if ret is None:
        ret = await SustainedChannel.create()
        _connection_holder.main_connection = ret
    return ret


async def close_sustained_connection() -> None:
    """
    Close the global connection object.
    Do nothing when connection is not created.
    """
    if _connection_holder.main_connection is not None:
        conn = _connection_holder.main_connection
        await conn.channel.close()
        await conn.conn.close()
        _connection_holder.main_connection = None


async def send_message(routing_key: str, message: Union[str, bytes], channel: AbstractChannel | None = None):
    """
    Send a message.
    If `channel` is not passed, use the global connection to get channel.
    """
    chan = (await get_sustained_connection()).channel if channel is None else channel
    exchange_name = routing_key.split('.')[0]
    exchange = await chan.declare_exchange(name=exchange_name, type=ExchangeType.TOPIC)
    await exchange.publish(
        Message(message.encode() if isinstance(message, str) else message),
        routing_key=routing_key
    )


def send_message_nowait(
    routing_key: str,
    message: Union[str, bytes],
    channel: AbstractChannel | None = None,
    loop: asyncio.AbstractEventLoop | None = None
):
    """
    Send a message, without blocking wait.
    If `channel` is not passed, use the global connection to get channel.
    If `loop` is not passed, use the running loop.
    """
    if loop is None:
        loop = asyncio.get_running_loop()
    loop.create_task(send_message(routing_key, message, channel))
# <-------------------- send end -------------------->


# <-------------------- send multi-thread -------------------->
async def create_cur_sustained_connection_thread_safe(**kwargs):
    """
    Create a thread-local connection object.
    This thread-local connection will be retrieved by `get_cur_sustained_connection_thread_safe`.
    """
    _id = threading.get_ident()
    with _connection_holder.lock:
        connections = _connection_holder.connections
        ret = connections.get(_id)
        if ret is not None:
            raise RuntimeError("Sustained connection already exists.")
        ret = await SustainedChannel.create(**kwargs)
        connections[_id] = ret
    return ret


async def get_cur_sustained_connection_thread_safe():
    """
    Get the thread-local connection object.
    If called before `create_cur_sustained_connection_thread_safe`,
    it will create a new connection without any argument.
    """
    _id = threading.get_ident()
    with _connection_holder.lock:
        connections = _connection_holder.connections
        ret = connections.get(_id)
        if ret is None:
            ret = await SustainedChannel.create()
            connections[_id] = ret
    return ret


async def close_sustained_connections_thread_safe():
    """
    Close all the thread-local connections object.
    Do nothing when connection is not created.
    """
    with _connection_holder.lock:
        connections = _connection_holder.connections
        _connection_holder.connections = dict()
    for conn in connections.values():
        await conn.channel.close()
        await conn.conn.close()
    connections.clear()


async def close_cur_sustained_connection_thread_safe():
    """
    Close the thread-local connection object.
    Do nothing when connection is not created.
    """
    _id = threading.get_ident()
    with _connection_holder.lock:
        ret = _connection_holder.connections.pop(_id, None)
    if ret is not None:
        await ret.channel.close()
        await ret.conn.close()


async def send_message_thread_safe(routing_key: str, message: Union[str, bytes]):
    """
    Send a message.
    This is only used for sending message with thread-local connection maintained by this module.
    """
    chan = (await get_cur_sustained_connection_thread_safe()).channel
    exchange_name = routing_key.split('.')[0]
    exchange = await chan.declare_exchange(name=exchange_name, type=ExchangeType.TOPIC)
    await exchange.publish(
        Message(message.encode() if isinstance(message, str) else message),
        routing_key=routing_key
    )


def send_message_nowait_thread_safe(routing_key: str, message: Union[str, bytes], loop: asyncio.AbstractEventLoop | None = None):
    """
    Send a message, without blocking wait.
    This is only used for sending message with thread-local connection maintained by this module.
    """
    if loop is None:
        loop = asyncio.get_running_loop()
    loop.create_task(send_message_thread_safe(routing_key, message))
# <-------------------- send multi-thread end -------------------->
