from rabbitmq_interface import get_sustained_connection, close_sustained_connection, send_message


async def f():
    await get_sustained_connection()
    await send_message('test', 'test')
    await close_sustained_connection()


import asyncio
loop = asyncio.get_event_loop()
loop.run_until_complete(f())
