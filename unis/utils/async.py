import asyncio

def make_async(coro, *args, **kwargs):
    async def _mock():
        return await coro(*args, **kwargs)
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    if loop.is_running():
        new_loop = asyncio.new_event_loop()
        loop.run_in_executor(None, new_loop.run_forever)
        fut = asyncio.run_coroutine_threadsafe(_mock(), new_loop)
        fut.add_done_callback(lambda f: new_loop.stop())
        result = fut.result(10)
        new_loop.close()
        return result
    else:
        return loop.run_until_complete(coro(*args, **kwargs))
