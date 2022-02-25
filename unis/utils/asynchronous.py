import asyncio

from lace.logging import trace

@trace("unis.utils")
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
        def _complete(future):
            if not future.cancelled() and future.exception():
                raise future.exception()
            new_loop.stop()
        loop.run_in_executor(None, new_loop.run_forever)
        fut = asyncio.run_coroutine_threadsafe(_mock(), new_loop)
        fut.add_done_callback(_complete)
        result = fut.result()
        if not new_loop.is_running():
            new_loop.close()
        return result
    else:
        return loop.run_until_complete(coro(*args, **kwargs))
