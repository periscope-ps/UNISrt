import threading, atexit, signal, asyncio
from mundus.containers import container
from mundus.containers.client import Client

def run():
    def cleanup():
        for c in container.remote_map.values():
            c.close()
        asyncio.get_event_loop().run_until_complete(Client.close_session())
    def _sigclose():
        cleanup()
        raise KeyboardInterrupt

    if threading.current_thread() is threading.main_thread():
        try:
            signal.signal(signal.SIGINT, _sigclose)
        except:
            pass
    atexit.register(cleanup)
