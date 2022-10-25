import threading, atexit, signal
from unis import threads
from unis.containers import container

def run():
    def cleanup():
        threads.shutdown()
        for c in container.instances.values(): c.forget()
    def _sigclose():
        cleanup()
        raise KeyboardInterrupt

    if threading.current_thread() is threading.main_thread():
        try: signal.signal(signal.SIGINT, _sigclose)
        except: pass
    atexit.register(self.cleanup)
