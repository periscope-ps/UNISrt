import logging
import os

from logging import DEBUG, INFO, CRITICAL, WARN, ERROR

NAME = "libdlt"
use_pad = False
pad = 0

def doTrace(l):
    global use_pad
    use_pad = l
def setLevel(level):
    getLogger().setLevel(level)
def getLogger():
    class ColourFormatter(logging.Formatter):
        def __init__(self, fmt, datefmt=None):
            self.colours = { 
                logging.CRITICAL: "\033[1;31m",
                logging.ERROR: "\033[0;31m",
                logging.WARNING: "\033[0;33m",
                logging.INFO: "\033[0;32m",
                logging.DEBUG: "\033[0;34m"
            }
            super(ColourFormatter, self).__init__(fmt, datefmt, '{')
        
        def _buildStr(self, *args, **kwargs):
            lens = []
            tmpargs = []
            tmpkwargs = []
            s = 0
            for arg in args:
                if arg == "":
                    tmpStr = "\"\", "
                else:
                    tmpStr = "{}".format(repr(arg))
                lens.append((len(tmpStr), tmpargs, len(tmpargs)))
                tmpargs.append(tmpStr)
                s += len(tmpStr)
            for k, arg in kwargs.items():
                if arg == "":
                    tmpStr = "\"\", "
                else:
                    tmpStr = "{}".format(repr(arg))
                lens.append((len(tmpStr) + len(k), tmpkwargs, len(tmpkwargs)))
                tmpkwargs.append((k, tmpStr))
                s += len(tmpStr)
                
            lens = sorted(lens, key=lambda v: v[0])
            try:
                size = os.get_terminal_size().columns - 60 - (pad if use_pad else 0)
            except OSError:
                size = 10000000
            while s > max(0, size):
                l, ls, i = lens.pop()
                ls[i] = "..." if isinstance(ls[i], str) else (ls[0], "...")
                s -= l
            args_str = ", ".join(tmpargs)
            kwargs_str = ", ".join(["{}: {}".format(k, v) for k, v in tmpkwargs])
            base_str = "{}{}{}".format("args=[{}]" if args_str else "{}", 
                                       ", " if args_str and kwargs_str else "", 
                                       "kwargs={{{}}}" if kwargs_str else "{}")
            base_str = base_str or "No arguments passed{}{}"
            if len(base_str) + len(args_str) + len(kwargs_str) > size:
                base_str = "Arguments too long, trucating..."
                args_str = ""
                kwargs_str = ""
            return base_str.format(args_str, kwargs_str)
            
        def format(self, record):
            old_fmt = self._style._fmt
            if record.args[0]:
                caller = " {}".format(record.args[0])
            else:
                caller = ""
            
            args, kwargs, isfunc = record.args[1]
            record.args = []
            if isfunc:
                record.msg = self._buildStr(*args, **kwargs)
            fmt = old_fmt.format(levelname=record.levelname[:1],
                                 pad="-" * (pad if use_pad and isfunc else 0),
                                 color=self.colours[record.levelno],
                                 reset="\033[0m",
                                 caller=caller)
            self._style._fmt = fmt
            result = logging.Formatter.format(self, record)
            self._style._fmt = old_fmt
            return result
    
    log = logging.getLogger(NAME)
    if not log.handlers:
        cout = logging.StreamHandler()
        log.addHandler(cout)
    
    for handler in log.handlers:
        handler.setFormatter(ColourFormatter("{pad}{color}[{levelname} {{asctime}}{caller}]{reset} {{message}}"))
    
    return log

class _log(object):
    op = getLogger().log
    def __init__(self, cls, immediate=False):
        self.cls = cls
        if immediate:
            self.op(cls, "", ({}, {}, False))
        
    def __call__(self, f):
        def wrapper(*args, **kwargs):
            # I just want you all to know I really hate this global.
            global pad
            compressed = (args, kwargs, True)
            self.op("", "{}.{}".format(self.cls, f.__name__), compressed)
            pad += 2
            try:
                result = f(*args, **kwargs)
            except:
                pad -= 2
                raise
            pad -= 2
            return result
            
        return wrapper

class info(_log):
    op = getLogger().info
class debug(_log):
    op = getLogger().debug
class error(_log):
    op = getLogger().error
class critical(_log):
    op = getLogger().critical
class warn(_log):
    op = getLogger().warning


if __name__ == "__main__":
    @debug("unittest")
    def test(a, b):
        pass
    
    setLevel(DEBUG)
    critical("This is a test", immediate=True)
    warn("This is a test", immediate=True)
    error("This is a test", immediate=True)
    debug("This is a test", immediate=True)
    info("This is a test", immediate=True)
    test("1", b="2")
