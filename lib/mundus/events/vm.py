import dis

from collections import defaultdict, namedtuple
from logging import getLogger

_function = namedtuple('_function', ('code', 'name', 'is_method', 'defaults', 'kwds', 'annots', 'free'))
class FunctionAnalyzer(object):
    def __init__(self, fn=None, globals=None, is_method=False):
        self.vars, self.stack, self.fn = defaultdict(lambda: None), [], fn
        self.read, self.write, self.delete = set(), set(), set()
        self.funcs = defaultdict(lambda: lambda *args, **kwargs: None)
        self._store = {}
        if fn:
            self.globals = fn.__globals__
        else:
            self.globals = globals
        if fn and hasattr(fn, "__qualname__"):
            self.qname = fn.__qualname__
        else:
            self.qname = None
        self.is_method = is_method
    def _binary(self):
        x, y = self.stack.pop(), self.stack.pop()
        self.stack.append(None)
    def _unary(self):
        self.stack.pop()
        self.stack.append(None)
    def _noop(self, v=None):
        pass

    def POP_TOP(self, v):
        self.stack.pop()
    def ROT_TWO(self, v):
        self.stack[-1], self.stack[-2] = self.stack[-2:]
    def ROT_THREE(self, v):
        self.stack[-2], self.stack[-1], self.stack[-3] = self.stack[-3:]
    def DUP_TOP(self, v):
        self.stack.append(self.stack[-1])
    def DUP_TOP_TWO(self, v):
        self.stack.extend(self.stack[-2:])
    def UNARY_POSITIVE(self, v):
        self._noop()
    def UNARY_NEGATIVE(self, v):
        self._noop()
    def UNARY_NOT(self, v):
        self._noop()
    def UNARY_INVERT(self, v):
        self._noop()
    def GET_ITER(self, v):
        self._noop()
    def BINARY_POWER(self, v):
        self._binary()
    def BINARY_MULTIPLY(self, v):
        self._binary()
    def BINARY_MATRIX_MULTIPLY(self, v):
        self._binary()
    def BINARY_FLOOR_DIVIDE(self, v):
        self._binary()
    def BINARY_TRUE_DIVIDE(self, v):
        self._binary()
    def BINARY_MODULO(self, v):
        self._binary()
    def BINARY_ADD(self, v):
        self._binary()
    def BINARY_SUBTRACT(self, v):
        self._binary()
    def BINARY_SUBSCR(self, v):
        self._binary()
    def BINARY_LSHIFT(self, v):
        self._binary()
    def BINARY_RSHIFT(self, v):
        self._binary()
    def BINARY_AND(self, v):
        self._binary()
    def BINARY_XOR(self, v):
        self._binary()
    def BINARY_OR(self, v):
        self._binary()
    def INPLACE_POWER(self, v):
        self._binary()
    def INPLACE_MULTIPLY(self, v):
        self._binary()
    def INPLACE_MATRIX_MULTIPLY(self, v):
        self._binary()
    def INPLACE_FLOOR_DIVIDE(self, v):
        self._binary()
    def INPLACE_TRUE_DIVIDE(self, v):
        self._binary()
    def INPLACE_MODULO(self, v):
        self._binary()
    def INPLACE_ADD(self, v):
        self._binary()
    def INPLACE_SUBTRACT(self, v):
        self._binary()
    def INPLACE_LSHIFT(self, v):
        self._binary()
    def INPLACE_RSHIFT(self, v):
        self._binary()
    def INPLACE_AND(self, v):
        self._binary()
    def INPLACE_XOR(self, v):
        self._binary()
    def INPLACE_OR(self, v):
        self._binary()
    def STORE_SUBSCR(self, v):
        i, n, v = self.stack.pop(), self.stack.pop(), self.stack.pop()
        if n in self.vars:
            self.write.add(n)
    def DELETE_SUBSCR(self, v):
        self._binary()
        self.stack.pop()
    def GET_ANEXT(self, v):
        self._unary()
    def PRINT_EXPR(self, v):
        self.stack.pop()
    def RETURN_VALUE(self, v):
        self.stack.pop()
    def LOAD_BUILD_CLASS(self, v):
        self.stack.append(None)
    def SETUP_WITH(self, v):
        self.stack.extend([None, None, None])
    def WITH_CLEANUP_START(self, v):
        self.stack[-2:] = [None, None]
    def WITH_CLEANUP_FINISH(self, v):
        self.stack.pop()
        self.stack.pop()
    def STORE_NAME(self, n):
        self.STORE_ATTR(n)
    def UNPACK_SEQUENCE(self, c):
        ls = self.stack.pop()
        [self.stack.append(v) for v in ls]
    def STORE_ATTR(self, n):
        tos = self.stack.pop()
        self.stack.pop()
        if tos in self.vars:
            n = f"{self.vars[tos]}.{n}"
            self.vars[n] = n
            self.write.add(n)
    def DELETE_ATTR(self, n):
        tos = self.stack.pop()
        if tos in self.vars:
            n = f"{self.vars[tos]}.{n}"
            self.vars[n] = n
            self.delete.add(n)
    def STORE_GLOBAL(self, v):
        self.stack.pop()
    def LOAD_CONST(self, v):
        self.stack.append(v)
    def LOAD_NAME(self, v):
        self.stack.append(v)
    def BUILD_TUPLE(self, c):
        self.stack.append([self.stack.pop() for _ in range(c)])
    def BUILD_LIST(self, c):
        self.BUILD_TUPLE(c)
    def BUILD_SET(self, c):
        self.stack.append(set([self.stack.pop() for _ in range(c)]))
    def BUILD_MAP(self, c):
        d = {}
        for _ in range(c):
            v, k = self.stack.pop(), self.stack.pop()
            d[k] = v
        self.stack.append(d)
    def BUILD_CONST_KEY_MAP(self, c):
        d, k = {}, self.stack.pop()
        for i in range(c):
            d[k[i]] = self.stack.pop()
        self.stack.append(d)
    def BUILD_STRING(self, c):
        [self.stack.pop() for _ in range(c)]
        self.stack.append(None)
    def BUILD_TUPLE_UNPACK(self, c):
        self.BUILD_STRING(c)
    def BUILD_LIST_UNPACK(self, c):
        self.BUILD_STRING(c)
    def BUILD_SET_UNPACK(self, c):
        self.BUILD_STRING(c)
    def BUILD_MAP_UNPACK(self, c):
        self.BUILD_STRING(c)
    def BUILD_MAP_UNPACK_WITH_CALL(self, c):
        self.BUILD_STRING(c + 2)
        self.stack.append(None)
    def LOAD_ATTR(self, n):
        tos = self.stack.pop()
        if tos in self.vars:
            p = f"{self.vars[tos]}.{n}"
            self.vars[p] = p
            self.stack.append(p)
            self.read.add(p)
        else:
            attr = getattr(tos, n, None)
            if isinstance(tos, type) and callable(attr):
                attr = _function(attr.__code__, attr.__qualname__, True, None, None, None, None)
                self.funcs[attr.name] = attr
                attr = attr.name
            self.stack.append(attr)
    def COMPARE_OP(self, c):
        self._binary()
    def IMPORT_NAME(self, n):
        self.stack.pop()
        self.stack.pop()
        self.stack.append(None)
    def FOR_ITER(self, n):
        self._noop()
    def LOAD_GLOBAL(self, n):
        if n in self.globals:
            self.stack.append(self.globals[n])
        else:
            self.stack.append(None)
        
    def LOAD_FAST(self, n):
        if n in self._store:
            self.stack.append(self._store[n])
        else:
            self.stack.append(n)
    def STORE_FAST(self, n):
        tos = self.stack.pop()
        if isinstance(tos, str) and tos in self.vars:
            self.vars[n] = self.vars[tos]
        elif isinstance(tos, _function):
            self.funcs[n] = tos
        else:
            self._store[n] = tos
    def LOAD_CLOSURE(self, n):
        self.stack.append(None)
    def LOAD_DEREF(self, n):
        self.stack.append(None)
    def LOAD_CLASSDEREF(self, n):
        self.stack.append(None)
    def STORE_DEREF(self, n):
        self.stack.pop()
    def CALL_FUNCTION(self, n):
        args = [self.stack.pop() for _ in range(n)]
        fn = self.stack.pop()
        if not isinstance(fn, _function) and callable(fn):
            name = f"{fn.__name__}"
            if hasattr(fn, '__module__'):
                name = f"{fn.__module__}.{name}"
            self.funcs[name] = _function(fn.__code__, name, False, None, None, None, None)
            fn = name
        if fn in self.funcs:
            fn, tmp_analyzer = self.funcs[fn], FunctionAnalyzer(globals=self.globals)
            arg_map = {}
            for i, v in enumerate(reversed(args)):
                if v in self.vars:
                    if fn.is_method:
                        arg_map[fn.code.co_varnames[i+1]] = self.vars[v]
                    else:
                        arg_map[fn.code.co_varnames[i]] = self.vars[v]
            deps = tmp_analyzer.find_dependencies(fn.code, fn.name, arg_map, fn.is_method)
            self.read |= deps[0]
            self.write |= deps[1]
            self.delete |= deps[2]
        self.stack.append(None)
    def CALL_FUNCTION_KW(self, n):
        kwds = self.stack.pop()
        args = [self.stack.pop() for _ in range(n)]
        fn = self.stack.pop()
        if not isinstance(fn, _function) and callable(fn):
            self.funcs[fn.__name__] = _function(fn.__code__, fn.__name__, False, None, None, None, None)
            fn = fn.__name__
        if fn in self.funcs:
            fn, tmp_analyzer = self.funcs[fn], FunctionAnalyzer(globals=self.globals)
            arg_map = {}
            for k in reversed(kwds):
                if args[0] in self.vars:
                    arg_map[k] = self.vars[args[0]]
                args.pop(0)
            for i, v in enumerate(reversed(args)):
                if v in self.vars:
                    arg_map[fn.code.co_varnames[i]] = self.vars[v]
            deps = tmp_analyzer.find_dependencies(fn.code, fn.name, arg_map, fn.is_method)
            self.read |= deps[0]
            self.write |= deps[1]
            self.delete |= deps[2]
        self.stack.append(None)
    def CALL_FUNCTION_EX(self, n):
        kwargs, args, fn = self.stack.pop(), self.stack.pop(), self.stack.pop()
        if not isinstance(fn, _function) and callable(fn):
            self.funcs[fn.__name__] = _function(fn.__code__, fn.__name__, False, None, None, None, None)
            fn = fn.__name__
        if fn in self.funcs:
            fn, tmp_analyzer = self.funcs[fn], FunctionAnalyzer(globals=self.globals)
            arg_map = {}
            for k,v in kwargs.items():
                if v in self.vars:
                    arg_map[k] = self.vars[v]
            for i, v in enumerate(reversed(args)):
                if v in self.vars:
                    arg_map[fn.code.co_varnames[i]] = self.vars[v]
            deps = tmp_analyzer.find_dependencies(fn.code, fn.name, arg_map, fn.is_method)
            self.read |= deps[0]
            self.write |= deps[1]
            self.delete |= deps[2]
        self.stack.append(None)
    def LOAD_METHOD(self, n):
        obj = self.stack.pop()
        self.stack.extend([None, None])
    def CALL_METHOD(self, n):
        args = [self.stack.pop() for _ in range(n)]
        fn, obj = self.stack.pop(), self.stack.pop()
        self.stack.append()
    def MAKE_FUNCTION(self, n):
        defaults, kwds, annots, free = None, None, None, None
        name, code = self.stack.pop(), self.stack.pop()
        if n & 0x8: free = self.stack.pop()
        if n & 0x4: annots = self.stack.pop()
        if n & 0x2: kwds = self.stack.pop()
        if n & 0x1: defaults = self.stack.pop()
        self.stack.append(_function(code, name, False, defaults, kwds, annots, free))
    def BUILD_SLICE(self, n):
        if n == 2:
            [self.stack.pop(), self.stack.pop()]
            self.stack.append(None)
        else:
            [self.stack.pop(), self.stack.pop()]
            self.stack.append(None)

    def _find_class(self):
        d = self.globals
        for p in self.qname.split('.')[:-1]:
            if isnstance(d, dict):
                d = d[p]
            else:
                getattr(d, p)
        return d
    def find_dependencies(self, fn=None, name=None, arg_map=None, is_method=False):
        if hasattr(fn, '__globals__'):
            self.globals = fn.__globals__
        if hasattr(fn, '__qualname__'):
            self.qname = fn.__qualname__
        else:
            self.qname = name or self.qname
        fn = fn or self.fn.__code__
        name = name or self.fn.__name__
        instructions = dis.get_instructions(fn)
        args, argc = fn.co_varnames, fn.co_argcount
        if (isinstance(arg_map, type(None)) and argc > 1) or is_method:
            self._store[args[0]] = self._find_class()
        if isinstance(arg_map, type(None)):
            if argc == 0:
                raise ValueError(f"Listener does not take a resource argument - {name}")
            if argc < 2:
                arg_map = {args[0]: args[0]}
            else:
                arg_map = {args[1]: args[1]}

        self.vars.update(arg_map)
        for instr in instructions:
            try:
                getattr(self, instr.opname, self._noop)(instr.argval)
            except Exception as e:
                getLogger("unis.events.vm").debug(e)
        return self.read, self.write, self.delete
