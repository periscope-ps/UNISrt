from collections import defaultdict

#from mundus import threads

_channels = defaultdict(list)
_dependencies = defaultdict(lambda: defaultdict(list))

def _read_before_write(a, b):
    result = set()
    for r in a.access.reads:
        for w in b.access.writes:
            if r == w:
                result.add(r)
    return result

def _dependency_has_cycle(d, ch, p=None):
    p = p or []
    if d in p:
        return p + [d]
    for fn in _dependencies[ch][d]:
        res = _dependency_has_cycle(fn, ch, p + [d])
        if res:
            return res
    return []

def register(fn):
    if not hasattr(fn, 'access'):
        _log.error("Cannot register listener without event type, please use a decorator from unis.events")
    else:
        for c in fn.ch:
            if fn not in _channels[c]:
                # Add dependencies for function
                for other in _channels[c]:
                    r_w, w_r = _read_before_write(fn, other), _read_before_write(other, fn)
                    if r_w: _dependencies[c][fn].append(other)
                    if w_r: _dependencies[c][other].append(fn)
                # Check dependencies for cycles
                for d in _channels[c]:
                    p = [f"{p.__module__}.{p.__name__}" for p in _dependency_has_cycle(d, c)]
                    if p:
                        nl, sep = "\n\t", " ->\n\t"
                        _log.error(f"Unable to determine ordering in listener: {nl}{sep.join(p)}{nl}"
                                   "Please use the unis.events.depends_on decorator to force an ordering")
                        return
        if not hasattr(fn, 'depends_on'):
            fn.depends_on = []
        _channels[c].append(fn)

def publish(obj, ch):
    done, fringe = [], [fn for fn in _channels[ch] if not _dependencies[ch][fn]]
    while fringe:
        for fn in fringe:
            if isinsatnce(fn.ty, obj):
                fn(obj)
        # hmmm are threads even doing anything worth considering here?
        #events = [threads.dispatch(fn, obj) for fn in fringe if isinstance(fn.ty, obj)]
        #threads.wait_for(events)
        done += fringe
        fringe = []
        for fn in _channels[ch]:
            if fn not in done and all([d in done for d in (_dependencies[ch][fn] + fn.depends_on)]):
                fringe.append(fn)
    obj._event_callback(ch)
