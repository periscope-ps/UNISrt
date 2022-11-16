import mundus, asyncio

def And(a, b):
    """
    This function serves as an alias for the logical `&` operation
    between two `Queries <mundus.query.Query>`.
    """
    return a & b
def Or(a, b):
    """
    This function serves as an alias for the logical `|` operation
    between two `Queries <mundus.query.Query>`.
    """
    return a | b
def Not(a):
    """
    This function serves as an alias for the logical `~` operation
    on a `Query <mundus.query.Query>`.
    """
    return ~a

class _sentinal(str):
    def __hash__(self): return hash(str(self))
    def __eq__(a,b): return False
    def __ne__(a,b): return False
    def __lt__(a,b): return False
    def __le__(a,b): return False
    def __gt__(a,b): return False
    def __ge__(a,b): return False

Any, All = _sentinal("Any"), _sentinal("All")

"""
SYMBOL := [!\\w]+
VALUE := int | float | string
PLAN := "{" ($and | $or | $not ":" "[" PLAN* "]") | SYMBOL ":" EXPR "}"
EXPR := ("{" $not | $gt | $ge | $lt | $le ":" EXPR "}") | VALUE
"""

class Prop(object):
    def __init__(self, n, q):
        self.n, self.q = n, q
    def __call__(self, v):
        return self.q & Query({self.n: v})
    def __eq__(self, v):
        return self.q & Query({self.n: v})
    def __ne__(self, v):
        return self.q & Query({self.n: {"$ne": v}})
    def __gt__(self, v):
        return self.q & Query({self.n: {"$gt": v}})
    def __ge__(self, v):
        return self.q & Query({self.n: {"$ge": v}})
    def __lt__(self, v):
        return self.q & Query({self.n: {"$lt": v}})
    def __le__(self, v):
        return self.q & Query({self.n: {"$le": v}})

class Query(object):
    """
    Queries operate as the basis for the entity pull pipeline in mundus.
    They can be chained using logical operators (by necessity, these use
    the python bitwise operators).  Any expression created through
    a combination of Query objects will construct a query plan that can
    be executed through the `execute <mundus.query.Query.execute>` function
    or implicitly through conventional python iteration (using `for`).

    The query plan generated by a combinatorial query is always request optimal
    and will automatically query the remotes and collections necessary to satisfy
    the expression.

    The generic Query results in an Any <- All query.  Such a query returns
    every entity from every connected remote.  This is potentially a very
    expensive request, but fortunately specifying conditions quickly reduces
    the complexity.  See the `Is <mundus.query.Is>` and `In <mundus.query.In>`
    specifiers.

    In addition to the remote pull plan, the query plan includes a filter
    function based on field predicates on the query.  Properties of the
    resulting entities can be asserted using conventional python
    comparison operators as shown in the example below.

    :Example:

    query = (Is(Node).name == "foo")
    for v in query.execute():
        print(v.name)

    Or inline

    :Example:
    
    expensive = [n for n in (Is(Node).cost > 1_000_000)]

    Or in the case of a more complex query

    :Example:

    conn1, conn2 = mundus.connect("http://server1"), mundus.connect("http://server2")
    query = ((In(conn1) & Is(Node)).name == "foo") | ((In(conn2) & Is(Port)).capacity > 100)
    print(v.name for v in query)

    This query will return zero to 2 entities, the first a Node from "server1" with the name "foo"
    and a Port from "server2" with "capacity" greater than 100.
    """
    def __init__(self, plan):
        self._result, self._plan, self.types = None, plan, {Any: All}

    def _p2s(self, plan):
        def cond(x):
            if isinstance(x, dict):
                return ",".join([f"{k}({v})" for k,v in x.items()])
            else:
                return str(x)
        if "$and" in plan:
            return f"({' & '.join([self._p2s(v) for v in plan['$and']])})"
        elif "$or" in plan:
            return f"({' | '.join([self._p2s(v) for v in plan['$or']])})"
        elif "$not" in plan:
            return f"~{self._p2s(plan['$not'])}"
        elif "$remote" in plan:
            return str(plan['$remote'])
        elif "$is" in plan:
            return f"IS_{plan['$is'].__name__}"
        else:
            return "".join([f"{k}({cond(v)})" for k,v in plan.items()])

    def _p2f(self, x, ctx, op, plan):
        if op == "$and":
            return all([self._p2f(x,ctx,k,v) for p in plan for k,v in p.items()])
        elif op == "$or":
            return any([all([self._p2f(x,ctx,k,v) for k,v in p.items()]) for p in plan])
        elif op == "$not":
            return not all([self._p2f(x,ctx,k,v) for p in plan for k,v in p.items()])
        elif op == "$eq":
            return getattr(x,ctx,Any) == plan
        elif op == "$ne":
            return getattr(x,ctx,Any) != plan
        elif op == "$lt":
            return getattr(x,ctx,Any) < plan
        elif op == "$gt":
            return getattr(x,ctx,Any) > plan
        elif op == "$le":
            return getattr(x,ctx,Any) <= plan
        elif op == "$ge":
            return getattr(x,ctx,Any) >= plan
        elif op == "$is":
            return isinstance(x, plan)
        elif op == "$remote":
            return x.container() == plan
        elif isinstance(plan, dict):
            return all([self._p2f(x,op,k,v) for k,v in plan.items()])
        else:
            return getattr(x,op,Any) == plan

    def _and_types(a, b):
        def _merge(a, b, default, isany=False):
            if a is All and b is All:
                r,d,a = All, set(), All
            elif a is All:
                r,d,a = (b | default), b, set()
            elif b is All:
                r,d,a = (a | default), a, set()
            else:
                r,d,a = (a | b | default), a | b, set()
            if isany:
                return a, d
            return r, d
        tys = (set(a.types) | set(b.types)) - set([Any])
        res,d = _merge(a.types[Any], b.types[Any], set(), isany=tys)
        return {**{r: _merge(a.types.get(r, set()), b.types.get(r, set()), d)[0] for r in tys},
                **{Any: res}}
    def _or_types(a, b):
        def _merge(a, b, default):
            if a is All and b is All:
                return All, set()
            elif a is All or b is All:
                return All, All
            else:
                return (a | b | default), a | b 
        tys = (set(a.types) | set(b.types)) - set([Any])
        res,d = _merge(a.types[Any], b.types[Any], set())
        return {**{r: _merge(a.types.get(r, set()), b.types.get(r, set()), d)[0] for r in tys},
                **{Any: res}}

    def __getattr__(self, n):
        return Prop(n, self)
    def __and__(self, other):
        q = Query({"$and": [self._plan, other._plan]})
        q.types = self._and_types(other)
        if not q._plan["$and"][0]:
            q._plan = q._plan["$and"][1]
        elif not q._plan["$and"][1]:
            q._plan = q._plan["$and"][0]
        return q
    def __or__(self, other):
        q = Query({"$or": [self._plan, other._plan]})
        q.types = self._or_types(other)
        if not q._plan["$or"][0]:
            q._plan = q._plan["$or"][1]
        elif not q._plan["$or"][1]:
            q._plan = q._plan["$or"][0]
        return q
    def __invert__(self):
        if "$not" in self._plan:
            q = Query(self._plan["$not"])
        else:
            q = Query({"$not": self._plan})
        q.types = self.types
        return q

    def __iter__(self):
        if not self._result:
            self._result = list(self._get_all())
        return iter(self._result)
    def _get_all(self):
        remotes = mundus.containers.container.netloc_map.keys()
        asyncio.get_event_loop().run_until_complete(self._pull_records(remotes))
        return self._filter_records(remotes)

    def _gen_types(self, remote):
        if self.types.get(remote, set()) is All or self.types[Any] is All:
            return set(remote._ty_children.keys())
        else:
            return self.types.get(remote, set()) | self.types[Any]

    async def _pull_records(self, remotes):
        calls = []
        for r in remotes:
            tys = set([v.colRef for v in self._gen_types(r) if getattr(v, "colRef", None)])
            calls.extend([r.pull(ty) for ty in tys])
        await asyncio.gather(*calls)
    def _filter_records(self, remotes):
        f = lambda x: all([self._p2f(x, None, k, v) for k,v in self._plan.items()])
        tys = {r: set([sty for ty in self._gen_types(r) for sty in r.subtypes(ty)]) for r in remotes}
        return list(filter(f, [v for r in remotes for ty in tys[r] for v in r.cols[ty].values()]))

    def __str__(self):
        tys = ''
        for r,ty in self.types.items():
            tys += f"\n\t{r} <- "
            if ty is All:
                tys += str(ty)
            else:
                tys += str([v.__name__ for v in ty])
        raw = '\n'.join([self._p2s(self._plan), str(self.types)])
        return f"""{self._p2s(self._plan)}{tys}"""
    def __repr__(self):
        return f"<Query {repr(self._plan)}>"

    def execute(self):
        """
        :return: Returns a list of `Entities <mundus.models.models.Entity>` that match the query
        :rtype: List[`Entity <mundus.models.models.Entity>`]

        Resolved the query and caches the result.  `execute` returns the matching results.
        """
        return list(self.__iter__())
    def first(self):
        """
        :return: Returns the first `Entity <mundus.models.models.Entity>` that match the query
        :rtype: `Entity <mundus.models.models.Entity>`

        Resolved the query and caches the result.  It returns the first valid match.
        """
        if self._result:
            try: return self._result[0]
            except IndexError: return None
        try: return next(self.__iter__())
        except StopIteration: return None
    def reset(self):
        """
        Reset the internal cursor to evict the local result cache.  Calls to `execute`, `first` and
        iteration will just reuse the local cache after the first call unless this is called first.
        """
        self._result = None

class In(Query):
    """
    `In` is a constructor for a specialized query for a remote.  Any In query returns results
    in a specific remote.
    """
    def __init__(self, remote):
        super().__init__({"$remote": remote})
        self.types[Any], self.types[remote] = set(), All

class Is(Query):
    """
    `Is` queries filter results for a specific object type and subclasses.  For instance,
    an `Is(Node)` query will also return `ComputeNodes` and `PhysicalNodes`.
    """
    def __init__(self, model):
        super().__init__({"$is": model})
        self.types[Any] = set([model])
