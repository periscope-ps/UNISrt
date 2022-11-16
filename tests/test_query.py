from mundus import query

class MockContainer(object):
    def __init__(self):
        self._ty_children = {}
        self.cols = {}
    def container(self):
        pass
    def subtypes(self):
        return []
    async def pull(self, path):
        pass

class sentinal_model(object): pass
class sentinal_complexmodel(object): pass
class sentinal_conn(object):
    def __str__(self):
        return "@mock_conn"
    
    
# mock mundus.containers.container.netloc_map


# Test generic Query
def test_generic():
    q = query.Query({})
    assert q.types == {query.Any: query.All}
    assert q._plan == {}

def test_repr():
    q = query.Query({})
    repr(q)

def test_In():
    conn = sentinal_conn()
    q = query.In(conn)

    assert q.types == {conn: query.All, query.Any: set()}
    assert q._plan == {"$remote": conn}

def test_str_In():
    conn = sentinal_conn()
    q = query.In(conn)

    assert str(q) == "@mock_conn\n\tAny <- []\n\t@mock_conn <- All"

def test_Is():
    q = query.Is(sentinal_model)

    assert q.types == {query.Any: set([sentinal_model])}
    assert q._plan == {"$is": sentinal_model}

def test_prop():
    q = query.Query({})
    q = q.name == "foo"
    assert q.types == {query.Any: query.All}
    assert q._plan == {"name": "foo"}

def test_prop_cal():
    q = query.Query({})
    q = q.name("foo")
    assert q.types == {query.Any: query.All}
    assert q._plan == {"name": "foo"}

def test_and():
    conn = sentinal_conn()
    q = query.Is(sentinal_model) & query.In(conn)

    assert q.types == {query.Any: set(), conn: set([sentinal_model])}
    assert q._plan == {"$and": [{"$is": sentinal_model}, {"$remote": conn}]}

def test_and_empty_righ():
    q = query.Query({"value": 5}) & query.Query({})

    assert q._plan == {"value": 5}

def test_str_and():
    q = query.Query({"value": 5}) & query.Query({"v": 10})

    assert str(q) == "(value(5) & v(10))\n\tAny <- All"
def test_or():
    conn = sentinal_conn()
    q = query.Is(sentinal_model) | query.In(conn)

    assert q.types == {query.Any: set([sentinal_model]), conn: query.All}
    assert q._plan == {"$or": [{"$is": sentinal_model}, {"$remote": conn}]}


def test_or_empty_left():
    q = query.Query({}) | query.Query({"value": 5})

    assert q._plan == {"value": 5}

def test_or_empty_right():
    q = query.Query({"value": 5}) | query.Query({})

    assert q._plan == {"value": 5}

def test_str_or():
    q = query.Query({"value": 5}) | query.Query({"value": 10})

    assert str(q) == "(value(5) | value(10))\n\tAny <- All"
    
def test_not():
    q = ~query.Is(sentinal_model)

    assert q.types == {query.Any: set([sentinal_model])}
    assert q._plan == {"$not": {"$is": sentinal_model}}

def test_dual_not():
    q = ~(~query.Is(sentinal_model))

    assert q._plan == {"$is": sentinal_model}

def test_str_not():
    q = ~query.Is(sentinal_model)

    assert str(q) == "~IS_sentinal_model\n\tAny <- ['sentinal_model']"

def test_ne():
    q = query.Query({}).value != 5

    assert q._plan == {"value": {"$ne": 5}}

def test_str_value():
    q = query.Query({}).value == 5

    assert str(q) == "value(5)\n\tAny <- All"

def test_str_op():
    q = query.Query({}).value != 5

    assert str(q) == "value($ne(5))\n\tAny <- All"

def test_gt():
    q = query.Query({}).value > 5

    assert q._plan == {"value": {"$gt": 5}}

def test_ge():
    q = query.Query({}).value >= 5

    assert q._plan == {"value": {"$ge": 5}}

def test_lt():
    q = query.Query({}).value < 5

    assert q._plan == {"value": {"$lt": 5}}

def test_le():
    q = query.Query({}).value <= 5

    assert q._plan == {"value": {"$le": 5}}

def test_complex_query():
    conn1 = sentinal_conn()
    conn2 = sentinal_conn()

    q = ((query.In(conn1) & query.Is(sentinal_model)).name == "foo") | \
        ((query.In(conn2) & query.Is(sentinal_complexmodel)).value < 5)

    assert q._plan == {"$or": [{"$and": [{"$and": [{"$remote": conn1}, {"$is": sentinal_model}]},
                                         {"name": "foo"}]},
                               {"$and": [{"$and": [{"$remote": conn2}, {"$is": sentinal_complexmodel}]},
                                         {"value": {"$lt": 5}}]}]}
def test_And():
    q1 = query.Query({"a": 5}) & query.Query({"b": 10})
    q2 = query.And(query.Query({"a": 5}), query.Query({"b": 10}))

    assert q1.types == q2.types
    assert q1._plan == q2._plan

def test_Or():
    q1 = query.Query({"a": 5}) | query.Query({"b": 10})
    q2 = query.Or(query.Query({"a": 5}), query.Query({"b": 10}))

    assert q1.types == q2.types
    assert q1._plan == q2._plan

def test_Not():
    q1 = ~query.Query({"a": 5})
    q2 = query.Not(query.Query({"a": 5}))

    assert q1.types == q2.types
    assert q1._plan == q2._plan



# Test $and
# Test $or
# Test $not
# Test $eq
# Test $ne
# Test $lt
# Test $gt
# Test $le
# Test $ge
# Test $in
# Test $is
# Test $remote
# Test props
# Test print ^
# Test repr ^
# Test actual filter responses ^
# Test iterator
# Test execute
# Test first
# Test reset

# Test And class
# Test Or class
# Test Not class
