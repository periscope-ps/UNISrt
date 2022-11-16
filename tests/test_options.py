from mundus import options

def test_set_no_category():
    try:
        options.set("badvalue", True)
        assert False
    except ValueError:
        assert True

def test_set_bad_category():
    try:
        options.set("foobar.novalue", True)
        assert False
    except ValueError:
        assert True

def test_set_good_value():
    options.set("conn.auto_validate", "setvalue")
    assert options._options["conn"]["auto_validate"] == "setvalue"

def test_get_no_category():
    try:
        options.get("badvalue")
        assert False
    except ValueError:
        assert True

def test_get_bad_category():
    try:
        options.get("badvalue.auto_push")
        assert False
    except ValueError:
        assert True

def test_get_bad_option():
    try:
        options.get("conn.badvalue")
        assert False
    except ValueError:
        assert True

def test_get_good_value():
    v = options.get("conn.auto_push")
    assert v == False

