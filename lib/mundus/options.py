from importlib.metadata import version

from mundus import config
from mundus.settings import OPTIONS, CONFIG_PATH
from mundus.utils import docs

_options = config.from_template(OPTIONS, ischild=True,
                                default_filepath=CONFIG_PATH, filevar="$MUNDUS_CONFIG_PATH",
                                version=version('mundus'))
def stringify():
    return "\n    ".join([f"{h}.{k}: {v}" for h,b in _options.items() for k,v in b.items()])

@docs.annotate(stringify())
def set(n, v):
    """
    :param n: Name of the option to modify
    :type n: str
    :param v: The value to set `n` to
    :type v: Any

    Modify the behavior of mundus.  Options are as follows.

    """
    try:
        h,k = n.split('.')
    except ValueError as e:
        raise ValueError(f"Unknown option '{n}'") from e
    try:
        _options[h][k] = v
    except KeyError as e:
        raise ValueError(f"Unknown option '{n}'") from e

@docs.annotate(stringify())
def get(n):
    """
    :param n: Name of the option to retrieve
    :type n: str

    Get  the current value of a configuration option

    """
    try:
        h,k = n.split('.')
    except ValueError as e:
        raise ValueError(f"Unknown option '{n}'") from e
    try:
        return _options[h][k]
    except KeyError as e:
        raise ValueError(f"Unknown option '{n}'") from e
