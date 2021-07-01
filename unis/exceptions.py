from requests import exceptions

class UnisError(Exception):
    """
    Base class for all Runtime exceptions
    """
    pass

class UnisReferenceError(UnisError):
    """
    Exception thrown when a resource is requested at a bad url
    """
    pass

class ConnectionError(UnisError, exceptions.ConnectionError):
    """
    Exception thrown when a connection to a backend data store fails
    """
    pass
