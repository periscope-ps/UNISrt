from requests import exceptions

class MundusError(Exception):
    """
    Base class for all Runtime exceptions
    """
    pass

class MundusMergeError(MundusError):
    """
    Exception thrown when an error occurs when merging containers.
    """
    pass

class MundusReferenceError(MundusError):
    """
    Exception thrown when a resource is requested at a bad url
    """
    pass

class RemovedEntityError(MundusError):
    """
    Exception thrown when attempting to modify a removed or deleted entity
    """
    pass

class ConnectionError(MundusError, exceptions.ConnectionError):
    """
    Exception thrown when a connection to a backend data store fails
    """
    pass

class SchemaError(MundusError):
    """
    Exception thrown when an object attempts to generate a child from an invalid schema
    """
    pass
