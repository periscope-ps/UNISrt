class UnisError(Exception):
    """
    Base class for all Runtime exceptions.
    """
    pass
class UnisAttributeError(AttributeError, UnisError):
    """
    Exception thrown when querying an attribute that does not
    exist in a resource.
    """
    pass
class UnisReferenceError(UnisError):
    """
    Exception thrown when the runtime attempts to resolve an
    unresolvable reference.
    """
    
    def __init__(self, msg, hrefs):
        super(UnisReferenceError, self).__init__(msg)
        self.hrefs = hrefs
    def __str__(self):
        return super().__str__() + ": " + str(self.hrefs)
    
class ConnectionError(UnisError):
    """
    Exception thrown when the runtime fails to connect to
    remote instance.
    """
    def __init__(self, msg, code):
        super(ConnectionError, self).__init__(msg)
        self.status = code


class CollectionIndexError(UnisError):
    """
    Exception thrown when an index attempts to access a resource
    not currently in the Index.
    """
    pass
