class UnisError(Exception):
    """
    Base class for all Runtime exceptions.
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
class ConnectionError(UnisError):
    """
    Exception thrown when the runtime fails to connect to
    remote instance.
    """
    def __init__(self, msg, code):
        super(ConnectionError, self).__init__(msg)
        self.status = code

