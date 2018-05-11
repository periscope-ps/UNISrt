class UnisError(Exception):
    pass
class UnisReferenceError(UnisError):
    def __init__(self, msg, hrefs):
        super(UnisReferenceError, self).__init__(msg)
        self.hrefs = hrefs
class ConnectionError(UnisError):
    def __init__(self, msg, code):
        super(ConnectionError, self).__init__(msg)
        self.status = code

