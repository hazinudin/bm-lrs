class LRSError(Exception):
    pass


class LRSConnectionError(LRSError):
    pass


class LRSValidationError(LRSError):
    pass


class LRSServerError(LRSError):
    pass
