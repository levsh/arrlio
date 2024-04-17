class ArrlioError(Exception):
    """Base exception."""


class InternalError(ArrlioError):
    pass


class HooksError(ArrlioError):
    def __init__(self, *args, exceptions: list[Exception] | tuple[Exception] | None = None):
        super().__init__(*args)
        self.exceptions = exceptions

    def __str__(self):
        if self.exceptions is not None:
            return f"{self.exceptions}"
        return super().__str__()

    def __repr__(self):
        if self.exceptions is not None:
            return f"{self.exceptions}"
        return super().__repr__()


class TaskError(ArrlioError):
    def __init__(self, *args, exceptions: list[Exception] | None = None):
        super().__init__(*args)
        self.exceptions = exceptions

    def __str__(self):
        if self.exceptions is not None:
            return f"{self.exceptions}"
        return super().__str__()

    def __repr__(self):
        if self.exceptions is not None:
            return f"{self.exceptions}"
        return super().__repr__()


class TaskClosedError(ArrlioError):
    pass


class TaskTimeoutError(ArrlioError):
    pass


class TaskCancelledError(ArrlioError):
    pass


class TaskResultError(ArrlioError):
    pass


class NotFoundError(ArrlioError):
    pass


class GraphError(ArrlioError):
    pass
