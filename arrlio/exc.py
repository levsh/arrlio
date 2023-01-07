class ArrlioError(Exception):
    pass


class TaskError(ArrlioError):
    pass


class TaskTimeoutError(ArrlioError):
    pass


class TaskNoResultError(ArrlioError):
    pass


class NotFoundError(ArrlioError):
    pass


class TaskClosedError(ArrlioError):
    pass


class GraphError(ArrlioError):
    pass
