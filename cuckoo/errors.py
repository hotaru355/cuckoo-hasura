class HasuraServerError(Exception):
    pass


class HasuraClientError(Exception):
    pass


class RecordNotFoundError(Exception):
    pass


class InsertFailedError(Exception):
    pass


class MutationFailedError(Exception):
    pass
