#!/usr/bin/python3


class CLIError(Exception):
    """
    Generic Exception.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Something Went Wrong!"):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return str(self.message)


class HasFailedEventException(CLIError):
    """
    Has Failed Event Exception.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Stack has failed event."):
        self.message = message
        super().__init__(self.message)


class InvalidSchemaVersionException(CLIError):
    """
    Invalid Schema Version Exception

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Schema Version is invalid."):
        self.message = message
        super().__init__(self.message)
