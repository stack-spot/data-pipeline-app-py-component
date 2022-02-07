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

class DataframesNotFound(CLIError):
    """
    Already Exist Registry.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, logger, message="Could not find Dataframes."):
        logger.error(message)
        self.message = message
        super().__init__(self.message)


class DataframesCreateFailed(CLIError):
    """
    Already Exist Registry.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, logger, message="Create Failed Dataframes."):
        logger.error(message)
        self.message = message
        super().__init__(self.message)