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

class ManifestCronNotFound(CLIError):
    """
    Already Exist Registry.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, logger, message="Cron configuration was not found, when type set to SCHEDULED the Cron becomes mandatory."):
        logger.error(message)
        self.message = message
        super().__init__(self.message)
        
class ManifestTriggerInvalid(CLIError):
    """
    Already Exist Registry.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, logger, message="The ON_DEMAND and SCHEDULED types are accepted."):
        logger.error(message)
        self.message = message
        super().__init__(self.message)