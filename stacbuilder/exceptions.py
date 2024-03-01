"""Exception specific to the STAC catalog builder application.

Here we define an exception class hierarchy so we can easier distinguish our
own exceptions from other exception type and handle them accordingly.
"""


class STACBuilderException(Exception):
    """Root of our exception hierarchy."""

    pass


class SettingsInvalid(STACBuilderException):
    """Raised when settings incorrect or dissallowed values"""

    pass


class InvalidOperation(STACBuilderException):
    """Raised when some state or settings are not set, and the operation can not be executed."""

    pass


class InvalidConfiguration(STACBuilderException):
    """Raised when some settings in the configuration objects don't make sense.

    This is for cases that are a bit more complex than Pydantic typically
    detects automatically, e.g. when setting A is used you will also need the
    value for setting.

    For example:
        You want to add alternate links for S3, but you did not fill in the
        value for the S3 bucket that needs to be in the URL.
    """

    pass


class DataValidationError(Exception):
    """Raised when one of the validations on our data processing fails."""
