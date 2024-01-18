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
