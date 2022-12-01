class YandexAPIException(Exception):
    """Raised when an error occurs while executing a weather request"""
    pass


class RequirementsException(Exception):
    """Raised when the system does not meet the requirements"""
    pass
