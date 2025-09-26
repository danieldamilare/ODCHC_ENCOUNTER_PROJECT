class ServiceError(Exception):
    """Base Class for all service-level errors"""

class MissingError(ServiceError):
    """Raised when a required resource is missing (like a row not found)"""

class InvalidReferenceError(ServiceError):
    """Raised when a foreign key points to a non-existent resource."""

class DuplicateError(ServiceError):
    """Raised when trying to create something that validate unique integrity"""

class ValidationError(ServiceError):
    """Raised when input values fail Validation rule (age < 0, bad gender, etc)"""

class AuthenticationError(ServiceError):
    """Raised when user credentials are wrongs"""