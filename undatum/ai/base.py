"""Base AI service interface for dataset documentation."""
from abc import ABC, abstractmethod
from typing import Optional


class AIServiceError(Exception):
    """Base exception for AI service errors."""
    pass


class AIConfigurationError(AIServiceError):
    """Raised when AI service configuration is invalid."""
    pass


class AIAPIError(AIServiceError):
    """Raised when AI API call fails."""
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class AIService(ABC):
    """Abstract base class for AI service providers."""

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 model: Optional[str] = None, timeout: int = 30):
        """Initialize AI service.

        Args:
            api_key: API key for authentication
            base_url: Base URL for API endpoint (optional, provider-specific defaults)
            model: Model name to use
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.timeout = timeout

    @abstractmethod
    def get_fields_info(self, fields: list[str], language: str = 'English') -> dict[str, str]:
        """Get descriptions for a list of field names.

        Args:
            fields: List of field names to describe
            language: Language for descriptions (default: 'English')

        Returns:
            Dictionary mapping field names to their descriptions

        Raises:
            AIConfigurationError: If service is not properly configured
            AIAPIError: If API call fails
        """
        pass

    @abstractmethod
    def get_description(self, data: str, language: str = 'English') -> str:
        """Get a description of the dataset.

        Args:
            data: Sample data as CSV string
            language: Language for description (default: 'English')

        Returns:
            String description of the dataset

        Raises:
            AIConfigurationError: If service is not properly configured
            AIAPIError: If API call fails
        """
        pass

    @abstractmethod
    def get_structured_metadata(self, data: str, fields: list[str],
                                language: str = 'English') -> dict:
        """Get structured dataset metadata from sample data.

        Args:
            data: Sample data as CSV string
            fields: List of field names
            language: Language for descriptions (default: 'English')

        Returns:
            Dictionary with structured metadata fields

        Raises:
            AIConfigurationError: If service is not properly configured
            AIAPIError: If API call fails
        """
        pass

    def _validate_config(self) -> None:
        """Validate that required configuration is present.

        Raises:
            AIConfigurationError: If configuration is invalid
        """
        if not self.api_key:
            raise AIConfigurationError(f"API key is required for {self.__class__.__name__}")
        if not self.model:
            raise AIConfigurationError(f"Model is required for {self.__class__.__name__}")
