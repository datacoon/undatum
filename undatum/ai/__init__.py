"""AI service module for dataset documentation."""
from typing import Any, Optional

from .base import AIAPIError, AIConfigurationError, AIService, AIServiceError
from .config import get_ai_config, get_provider_config
from .providers import (
    LMStudioProvider,
    OllamaProvider,
    OpenAIProvider,
    OpenRouterProvider,
    PerplexityProvider,
)

# Provider registry
PROVIDERS = {
    'openai': OpenAIProvider,
    'openrouter': OpenRouterProvider,
    'ollama': OllamaProvider,
    'lmstudio': LMStudioProvider,
    'perplexity': PerplexityProvider,
}


def get_ai_service(provider: Optional[str] = None,
                   config: Optional[dict[str, Any]] = None) -> AIService:
    """Get AI service instance based on configuration.

    Args:
        provider: Provider name (openai, openrouter, ollama, lmstudio, perplexity)
                  If None, will be auto-detected from config
        config: Optional configuration dictionary. If None, will load from
                environment variables and config files

    Returns:
        Configured AI service instance

    Raises:
        AIConfigurationError: If provider is not configured or invalid

    Examples:
        >>> # Auto-detect from environment
        >>> service = get_ai_service()

        >>> # Explicit provider
        >>> service = get_ai_service('openai', {'api_key': '...', 'model': 'gpt-4'})

        >>> # From config file
        >>> service = get_ai_service('ollama')
    """
    # Load configuration
    full_config = get_ai_config(config or {})

    # Determine provider
    if provider:
        provider_name = provider.lower()
    else:
        provider_name = full_config.get('provider', '').lower()

    # Backward compatibility: if PERPLEXITY_API_KEY is set and no provider specified
    if not provider_name:
        import os
        if os.getenv('PERPLEXITY_API_KEY'):
            provider_name = 'perplexity'
            full_config['provider'] = 'perplexity'

    if not provider_name:
        raise AIConfigurationError(
            "No AI provider specified. Set UNDATUM_AI_PROVIDER environment variable, "
            "configure in undatum.yaml, or pass provider argument."
        )

    if provider_name not in PROVIDERS:
        raise AIConfigurationError(
            f"Unknown provider: {provider_name}. "
            f"Available providers: {', '.join(PROVIDERS.keys())}"
        )

    # Get provider class
    provider_class = PROVIDERS[provider_name]

    # Get provider-specific configuration
    provider_config = get_provider_config(full_config, provider_name)

    # Instantiate provider
    try:
        return provider_class(**provider_config)
    except AIConfigurationError as e:
        raise AIConfigurationError(
            f"Failed to configure {provider_name} provider: {str(e)}"
        ) from e


# Backward compatibility: export old function signatures
def get_fields_info(fields, language='English', ai_service: Optional[AIService] = None):
    """Get field descriptions (backward compatibility wrapper).

    Args:
        fields: List of field names or comma-separated string
        language: Language for descriptions
        ai_service: Optional AI service instance. If None, will auto-detect.

    Returns:
        Dictionary mapping field names to descriptions
    """
    if ai_service is None:
        ai_service = get_ai_service()

    # Handle both list and string input
    if isinstance(fields, str):
        fields = [f.strip() for f in fields.split(',')]

    return ai_service.get_fields_info(fields, language)


def get_description(data, language='English', ai_service: Optional[AIService] = None):
    """Get dataset description (backward compatibility wrapper).

    Args:
        data: Sample data as CSV string
        language: Language for description
        ai_service: Optional AI service instance. If None, will auto-detect.

    Returns:
        String description of the dataset
    """
    if ai_service is None:
        ai_service = get_ai_service()

    return ai_service.get_description(data, language)


def get_structured_metadata(data, fields, language='English', ai_service: Optional[AIService] = None):
    """Get structured metadata (backward compatibility wrapper).

    Args:
        data: Sample data as CSV string
        fields: List of field names
        language: Language for descriptions
        ai_service: Optional AI service instance. If None, will auto-detect.

    Returns:
        Dictionary with structured metadata fields
    """
    if ai_service is None:
        ai_service = get_ai_service()

    if isinstance(fields, str):
        fields = [f.strip() for f in fields.split(',')]

    return ai_service.get_structured_metadata(data, fields, language)


__all__ = [
    'AIService',
    'AIServiceError',
    'AIConfigurationError',
    'AIAPIError',
    'get_ai_service',
    'get_fields_info',
    'get_description',
    'get_structured_metadata',
    'OpenAIProvider',
    'OpenRouterProvider',
    'OllamaProvider',
    'LMStudioProvider',
    'PerplexityProvider',
]
