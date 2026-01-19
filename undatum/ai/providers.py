"""AI service provider implementations."""
import json
import os
import re
import time
from typing import Callable, Optional

import requests

from ..constants import EU_DATA_THEMES


def _truncate_sample(data: str, max_length: int) -> str:
    if len(data) <= max_length:
        return data
    return data[:max_length] + "\n... (truncated)"


def _metadata_prompt(data: str, fields: list[str], language: str) -> str:
    fields_str = ", ".join(fields)
    themes_str = json.dumps(EU_DATA_THEMES, ensure_ascii=False)
    return (
        "I have the following CSV data sample:\n"
        f"{data}\n"
        f"Field names: {fields_str}\n\n"
        f"Generate structured dataset metadata in {language}. "
        "Return a JSON object with these keys only:\n"
        "- title (string)\n"
        "- keywords (array of strings)\n"
        "- geographic_coverage (object with countries, regions, coordinates_present)\n"
        "- temporal_coverage (object with start, end, granularity)\n"
        "- languages (array of objects with code and confidence)\n"
        "- data_theme (object with label and uri, or null)\n"
        "- confidence (object with per-section confidence 0-1)\n"
        "- evidence (object mapping keys to brief evidence strings)\n\n"
        "Use the EU Data Theme vocabulary list for data_theme selection:\n"
        f"{themes_str}\n"
        "If you cannot determine a field, set it to null or empty."
    )


def _extract_json_payload(text: str) -> dict:
    json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if json_match:
        return json.loads(json_match.group(1))
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        return json.loads(json_match.group(0))
    return json.loads(text)

from .base import AIAPIError, AIConfigurationError, AIService


def retry_with_backoff(func: Callable, max_retries: int = 3, initial_delay: float = 1.0,
                      backoff_factor: float = 2.0, retry_statuses: tuple = (429, 500, 502, 503, 504)):
    """Retry a function with exponential backoff for rate limiting and server errors.

    Args:
        func: Function to retry (should raise requests.exceptions.RequestException)
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
        backoff_factor: Factor to multiply delay by for each retry
        retry_statuses: HTTP status codes that should trigger a retry

    Returns:
        Result of the function call

    Raises:
        AIAPIError: If all retries are exhausted
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except requests.exceptions.RequestException as e:
            last_exception = e
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None

            # Only retry on specific status codes or if status_code is None (network error)
            if status_code not in retry_statuses and status_code is not None:
                raise

            # Don't retry on last attempt
            if attempt >= max_retries:
                break

            # Extract retry-after header if present (for 429 errors)
            retry_after = None
            if hasattr(e, 'response') and e.response is not None:
                retry_after = e.response.headers.get('Retry-After')
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        pass

            # Wait before retrying
            time.sleep(delay)
            delay *= backoff_factor

    # All retries exhausted, raise the last exception
    status_code = getattr(last_exception.response, 'status_code', None) if hasattr(last_exception, 'response') else None
    error_msg = f"API request failed after {max_retries + 1} attempts: {str(last_exception)}"

    if status_code == 429:
        error_msg += "\nRate limit exceeded. Please wait a moment and try again, or check your API usage limits."
    elif status_code in (500, 502, 503, 504):
        error_msg += "\nServer error. The API may be temporarily unavailable. Please try again later."

    raise AIAPIError(error_msg,
                    status_code=status_code,
                    response=getattr(last_exception.response, 'text', None) if hasattr(last_exception, 'response') else None)


class OpenAIProvider(AIService):
    """OpenAI API provider."""

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 model: Optional[str] = None, timeout: int = 30):
        """Initialize OpenAI provider.

        Args:
            api_key: OpenAI API key (defaults to OPENAI_API_KEY env var)
            base_url: Base URL (defaults to https://api.openai.com/v1)
            model: Model name (defaults to gpt-4o-mini)
            timeout: Request timeout in seconds
        """
        api_key = api_key or os.getenv('OPENAI_API_KEY')
        base_url = base_url or 'https://api.openai.com/v1'
        model = model or 'gpt-4o-mini'
        super().__init__(api_key, base_url, model, timeout)
        self._validate_config()

    def get_fields_info(self, fields: list[str], language: str = 'English') -> dict[str, str]:
        """Get field descriptions using OpenAI API."""
        fields_str = ', '.join(fields)
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide clear, concise descriptions of data fields in {language}. Always respond with valid JSON."
                },
                {
                    "role": "user",
                    "content": f"Please describe these data fields in {language}: {fields_str}. Provide a description for each field explaining what it represents. Return your response as a JSON object with a 'fields' array containing objects with 'name' and 'description' keys."
                }
            ],
            "response_format": {
                "type": "json_object"
            },
            "temperature": 0.3
        }

        def _make_request():
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            return response

        try:
            response = retry_with_backoff(_make_request)
            data = response.json()

            content = data["choices"][0]["message"]["content"]
            result = json.loads(content)

            # Validate and convert to expected format
            if "fields" not in result:
                raise AIAPIError("Invalid response format: missing 'fields' key")

            field_dict = {}
            for field_info in result["fields"]:
                if "name" not in field_info or "description" not in field_info:
                    continue
                field_dict[field_info["name"]] = field_info["description"]

            # Ensure all requested fields are in the result
            for field in fields:
                if field not in field_dict:
                    field_dict[field] = f"Field: {field}"

            return field_dict

        except AIAPIError:
            raise
        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"OpenAI API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse OpenAI response: {str(e)}") from e

    def get_description(self, data: str, language: str = 'English') -> str:
        """Get dataset description using OpenAI API."""
        # Truncate data if too large (OpenAI has token limits)
        # Use a more conservative limit to account for prompt overhead
        MAX_DATA_LENGTH = 3000
        if len(data) > MAX_DATA_LENGTH:
            data = data[:MAX_DATA_LENGTH] + "\n... (truncated)"

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        user_content = f"""I have the following CSV data sample:
{data}
Please provide a short description of this dataset in {language}. Consider this as a sample of a larger dataset. Don't generate code or data examples.
Return your response as a JSON object with a "description" key."""

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide concise dataset descriptions in {language}. Always respond with valid JSON."
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            "response_format": {
                "type": "json_object"
            },
            "temperature": 0.3
        }

        def _make_request():
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            return response

        try:
            response = retry_with_backoff(_make_request)
            response_data = response.json()

            content = response_data["choices"][0]["message"]["content"]
            result = json.loads(content)

            if "description" in result:
                return result["description"]
            else:
                # Fallback: return the content as-is if structure is different
                return content

        except AIAPIError:
            raise
        except requests.exceptions.RequestException as e:
            error_msg = f"OpenAI API request failed: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    if 'error' in error_detail:
                        error_info = error_detail['error']
                        if 'message' in error_info:
                            error_msg += f"\nError details: {error_info['message']}"
                        if 'code' in error_info:
                            error_msg += f"\nError code: {error_info['code']}"
                except (ValueError, KeyError):
                    # If we can't parse the error response, include the raw text
                    error_text = getattr(e.response, 'text', None)
                    if error_text:
                        error_msg += f"\nResponse: {error_text[:500]}"
            raise AIAPIError(error_msg,
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError:
            # If JSON parsing fails, try to extract description from text
            try:
                content = response_data["choices"][0]["message"]["content"]
                return content
            except (KeyError, IndexError) as e:
                raise AIAPIError("Failed to extract description from OpenAI response") from e

    def get_structured_metadata(self, data: str, fields: list[str],
                                language: str = 'English') -> dict:
        """Get structured metadata using OpenAI API."""
        MAX_DATA_LENGTH = 3000
        data = _truncate_sample(data, MAX_DATA_LENGTH)
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        user_content = _metadata_prompt(data, fields, language)

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        f"You are a data documentation assistant. "
                        f"Return structured dataset metadata in {language} as valid JSON."
                    )
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            "response_format": {
                "type": "json_object"
            },
            "temperature": 0.2
        }

        def _make_request():
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            return response

        try:
            response = retry_with_backoff(_make_request)
            response_data = response.json()
            content = response_data["choices"][0]["message"]["content"]
            return _extract_json_payload(content)
        except AIAPIError:
            raise
        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"OpenAI API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse OpenAI response: {str(e)}") from e


class OpenRouterProvider(AIService):
    """OpenRouter API provider (OpenAI-compatible)."""

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 model: Optional[str] = None, timeout: int = 30):
        """Initialize OpenRouter provider.

        Args:
            api_key: OpenRouter API key (defaults to OPENROUTER_API_KEY env var)
            base_url: Base URL (defaults to https://openrouter.ai/api/v1)
            model: Model name (defaults to openai/gpt-4o-mini)
            timeout: Request timeout in seconds
        """
        api_key = api_key or os.getenv('OPENROUTER_API_KEY')
        base_url = base_url or 'https://openrouter.ai/api/v1'
        model = model or 'openai/gpt-4o-mini'
        super().__init__(api_key, base_url, model, timeout)
        self._validate_config()

    def get_fields_info(self, fields: list[str], language: str = 'English') -> dict[str, str]:
        """Get field descriptions using OpenRouter API."""
        fields_str = ', '.join(fields)
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/datacoon/undatum",
            "X-Title": "Undatum Data Analysis"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide clear, concise descriptions of data fields in {language}. Always respond with valid JSON."
                },
                {
                    "role": "user",
                    "content": f"Please describe these data fields in {language}: {fields_str}. Provide a description for each field explaining what it represents. Return your response as a JSON object with a 'fields' array containing objects with 'name' and 'description' keys."
                }
            ],
            "response_format": {
                "type": "json_object"
            },
            "temperature": 0.3
        }

        def _make_request():
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            return response

        try:
            response = retry_with_backoff(_make_request)
            data = response.json()

            content = data["choices"][0]["message"]["content"]
            result = json.loads(content)

            field_dict = {}
            if "fields" in result:
                for field_info in result["fields"]:
                    if "name" in field_info and "description" in field_info:
                        field_dict[field_info["name"]] = field_info["description"]

            # Ensure all requested fields are in the result
            for field in fields:
                if field not in field_dict:
                    field_dict[field] = f"Field: {field}"

            return field_dict

        except AIAPIError:
            raise
        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"OpenRouter API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse OpenRouter response: {str(e)}") from e

    def get_description(self, data: str, language: str = 'English') -> str:
        """Get dataset description using OpenRouter API."""
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/datacoon/undatum",
            "X-Title": "Undatum Data Analysis"
        }

        user_content = f"""I have the following CSV data sample:
{data}
Please provide a short description of this dataset in {language}. Consider this as a sample of a larger dataset. Don't generate code or data examples.
Return your response as a JSON object with a "description" key."""

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide concise dataset descriptions in {language}. Always respond with valid JSON."
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            "response_format": {
                "type": "json_object"
            },
            "temperature": 0.3
        }

        # Truncate data if too large
        MAX_DATA_LENGTH = 5000
        if len(data) > MAX_DATA_LENGTH:
            data = data[:MAX_DATA_LENGTH] + "\n... (truncated)"
            payload["messages"][1]["content"] = f"""I have the following CSV data sample:
{data}
Please provide a short description of this dataset in {language}. Consider this as a sample of a larger dataset. Don't generate code or data examples. Return JSON with a 'description' key."""

        def _make_request():
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            return response

        try:
            response = retry_with_backoff(_make_request)
            response_data = response.json()

            content = response_data["choices"][0]["message"]["content"]
            result = json.loads(content)

            if "description" in result:
                return result["description"]
            else:
                return content

        except AIAPIError:
            raise
        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"OpenRouter API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError:
            try:
                content = response_data["choices"][0]["message"]["content"]
                return content
            except (KeyError, IndexError) as e:
                raise AIAPIError("Failed to extract description from OpenRouter response") from e

    def get_structured_metadata(self, data: str, fields: list[str],
                                language: str = 'English') -> dict:
        """Get structured metadata using OpenRouter API."""
        MAX_DATA_LENGTH = 5000
        data = _truncate_sample(data, MAX_DATA_LENGTH)
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/datacoon/undatum",
            "X-Title": "Undatum Data Analysis"
        }
        user_content = _metadata_prompt(data, fields, language)

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        f"You are a data documentation assistant. "
                        f"Return structured dataset metadata in {language} as valid JSON."
                    )
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            "response_format": {
                "type": "json_object"
            },
            "temperature": 0.2
        }

        def _make_request():
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            return response

        try:
            response = retry_with_backoff(_make_request)
            response_data = response.json()
            content = response_data["choices"][0]["message"]["content"]
            return _extract_json_payload(content)
        except AIAPIError:
            raise
        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"OpenRouter API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse OpenRouter response: {str(e)}") from e


class OllamaProvider(AIService):
    """Ollama local API provider."""

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 model: Optional[str] = None, timeout: int = 30):
        """Initialize Ollama provider.

        Args:
            api_key: Not used for Ollama (kept for interface compatibility)
            base_url: Base URL (defaults to http://localhost:11434)
            model: Model name (defaults to llama3.2)
            timeout: Request timeout in seconds
        """
        base_url = base_url or os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')
        model = model or 'llama3.2'
        super().__init__(api_key, base_url, model, timeout)
        if not self.model:
            raise AIConfigurationError("Model is required for OllamaProvider")

    def _validate_config(self) -> None:
        """Ollama doesn't require API key."""
        if not self.model:
            raise AIConfigurationError("Model is required for OllamaProvider")

    def get_fields_info(self, fields: list[str], language: str = 'English') -> dict[str, str]:
        """Get field descriptions using Ollama API."""
        fields_str = ', '.join(fields)
        url = f"{self.base_url}/api/chat"

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide clear, concise descriptions of data fields in {language}. Always respond with valid JSON only."
                },
                {
                    "role": "user",
                    "content": f"""Please describe these data fields in {language}: {fields_str}.
Return a JSON object with a "fields" array. Each item should have "name" and "description" keys.
Example format: {{"fields": [{{"name": "field1", "description": "..."}}, {{"name": "field2", "description": "..."}}]}}"""
                }
            ],
            "format": "json",
            "stream": False,  # Explicitly disable streaming
            "options": {
                "temperature": 0.3
            }
        }

        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()

            # Handle potential streaming response or malformed JSON
            # Ollama may return multiple JSON objects even with stream=False
            response_text = response.text.strip()
            data = None

            try:
                data = response.json()
            except (ValueError, json.JSONDecodeError) as e:
                # If response.json() fails, it might be a streaming response with multiple JSON objects
                # Try to parse the last complete JSON object from the response text
                if response_text:
                    # Split by newlines and try to parse each line as JSON
                    # Ollama streaming format has one JSON object per line
                    lines = response_text.strip().split('\n')
                    for line in reversed(lines):  # Start from the last line
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            # Try to parse as JSON
                            parsed = json.loads(line)
                            # Check if it's a valid Ollama response structure
                            if isinstance(parsed, dict) and ('message' in parsed or 'content' in parsed or 'response' in parsed):
                                data = parsed
                                break
                        except json.JSONDecodeError:
                            continue

                    # If we still don't have data, try to extract JSON object with regex
                    if data is None:
                        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                        if json_match:
                            try:
                                data = json.loads(json_match.group(0))
                            except json.JSONDecodeError:
                                pass

                    if data is None:
                        raise AIAPIError(f"Failed to parse Ollama response: {str(e)}. Response: {response_text[:500]}") from e
                else:
                    raise AIAPIError(f"Empty response from Ollama: {str(e)}") from e

            content = data.get("message", {}).get("content", "")

            # Try to extract JSON from content if it contains markdown code blocks
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            else:
                # Try to find JSON object in the text
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)

            result = json.loads(content)

            field_dict = {}
            if "fields" in result:
                for field_info in result["fields"]:
                    if "name" in field_info and "description" in field_info:
                        field_dict[field_info["name"]] = field_info["description"]

            # Ensure all requested fields are in the result
            for field in fields:
                if field not in field_dict:
                    field_dict[field] = f"Field: {field}"

            return field_dict

        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"Ollama API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse Ollama response: {str(e)}. Content: {content[:200] if 'content' in locals() else 'N/A'}") from e

    def get_description(self, data: str, language: str = 'English') -> str:
        """Get dataset description using Ollama API."""
        url = f"{self.base_url}/api/chat"

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide concise dataset descriptions in {language}. Always respond with valid JSON only."
                },
                {
                    "role": "user",
                    "content": f"""I have the following CSV data sample:
{data}
Please provide a short description of this dataset in {language}. Consider this as a sample of a larger dataset. Don't generate code or data examples.
Return JSON with format: {{"description": "..."}}"""
                }
            ],
            "format": "json",
            "stream": False,  # Explicitly disable streaming
            "options": {
                "temperature": 0.3
            }
        }

        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()

            # Handle potential streaming response or malformed JSON
            # Ollama may return multiple JSON objects even with stream=False
            response_text = response.text.strip()
            data = None

            try:
                data = response.json()
            except (ValueError, json.JSONDecodeError) as e:
                # If response.json() fails, it might be a streaming response with multiple JSON objects
                # Try to parse the last complete JSON object from the response text
                if response_text:
                    # Split by newlines and try to parse each line as JSON
                    # Ollama streaming format has one JSON object per line
                    lines = response_text.strip().split('\n')
                    for line in reversed(lines):  # Start from the last line
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            # Try to parse as JSON
                            parsed = json.loads(line)
                            # Check if it's a valid Ollama response structure
                            if isinstance(parsed, dict) and ('message' in parsed or 'content' in parsed or 'response' in parsed):
                                data = parsed
                                break
                        except json.JSONDecodeError:
                            continue

                    # If we still don't have data, try to extract JSON object with regex
                    if data is None:
                        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                        if json_match:
                            try:
                                data = json.loads(json_match.group(0))
                            except json.JSONDecodeError:
                                pass

                    if data is None:
                        raise AIAPIError(f"Failed to parse Ollama response: {str(e)}. Response: {response_text[:500]}") from e
                else:
                    raise AIAPIError(f"Empty response from Ollama: {str(e)}") from e

            content = data.get("message", {}).get("content", "")

            # Try to extract JSON from content if it contains markdown code blocks
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            else:
                # Try to find JSON object in the text
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)

            # Try to parse as JSON
            try:
                result = json.loads(content)
                if "description" in result:
                    return result["description"]
                else:
                    # If JSON is valid but doesn't have description, return content as-is
                    return content
            except json.JSONDecodeError as json_err:
                # If JSON parsing fails, try to return the raw content
                # This handles cases where the model returns plain text instead of JSON
                if content:
                    return content
                raise AIAPIError(f"Failed to parse Ollama JSON response: {str(json_err)}. Content: {content[:200]}") from json_err

        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"Ollama API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            # This should not happen now, but keep as fallback
            try:
                content = data.get("message", {}).get("content", "")
                if content:
                    return content
            except (KeyError, IndexError, NameError):
                pass
            raise AIAPIError(f"Failed to extract description from Ollama response: {str(e)}") from e

    def get_structured_metadata(self, data: str, fields: list[str],
                                language: str = 'English') -> dict:
        """Get structured metadata using Ollama API."""
        MAX_DATA_LENGTH = 3000
        data = _truncate_sample(data, MAX_DATA_LENGTH)
        url = f"{self.base_url}/api/chat"

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        f"You are a data documentation assistant. "
                        f"Return structured dataset metadata in {language} as valid JSON only."
                    )
                },
                {
                    "role": "user",
                    "content": _metadata_prompt(data, fields, language)
                }
            ],
            "format": "json",
            "stream": False,
            "options": {
                "temperature": 0.2
            }
        }

        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()

            response_text = response.text.strip()
            payload_data = None

            try:
                payload_data = response.json()
            except (ValueError, json.JSONDecodeError) as e:
                if response_text:
                    lines = response_text.strip().split('\n')
                    for line in reversed(lines):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            parsed = json.loads(line)
                            if isinstance(parsed, dict) and (
                                'message' in parsed or 'content' in parsed or 'response' in parsed
                            ):
                                payload_data = parsed
                                break
                        except json.JSONDecodeError:
                            continue
                    if payload_data is None:
                        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                        if json_match:
                            try:
                                payload_data = json.loads(json_match.group(0))
                            except json.JSONDecodeError:
                                pass
                    if payload_data is None:
                        raise AIAPIError(
                            f"Failed to parse Ollama response: {str(e)}. Response: {response_text[:500]}"
                        ) from e
                else:
                    raise AIAPIError(f"Empty response from Ollama: {str(e)}") from e

            content = payload_data.get("message", {}).get("content", "")
            return _extract_json_payload(content)
        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"Ollama API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(
                f"Failed to parse Ollama response: {str(e)}. "
                f"Content: {content[:200] if 'content' in locals() else 'N/A'}"
            ) from e


class LMStudioProvider(AIService):
    """LM Studio local API provider (OpenAI-compatible)."""

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 model: Optional[str] = None, timeout: int = 30):
        """Initialize LM Studio provider.

        Args:
            api_key: Not used for LM Studio (kept for interface compatibility, can be "lm-studio")
            base_url: Base URL (defaults to http://localhost:1234/v1)
            model: Model name (REQUIRED - must match a model loaded in LM Studio)
            timeout: Request timeout in seconds
        """
        base_url = base_url or os.getenv('LMSTUDIO_BASE_URL', 'http://localhost:1234/v1')
        super().__init__(api_key, base_url, model, timeout)
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate LM Studio configuration."""
        if not self.model:
            raise AIConfigurationError(
                "Model name is required for LM Studio. "
                "Please specify a model name that matches a model loaded in LM Studio. "
                "Example: --ai-model 'your-model-name'"
            )

    def _get_available_models(self) -> list[str]:
        """Get list of available models from LM Studio."""
        try:
            models_url = f"{self.base_url}/models"
            response = requests.get(models_url, timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and 'data' in data:
                    return [model.get('id', '') for model in data['data'] if 'id' in model]
        except Exception:
            pass
        return []

    def get_fields_info(self, fields: list[str], language: str = 'English') -> dict[str, str]:
        """Get field descriptions using LM Studio API."""
        fields_str = ', '.join(fields)
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide clear, concise descriptions of data fields in {language}. Always respond with valid JSON only, no markdown, no code blocks."
                },
                {
                    "role": "user",
                    "content": f"""Please describe these data fields in {language}: {fields_str}.
Return ONLY a JSON object with a "fields" array. Each item must have "name" and "description" keys.
Format: {{"fields": [{{"name": "field1", "description": "..."}}, {{"name": "field2", "description": "..."}}]}}
Return only the JSON, nothing else."""
                }
            ],
            "temperature": 0.3
        }

        # Try with json_object format first (some models support it)
        # If that fails, fall back to text parsing
        try:
            payload["response_format"] = {"type": "json_object"}
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            # Remove response_format if not supported
            payload.pop("response_format", None)
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()

        try:
            data = response.json()
            content = data["choices"][0]["message"]["content"].strip()

            # Try to extract JSON from markdown code blocks if present
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            else:
                # Try to find JSON object in the text
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)

            result = json.loads(content)

            field_dict = {}
            if "fields" in result:
                for field_info in result["fields"]:
                    if "name" in field_info and "description" in field_info:
                        field_dict[field_info["name"]] = field_info["description"]

            # Ensure all requested fields are in the result
            for field in fields:
                if field not in field_dict:
                    field_dict[field] = f"Field: {field}"

            return field_dict

        except requests.exceptions.RequestException as e:
            available_models = self._get_available_models()
            error_msg = f"LM Studio API request failed: {str(e)}"
            if available_models:
                error_msg += f"\nAvailable models: {', '.join(available_models)}"
            elif e.response and e.response.status_code == 404:
                error_msg += "\nMake sure LM Studio server is running and a model is loaded."
            raise AIAPIError(error_msg,
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse LM Studio response: {str(e)}. Response content: {content[:200]}") from e

    def get_description(self, data: str, language: str = 'English') -> str:
        """Get dataset description using LM Studio API."""
        # Truncate data if too large
        MAX_DATA_LENGTH = 5000
        if len(data) > MAX_DATA_LENGTH:
            data = data[:MAX_DATA_LENGTH] + "\n... (truncated)"

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide concise dataset descriptions in {language}. Always respond with valid JSON only, no markdown, no code blocks."
                },
                {
                    "role": "user",
                    "content": f"""I have the following CSV data sample:
{data}
Please provide a short description of this dataset in {language}. Consider this as a sample of a larger dataset. Don't generate code or data examples.
Return ONLY a JSON object with format: {{"description": "..."}}
Return only the JSON, nothing else."""
                }
            ],
            "temperature": 0.3
        }

        # Try with json_object format first (some models support it)
        # If that fails, fall back to text parsing
        try:
            payload["response_format"] = {"type": "json_object"}
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            # Remove response_format if not supported
            payload.pop("response_format", None)
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()

        try:
            response_data = response.json()
            content = response_data["choices"][0]["message"]["content"].strip()

            # Try to extract JSON from markdown code blocks if present
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            else:
                # Try to find JSON object in the text
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)

            # Try to parse as JSON first
            try:
                result = json.loads(content)
                if "description" in result:
                    return result["description"]
            except json.JSONDecodeError:
                pass

            # If JSON parsing fails, return the content as-is (might be plain text description)
            return content

        except requests.exceptions.RequestException as e:
            available_models = self._get_available_models()
            error_msg = f"LM Studio API request failed: {str(e)}"
            if available_models:
                error_msg += f"\nAvailable models: {', '.join(available_models)}"
            elif e.response and e.response.status_code == 404:
                error_msg += "\nMake sure LM Studio server is running and a model is loaded."
            raise AIAPIError(error_msg,
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except (KeyError, IndexError) as e:
            raise AIAPIError(f"Failed to extract description from LM Studio response: {str(e)}") from e

    def get_structured_metadata(self, data: str, fields: list[str],
                                language: str = 'English') -> dict:
        """Get structured metadata using LM Studio API."""
        MAX_DATA_LENGTH = 5000
        data = _truncate_sample(data, MAX_DATA_LENGTH)

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json"
        }
        user_content = _metadata_prompt(data, fields, language)

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        f"You are a data documentation assistant. "
                        f"Return structured dataset metadata in {language} as valid JSON only."
                    )
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            "temperature": 0.2
        }

        try:
            payload["response_format"] = {"type": "json_object"}
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            payload.pop("response_format", None)
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()

        try:
            response_data = response.json()
            content = response_data["choices"][0]["message"]["content"].strip()
            return _extract_json_payload(content)
        except requests.exceptions.RequestException as e:
            available_models = self._get_available_models()
            error_msg = f"LM Studio API request failed: {str(e)}"
            if available_models:
                error_msg += f"\nAvailable models: {', '.join(available_models)}"
            elif e.response and e.response.status_code == 404:
                error_msg += "\nMake sure LM Studio server is running and a model is loaded."
            raise AIAPIError(error_msg,
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse LM Studio response: {str(e)}. Response content: {content[:200]}") from e


class PerplexityProvider(AIService):
    """Perplexity API provider with structured output."""

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 model: Optional[str] = None, timeout: int = 30):
        """Initialize Perplexity provider.

        Args:
            api_key: Perplexity API key (defaults to PERPLEXITY_API_KEY env var)
            base_url: Base URL (defaults to https://api.perplexity.ai)
            model: Model name (defaults to sonar)
            timeout: Request timeout in seconds
        """
        # Always prioritize PERPLEXITY_API_KEY environment variable
        perplexity_key = os.getenv('PERPLEXITY_API_KEY')
        if perplexity_key:
            # If PERPLEXITY_API_KEY is set, always use it (ignore passed api_key)
            api_key = perplexity_key
        # If PERPLEXITY_API_KEY is not set, use the passed api_key (which may be None)

        base_url = base_url or 'https://api.perplexity.ai'
        model = model or 'sonar'
        super().__init__(api_key, base_url, model, timeout)
        self._validate_config()

    def get_fields_info(self, fields: list[str], language: str = 'English') -> dict[str, str]:
        """Get field descriptions using Perplexity API."""
        fields_str = ', '.join(fields)
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide clear, concise descriptions of data fields in {language}. Always respond with valid JSON only, no markdown, no code blocks."
                },
                {
                    "role": "user",
                    "content": f"""Please describe these data fields in {language}: {fields_str}.
Return ONLY a JSON object with a "fields" array. Each item must have "name" and "description" keys.
Format: {{"fields": [{{"name": "field1", "description": "..."}}, {{"name": "field2", "description": "..."}}]}}
Return only the JSON, nothing else."""
                }
            ],
            "temperature": 0.3
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            content = data["choices"][0]["message"]["content"].strip()

            # Try to extract JSON from markdown code blocks if present
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            else:
                # Try to find JSON object in the text
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)

            result = json.loads(content)

            field_dict = {}
            if "fields" in result:
                for field_info in result["fields"]:
                    if "name" in field_info and "description" in field_info:
                        field_dict[field_info["name"]] = field_info["description"]

            # Ensure all requested fields are in the result
            for field in fields:
                if field not in field_dict:
                    field_dict[field] = f"Field: {field}"

            return field_dict

        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"Perplexity API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse Perplexity response: {str(e)}. Response content: {content[:200]}") from e

    def get_description(self, data: str, language: str = 'English') -> str:
        """Get dataset description using Perplexity API."""
        # Truncate data if too large (Perplexity has token limits)
        # Keep first ~5000 characters to ensure we stay within limits
        MAX_DATA_LENGTH = 5000
        if len(data) > MAX_DATA_LENGTH:
            data = data[:MAX_DATA_LENGTH] + "\n... (truncated)"

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": f"You are a data documentation assistant. Provide concise dataset descriptions in {language}. Always respond with valid JSON only, no markdown, no code blocks."
                },
                {
                    "role": "user",
                    "content": f"""I have the following CSV data sample:
{data}
Please provide a short description of this dataset in {language}. Consider this as a sample of a larger dataset. Don't generate code or data examples.
Return ONLY a JSON object with format: {{"description": "..."}}
Return only the JSON, nothing else."""
                }
            ],
            "temperature": 0.3
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            response_data = response.json()

            content = response_data["choices"][0]["message"]["content"].strip()

            # Try to extract JSON from markdown code blocks if present
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            else:
                # Try to find JSON object in the text
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)

            # Try to parse as JSON first
            try:
                result = json.loads(content)
                if "description" in result:
                    return result["description"]
            except json.JSONDecodeError:
                pass

            # If JSON parsing fails, return the content as-is (might be plain text description)
            return content

        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"Perplexity API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except (KeyError, IndexError) as e:
            raise AIAPIError(f"Failed to extract description from Perplexity response: {str(e)}") from e

    def get_structured_metadata(self, data: str, fields: list[str],
                                language: str = 'English') -> dict:
        """Get structured metadata using Perplexity API."""
        MAX_DATA_LENGTH = 5000
        data = _truncate_sample(data, MAX_DATA_LENGTH)
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        user_content = _metadata_prompt(data, fields, language)

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        f"You are a data documentation assistant. "
                        f"Return structured dataset metadata in {language} as valid JSON only, no markdown."
                    )
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            "temperature": 0.2
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            response_data = response.json()
            content = response_data["choices"][0]["message"]["content"].strip()
            return _extract_json_payload(content)
        except requests.exceptions.RequestException as e:
            raise AIAPIError(f"Perplexity API request failed: {str(e)}",
                           status_code=getattr(e.response, 'status_code', None),
                           response=getattr(e.response, 'text', None)) from e
        except json.JSONDecodeError as e:
            raise AIAPIError(f"Failed to parse Perplexity response: {str(e)}. Response content: {content[:200]}") from e
