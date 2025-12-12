"""JSON Schema definitions for structured AI output."""

# Schema for field descriptions response
FIELD_INFO_SCHEMA = {
    "type": "object",
    "properties": {
        "fields": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The name of the field"
                    },
                    "description": {
                        "type": "string",
                        "description": "A clear, concise description of what this field represents"
                    }
                },
                "required": ["name", "description"],
                "additionalProperties": False
            }
        }
    },
    "required": ["fields"],
    "additionalProperties": False
}

# Schema for dataset description (simple string response)
# For providers that support JSON Schema, we can use a simple object wrapper
DATASET_DESCRIPTION_SCHEMA = {
    "type": "object",
    "properties": {
        "description": {
            "type": "string",
            "description": "A concise description of the dataset"
        }
    },
    "required": ["description"],
    "additionalProperties": False
}
