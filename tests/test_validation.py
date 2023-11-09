import json
import logging
from typing import Optional

from jsonschema import Draft7Validator, ValidationError

# Schema that allows a field to be either a string or null
schema = {
    "type": "object",
    "properties": {
        "name": {"type": ["string", "null"]},
        "age": {"type": "number"},
    },
    "required": ["name", "age"],
}

nested_schema = {
    "type": "object",
    "properties": {
        "name": {"type": ["string", "null"]},
        "age": {"type": "number"},
        "address": {
            "type": "object",
            "properties": {
                "street": {"type": "string"},
                "city": {"type": "string"},
            },
            "required": ["street", "city"],
        },
    },
    "required": ["name", "age", "address"],
}


# Validator instance
validator = Draft7Validator(schema)

# Function to handle validation errors
def handle_validation_error(record,
                            e: ValidationError,
                            logger: Optional[logging.Logger] = None):
    if "'string'" in e.message:
        if logger:
            logger.warning(
                f"Received non valid record for types 'string', {e.path}, "
                f"attempting conversion for record, {record}",
            )


        key_path = list(e.path)

        # Access the problematic value using the key_path
        current_level = record
        for key in key_path[:-1]:  # Go to parent of the problematic key
            current_level = current_level[key]

        problem_key = key_path[-1]
        problem_value = current_level[problem_key]

        # Convert the problematic value to string only if it's not null.
        if problem_value is not None:
            current_level[problem_key] = str(problem_value)
            if logger:
                logger.warning("Validating converted record")
            return record
        return None
    return None

# Set up the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test cases
def test_validation_string_conversion():
    record = {"name": 123, "age": 30}
    try:
        validator.validate(record)
    except ValidationError as e:
        updated_record = handle_validation_error(record, e, logger)
        assert (
            updated_record["name"] == "123"
        ), "The 'name' should have been converted to a string."
        validator.validate(updated_record)  # This should not raise an error

def test_validation_no_error_raised():
    record = {"name": "John", "age": 30}
    # This should not raise an error, hence no need to handle validation
    validator.validate(record)  # This should not raise an error

def test_validation_null_allowed():
    record = {"name": None, "age": 30}
    try:
        validator.validate(record)
    except ValidationError as e:
        updated_record = handle_validation_error(record, e, logger)
        assert (
            updated_record is None
        ), "The 'name' field is null and should be valid."

def test_validation_non_string_non_null_field():
    record = {"name": {"first": "John", "last": "Doe"}, "age": 30}
    try:
        validator.validate(record)
    except ValidationError as e:
        updated_record = handle_validation_error(record, e, logger)
        assert (
            isinstance(updated_record["name"], str)
        ), "The 'name' should have been converted to a string."

def test_nested_dict_string_conversion():
    record = {"name": "John", "age": 30, "address": {"street": 123, "city": "New York"}}
    try:
        validator.validate(record)
    except ValidationError as e:
        updated_record = handle_validation_error(record, e, logger)
        assert (
            updated_record["address"]["street"] == "123"
        ), "The 'street' should have been converted to a string."
        validator.validate(updated_record)  # This should not raise an error

def test_nested_dict_with_nested_non_string():
    record = {"name": "John", "age": 30,
              "address": {"street": "Main", "city": {"name": "New York"}}}
    try:
        validator.validate(record)
    except ValidationError as e:
        updated_record = handle_validation_error(record, e, logger)
        assert (
            isinstance(updated_record["address"]["city"], str)
        ), "The 'city' should have been converted to a string."
        validator.validate(updated_record)  # This should not raise an error

    def test_single_level_schema_nested_dict_to_string():
        record = {"name": {"first": "John", "last": "Doe"}, "age": 30}
        try:
            validator.validate(record)
        except ValidationError as e:
            updated_record = handle_validation_error(record, e, logger)
            assert (
                isinstance(updated_record["name"], str)
            ), "The 'name' should have been converted to a JSON string."
            assert (
                json.loads(updated_record["name"]) == {"first": "John", "last": "Doe"}
            ), "The JSON string is not correct."

    def test_single_level_schema_deeply_nested_dict_to_string():
        record = {"name":
                  {"first": "John", "last": "Doe",
                   "nicknames": {"short": "JD", "long": "Johnny"},
                   },
                   "age": 30,
                }
        try:
            validator.validate(record)
        except ValidationError as e:
            updated_record = handle_validation_error(record, e, logger)
            assert (
                isinstance(updated_record["name"], str)
            ), "The 'name' field should have been converted to a JSON string."
            assert (
                "nicknames" in json.loads(updated_record["name"])
            ), "The JSON string does not correctly represent the nested dict."
