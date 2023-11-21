import json
import logging

from jsonschema import Draft7Validator

# Schema definitions
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
            "type": ["string", "null"],
        },
    },
    "required": ["name", "age", "address"],
}

# Validator instances
validator = Draft7Validator(schema)
nested_validator = Draft7Validator(nested_schema)

# Set up the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def pre_validate_for_string_type(record: dict, schema: dict) -> dict:
    for key, value in record.items():
        expected_type = schema.get("properties", {}).get(key, {}).get("type")
        if "string" in expected_type and not isinstance(value, str):
            record[key] = (
                json.dumps(value)
                if isinstance(value, (dict, list)) else str(value)
            )
            if logger:
                logger.debug(f"Converted field {key} to string: {record[key]}")
    return record

# Test cases
def test_validation_string_conversion():
    record = {"name": 123, "age": 30}
    pre_validated_record = pre_validate_for_string_type(record, schema)
    validator.validate(pre_validated_record)  # This should not raise an error
    assert (
        pre_validated_record["name"] == "123"
    ), "The 'name' should have been converted to a string."

def test_validation_no_error_raised():
    record = {"name": "John", "age": 30}
    validator.validate(record)  # This should not raise an error

def test_validation_null_allowed():
    record = {"name": None, "age": 30}
    validator.validate(record)  # This should not raise an error

def test_nested_dict_string_conversion():
    record = {
        "name": "John", "age": 30,
        "address": {"street": 123, "city": "New York"},
    }
    pre_validated_record = pre_validate_for_string_type(record, nested_schema)
    validator.validate(pre_validated_record)  # This should not raise an error
    assert (
        "street" in json.loads(pre_validated_record["address"])
    ), "The 'address' should have been converted to a JSON string."

def test_nested_dict_with_nested_non_string():
    record = {
        "name": "John", "age": 30,
        "address": {"street": "Main", "city": {"name": "New York"}},
    }
    pre_validated_record = pre_validate_for_string_type(record, nested_schema)
    validator.validate(pre_validated_record)  # This should not raise an error
    assert (
        "city" in json.loads(pre_validated_record["address"])
    ), "The 'city' should have been converted to a JSON string."

def test_single_level_schema_nested_dict_to_string():
    record = {"name": {"first": "John", "last": "Doe"}, "age": 30, "address": None}
    pre_validated_record = pre_validate_for_string_type(record, nested_schema)
    nested_validator.validate(pre_validated_record)  # This should not raise an error
    assert (
        json.loads(pre_validated_record["name"]) == {"first": "John", "last": "Doe"}
    ), "The JSON string is not correct."

def test_single_level_schema_deeply_nested_list_of_dicts_to_string():
    record = {
        "name": "John",
        "age": 30,
        "address": [
            {"street": "Main", "city": {"name": "New York"}},
            {"street": "Second", "city": {"name": "Los Angeles"}},
        ],
    }
    pre_validated_record = pre_validate_for_string_type(record, nested_schema)
    nested_validator.validate(pre_validated_record)  # This should not raise an error
    address_list = json.loads(pre_validated_record["address"])
    assert (
        all("street" in addr for addr in address_list)
    ), "The JSON string does not correctly represent the nested list of dicts."

def test_multiple_fields_conversion():
    # Test record with multiple fields needing conversion
    record = {
        "name": {"first": "John", "last": "Doe"},  # Expected to be a string
        "age": 30,
        "address": {"street": "Main", "city": {"name": "New York"}},
    }
    pre_validated_record = pre_validate_for_string_type(record, nested_schema)
    nested_validator.validate(pre_validated_record)  # This should not raise an error

    # Asserting the conversions
    assert (
        isinstance(pre_validated_record["name"], str)
    ), "The 'name' should have been converted to a JSON string."
    assert (
        isinstance(pre_validated_record["address"], str,
    )), "The 'address' should have been converted to a JSON string."
    assert (
        json.loads(pre_validated_record["name"]) == {"first": "John", "last": "Doe"}
    ), "The JSON string for 'name' is not correct."
    assert (
        "street" in json.loads(pre_validated_record["address"])
    ), "The JSON string for 'address' does not correctly represent the nested dict."
