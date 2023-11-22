import logging

from jsonschema import Draft7Validator

from target_clickhouse.sinks import pre_validate_for_string_type

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
        "age": {"type": "string"},
        "address": {
            "type": "object",
            "properties": {
                "street": {"type": ["string", "null"]},
                "city": {"type": "string"},
                "state": {"type": "string"},
                "zip": {"type": "string"},
            },
            "required": ["street", "city", "state", "zip"],
        },
    },
    "required": ["name", "age", "address"],
}

list_schema = {
    "type": "object",
    "properties": {
        "name": {"type": ["string", "null"]},
        "age": {"type": "string"},
        "address": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "street": {"type": ["string", "null"]},
                    "city": {"type": "string"},
                    "state": {"type": "string"},
                    "zip": {"type": "string"},
                },
                "required": ["street", "city", "state", "zip"],
            },
        },
    },
    "required": ["name", "age", "address"],
}

# Validator instances
validator = Draft7Validator(schema)
nested_validator = Draft7Validator(nested_schema)
list_validator = Draft7Validator(list_schema)

# Set up the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        "address": {"street": 123, "city": "New York", "state": "NY", "zip": "10001"},
    }
    pre_validated_record = pre_validate_for_string_type(record, nested_schema)
    nested_validator.validate(pre_validated_record)  # This should not raise an error
    assert (
        pre_validated_record["age"] == "30"
    ), "The 'age' should have been converted to a string."

def test_single_level_schema_deeply_nested_list_of_dicts_to_string():
    record = {
        "name": "John",
        "age": 30,
        "address": {
            "street": "Main",
            "city": {"name": "New York"},
            "state": "NY",
            "zip": ["10001", "10002"],
        },
    }
    pre_validated_record = pre_validate_for_string_type(record, nested_schema)
    nested_validator.validate(pre_validated_record)  # This should not raise an error
    assert (
        isinstance(pre_validated_record["address"]["zip"], str)
    ), "The JSON string does not represent a str."

def test_multiple_fields_conversion():
    # Test record with multiple fields needing conversion
    record = {
        "name": {"first": "John", "last": "Doe"},  # Expected to be a string
        "age": 30,
        "address": {"street": "Main",
                    "city": {"name": "New York"}, "state": "NY", "zip": 10001},
    }
    pre_validated_record = pre_validate_for_string_type(record, nested_schema)
    nested_validator.validate(pre_validated_record)  # This should not raise an error

    # Asserting the conversions
    assert (
        isinstance(pre_validated_record["address"]["city"], str)
    ), "The 'city' should have been converted to a string."
    assert (
        isinstance(pre_validated_record["address"]["zip"], str,
    )), "The 'zip' should have been converted to a string."


def test_list_of_dicts_conversion():
    # Test record with list of dicts
    record = {
        "name": "John",
        "age": 30,
        "address": [
            {"street": "Main", "city": {"name": "New York"},
             "state": "NY", "zip": 10001},
            {"street": "Main", "city": {"name": "New York"},
             "state": "NY", "zip": 10002},
        ],
    }
    pre_validated_record = pre_validate_for_string_type(record, list_schema)
    list_validator.validate(pre_validated_record)  # This should not raise an error

    # Asserting the conversions
    assert (
        isinstance(pre_validated_record["address"][0]["zip"], str)
    ), "The 'zip' should have been converted to a string."
    assert (
        isinstance(pre_validated_record["address"][1]["zip"], str)
    ), "The 'zip' should have been converted to a string."
