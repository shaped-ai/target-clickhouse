import json
import logging

from jsonschema import Draft7Validator, ValidationError

from target_clickhouse.sinks import handle_validation_error

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
            "type": ["string", "null"],
        },
    },
    "required": ["name", "age", "address"],
}


# Validator instance
validator = Draft7Validator(schema)

nested_validator = Draft7Validator(nested_schema)

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
        nested_validator.validate(record)
    except ValidationError as e:
        updated_record = handle_validation_error(record, e, logger)
        assert (
            isinstance(updated_record["name"], str)
        ), "The 'name' should have been converted to a JSON string."
        assert (
            json.loads(updated_record["name"]) == {"first": "John", "last": "Doe"}
        ), "The JSON string is not correct."

def test_single_level_schema_deeply_nested_dict_to_string():
    record = {
        "name": "John",
        "age": 30,
        "address": {"street": "Main", "city": {"name": "New York"}},
    }
    try:
        nested_validator.validate(record)
    except ValidationError as e:
        updated_record = handle_validation_error(record, e, logger)
        assert (
            isinstance(updated_record["address"], str)
        ), "The 'address' field should have been converted to a JSON string."
        assert (
            "street" in json.loads(updated_record["address"])
        ), "The JSON string does not correctly represent the nested dict."

def test_single_level_schema_deeply_nested_list_of_dicts_to_string():
    record = {
        "name": "John",
        "age": 30,
        "address": [
            {"street": "Main", "city": {"name": "New York"}},
            {"street": "Second", "city": {"name": "Los Angeles"}},
        ],
    }
    address_str = json.dumps(record["address"])
    try:
        nested_validator.validate(record)
    except ValidationError as e:
        updated_record = handle_validation_error(record, e, logger)
        assert (
            isinstance(updated_record["address"], str)
        ), "The 'address' field should have been converted to a JSON string."
        assert (
            updated_record["address"] == address_str
        ), "The JSON string does not correctly represent the nested list of dicts."
