import json
from collections import defaultdict

def infer_schema(data, parent_key=""):
    """
    Infers the schema from a JSON object and returns a dictionary of fields with their types.
    """
    schema = {}
    for key, value in data.items():
        full_key = f"{parent_key}.{key}" if parent_key else key
        if isinstance(value, dict):
            schema[key] = {"type": "struct", "fields": infer_schema(value, full_key)}
        elif isinstance(value, list):
            if value:
                schema[key] = {"type": "array", "element": infer_schema(value[0])}
            else:
                schema[key] = {"type": "array", "element": "unknown"}
        else:
            schema[key] = {"type": infer_type(value)}
    return schema

def infer_type(value):
    """
    Maps Python types to Iceberg-compatible types.
    """
    if isinstance(value, int):
        return "long"
    elif isinstance(value, float):
        return "double"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, bool):
        return "boolean"
    elif value is None:
        return "null"
    else:
        return "string"  # Default type for unknown structures

def iceberg_schema(json_file):
    """
    Generates an Iceberg-compatible schema from a JSON file.
    """
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    if isinstance(data, dict):  # Single JSON object
        schema = infer_schema(data)
    elif isinstance(data, list):  # Array of JSON objects
        schema = infer_schema(data[0])
    else:
        raise ValueError("Unsupported JSON structure")
    
    return schema

# Replace 'your_file.json' with your actual JSON file
json_file = 'your_file.json'
schema = iceberg_schema(json_file)

# Output the Iceberg-compatible schema
import pprint
pprint.pprint(schema)
