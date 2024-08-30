import re
import json
from pathlib import Path


def rationalise_property_bag(data: str) -> dict[str]:
    data = data.splitlines()

    pattern = re.compile(r"([\w\s\(\)]+):(\w+)\|(.+)")
    matches = [match for line in data for match in pattern.findall(line)]

    metadata = {}

    for match in matches:
        key, data_type, value = match
        metadata[key] = value

    desired_metadata = [
        "Name",
        "Last modified0",
        "First issued",
        "Number",
        "_Author",
        "Expert affiliation",
        "Technical area",
        "Field",
        "Kind",
        "Contents (1)",
        "Contents (2)",
        "Contents (3)",
        "Contents (4)",
        "Project ID",
        "First author",
        "Affiliation",
        "Modifications (1)",
        "Modifications (2)",
        "Modifications (3)",
        "Modifications (4)",
    ]

    rationalised_metadata = {}
    contents_values = []
    contents_keys = [
        "Contents (1)",
        "Contents (2)",
        "Contents (3)",
        "Contents (4)",
    ]
    modifications_values = []
    modifications_keys = [
        "Modifications (1)",
        "Modifications (2)",
        "Modifications (3)",
        "Modifications (4)",
    ]

    for key in desired_metadata:
        if key in metadata:
            if key in contents_keys:
                contents_values.append(metadata[key])
            elif key in modifications_keys:
                modifications_values.append(metadata[key])
            else:
                rationalised_metadata[key] = metadata[key]

    rationalised_metadata["Contents"] = "\n".join(contents_values)
    rationalised_metadata["Modifications"] = "\n".join(modifications_values)
    return rationalised_metadata


cwd = Path().cwd()
with open(cwd / "deliverables.json", "r") as f:
    metadata_list = json.load(f)

for file, metadata in enumerate(metadata_list):
    property_bag_dict = rationalise_property_bag(metadata["Property Bag"])
    metadata.update(property_bag_dict)
    print(metadata)

print(metadata)
