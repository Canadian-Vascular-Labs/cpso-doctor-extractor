import json
import os
import re

input_path = "data/postal_temp.json"


def group_postal_codes(
    output_path: str = "grouped_postal_codes.json", verbose: bool = True
) -> None:
    """
    Groups Canadian postal codes by FSA (first 3 characters) and writes to a new JSON file.

    Args:
        input_path (str): Path to the input JSON file (must contain a top-level "postal_codes" list).
        output_path (str): Path to the output grouped JSON file.
        verbose (bool): If True, print skipped codes and file sizes.
    """
    with open(input_path, "r") as f:
        data = json.load(f)

    postal_codes = data.get("postal_codes", [])
    grouped_postal_codes = {}

    for code in postal_codes:
        code = code.strip().upper()

        # validate postal code format against REGEX: '^[A-Z]\d[A-Z] \d[A-Z]\d$'
        if re.match(r"^[A-Z]\d[A-Z] \d[A-Z]\d$", code):
            fsa = code[:3]
            ldu = code[4:]

            grouped_postal_codes.setdefault(fsa, []).append(ldu)
        elif verbose:
            print(f"Skipping invalid postal code: {code}")

    with open(output_path, "w") as f:
        json.dump(grouped_postal_codes, f)

    if verbose:
        print(f"✅ Grouped postal codes written to: {output_path}")
        print(f"📦 Size of original file: {os.path.getsize(input_path)} bytes")
        print(f"📦 Size of grouped file:  {os.path.getsize(output_path)} bytes")
