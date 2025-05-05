import json
import os

with open('postal_codes.json', 'r') as f:
    data = json.load(f)

postal_codes = data['postal_codes']
grouped_postal_codes = {}

for code in postal_codes:
    code = code.strip().upper()

    if len(code) == 7 and code[3] == ' ' and code[:3].isalnum() and code[4:].isalnum():
        fsa = code[:3]
        ldu = code[4:]

        if fsa not in grouped_postal_codes:
            grouped_postal_codes[fsa] = []

        grouped_postal_codes[fsa].append(ldu)
    else:
        print(f"Skipping invalid postal code: {code}")


# create a new JSON file with the grouped postal codes
with open('grouped_postal_codes.json', 'w') as f:
    json.dump(grouped_postal_codes, f)  # no indent



file_path = "grouped_postal_codes.json"

# print size of both files
print(f"Size of original file: {os.path.getsize('postal_codes.json')} bytes")
print(f"Size of grouped file: {os.path.getsize(file_path)} bytes")

