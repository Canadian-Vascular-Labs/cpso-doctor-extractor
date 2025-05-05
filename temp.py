from requests.exceptions import ReadTimeout, RequestException
import json
import requests
from bs4 import BeautifulSoup
from time import sleep


BASE_URL = "https://register.cpso.on.ca/Get-Search-Results/"
# read grouped_postal_codes.json

with open('data/grouped_postal_codes.json', 'r') as f:
    data = json.load(f)


def extract_doctors(postal_code):
    url = f"{BASE_URL}?cbx-includeinactive=on&postalCode={postal_code}"

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                print(
                    f"Successfully fetched data for postal code: {postal_code}")
                data = response.json()
                print("Total Count: ", data["totalcount"])

                FSA = postal_code[:3]
                with open(f"extracted_data/{FSA}.json", 'r+') as f:
                    existing_data = json.load(f)
                    existing_data[postal_code] = data
                    f.seek(0)
                    json.dump(existing_data, f, indent=4)
                return
            else:
                print(
                    f"Failed with status {response.status_code} for {postal_code}")
                return
        except ReadTimeout:
            print(
                f"⏱ Timeout for {postal_code}, attempt {attempt + 1}/{max_retries}")
        except RequestException as e:
            print(f"⚠️ Error for {postal_code}: {e}")
            return

    print(f"❌ Failed all attempts for {postal_code}")


for fsa in data.keys():
    # FSA: Forward Sortation Area (first 3 characters of postal code)
    print(f"fsa: {fsa}")

    # create fresh FSA file:
    with open(f"extracted_data/{fsa}.json", 'w') as f:
        # create a dictionary to hold the data
        f.write(json.dumps({}, indent=4))

    # extract the list of LDU (Last Digit of the Postal Code) from the dictionary
    LDUs = data[fsa]
    # iterate through the list of LDUs
    for ldu in LDUs:
        # construct the postal code
        postal_code = f"{fsa} {ldu}"
        print(f"postal_code: {postal_code}")

        # call the function to get doctor info
        extract_doctors(postal_code)

    exit(1)

# M2J 1V1
