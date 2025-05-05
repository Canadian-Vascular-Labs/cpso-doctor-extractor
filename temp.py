from requests.exceptions import ReadTimeout, RequestException
import json
import demjson3
import requests
from bs4 import BeautifulSoup
from data.generate_postal_codes_to_json import group_postal_codes
from rich.progress import Progress
import rich as r
import os
import re


total_bytes_stored = 0
extracted_doctors = set()
skipped_doctors = set()
num_doctors_extracted = 0

BASE_URL = "https://register.cpso.on.ca/Get-Search-Results/"


def send_request(url, postal_code):
    try:
        # print(f"Attempt {attempt + 1}/{max_retries} for {postal_code}")
        response = requests.get(url, timeout=1)

        if response.status_code == 200:
            print(
                f"Successfully received response for postal code: {postal_code}")

            raw = response.text
            cleaned = repair_broken_json(raw)

            data = demjson3.decode(cleaned)
            total_count = data.get("totalcount", 0)
            print("Total Count: ", data["totalcount"])
            if total_count is -1:
                r.print(
                    "[red]Need to search with refined paramaters -- too many results")
                return False  # + somehow pass value that is used for advanced search

        else:
            print(
                f"Failed with status {response.status_code} for {postal_code}")
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON decode error for FSA {fsa}: {e}")
        print(f"Raw snippet around error (char {e.pos}):")
        # Show 100 chars before/after
        print(cleaned[e.pos - 100:e.pos + 100])
        return False
    except ReadTimeout:
        return False  # + Retry on timeout


def repair_broken_json(raw: str) -> str:
    # Remove control characters
    raw = re.sub(r'[\x00-\x1F\x7F]', '', raw)

    # Fix common bad escape sequences (e.g., unescaped \)
    raw = raw.replace('\\"', '"')
    raw = re.sub(r'(?<!\\)\\(?![\\nt"r/])', r'\\\\', raw)

    # Add missing commas between fields
    raw = re.sub(r'(":[^,}\]]+)(\s*)"(\w+)":', r'\1,\2"\3":', raw)

    # Fix double commas or commas after brace/bracket
    raw = re.sub(r',\s*,+', ',', raw)               # remove duplicate commas
    # opening brace followed by comma
    raw = re.sub(r'{\s*,', '{', raw)
    # opening bracket followed by comma
    raw = re.sub(r'\[\s*,', '[', raw)
    # trailing comma before close brace
    raw = re.sub(r',\s*}', '}', raw)
    # trailing comma before close bracket
    raw = re.sub(r',\s*]', ']', raw)

    return raw


def append_data_to_file(file_path, data, postal_code):
    global num_doctors_extracted
    with open(file_path, 'r+') as f:
        existing_data = json.load(f)

        if postal_code not in existing_data:
            existing_data[postal_code] = {
                "totalcount": 0,
                "results": []
            }

        # Loop through each doctor in the result
        for doctor in data.get("results", []):
            cpso = doctor.get("cpsonumber")

            # check if the doctor is already in the seen set, or if the postal code is not the same
            raw_postal_code = doctor.get("postalcode")
            if not raw_postal_code:
                print(
                    f"⚠️ Skipping doctor with missing postal code: CPSO# {doctor.get('cpsonumber')}")
                skipped_doctors.add(doctor.get("cpsonumber"))
                continue

            doctor_postal_code = raw_postal_code.strip().upper()

            if cpso in extracted_doctors or doctor_postal_code != postal_code:
                if (doctor_postal_code != postal_code):
                    # print(
                    # f"⚠️ Skipping doctor with different postal code: {doctor_postal_code} != {postal_code} for CPSO#: {cpso}")
                    skipped_doctors.add(cpso)
                else:
                    print(f"⚠️ Skipping duplicate CPSO#: {cpso}")

                continue
            # check if we can remove the doctor from the skipped_doctors set
            elif cpso in skipped_doctors:
                # print(
                # f"✅ Adding doctor back to the seen set: {cpso} for postal code: {postal_code}")
                skipped_doctors.remove(cpso)

            # Add to seen set
            extracted_doctors.add(cpso)
            num_doctors_extracted += 1

            # Append to results
            existing_data[postal_code]["results"].append(doctor)
            existing_data[postal_code]["totalcount"] += 1
        f.seek(0)
        json.dump(existing_data, f, indent=4)


def extract_doctors(fsa, ldu, doctorType=None, specialty=None):
    postal_code = f"{fsa} {ldu}"
    # global failed_postal_codes
    global failed_postal_codes
    FSA = postal_code[:3]
    url = f"{BASE_URL}?cbx-includeinactive=on&postalCode={postal_code}"

    # check if doctorType is not None to add it to the URL
    if doctorType is not None:
        url += f"&doctorType={doctorType}"
    # check if specialty is not None to add it to the URL
    if specialty is not None:
        url += f"&specialty={specialty}"

    max_retries = 10
    for attempt in range(max_retries):
        try:
            # print(f"Attempt {attempt + 1}/{max_retries} for {postal_code}")
            response = requests.get(url, timeout=1)
            if response.status_code == 200:
                print(
                    f"Successfully fetched data for postal code: {postal_code}")

                raw = response.text
                cleaned = repair_broken_json(raw)

                data = demjson3.decode(cleaned)
                total_count = data.get("totalcount", 0)
                print("Total Count: ", data["totalcount"])
                if total_count is -1:
                    r.print(
                        "[red]Need to search with refined paramaters -- too many results")
                    return False or extract_doctors(fsa, ldu, doctorType="Family Doctor") or extract_doctors(fsa, ldu, specialty="Specialist")

                    # SEARCH BY Family Doctor
                    # WHAT IF MORE THAN 100 FAMILY DOCTORS?
                    # THEN DO Specialty Advanced Search

                    pass
                # only append if totalcount is greater than 0
                if total_count > 0:
                    append_data_to_file(
                        f"extracted_data/{FSA}.json", data, postal_code)
                return True
            else:
                print(
                    f"Failed with status {response.status_code} for {postal_code}")
        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON decode error for FSA {fsa}: {e}")
            print(f"Raw snippet around error (char {e.pos}):")
            # Show 100 chars before/after
            print(cleaned[e.pos - 100:e.pos + 100])
            return False
        except ReadTimeout:
            continue  # Retry on timeout
            print(
                f"⏱ Timeout for {postal_code}, attempt {attempt + 1}/{max_retries}")

    print(f"❌ Failed all attempts for {postal_code}")
    return False


if __name__ == "__main__":
    r.print("[red]Beginning extraction of doctors from postal codes...")

    failed_postal_codes = {}

    postal_json_output_path = "data/grouped_postal_codes.json"
    # group_postal_codes(postal_json_output_path)

    # read grouped_postal_codes.json
    with open(postal_json_output_path, 'r') as f:
        data = json.load(f)

    with Progress(transient=True) as progress:
        fsa_task = progress.add_task(
            "[cyan]Processing FSAs:", total=len(data.keys()))
        count = 0
        for fsa in data.keys():

            # FSA: Forward Sortation Area (first 3 characters of postal code)
            # print(f"fsa: {fsa}")

            # create fresh FSA file:
            with open(f"extracted_data/{fsa}.json", 'w') as f:
                # create a dictionary to hold the data
                f.write(json.dumps({}, indent=4))

            # extract the list of LDU (Last Digit of the Postal Code) from the dictionary
            LDUs = data[fsa]
            failure_in_fsa = False
            # update the progress bar
            progress.update(
                fsa_task, description=f"[cyan]Processing FSA: {fsa}", advance=1)

            for ldu in LDUs:

                isSuccess = extract_doctors(fsa, ldu)
                if not isSuccess:
                    # if the function returns False, add the postal code to the failed_postal_codes dictionary to retry later
                    if fsa not in failed_postal_codes:
                        failed_postal_codes[fsa] = []
                    failed_postal_codes[fsa].append(ldu)
                    print(f"Added {fsa}-{ldu} to failed_postal_codes")
                    failure_in_fsa = True

            total_bytes_stored += os.path.getsize(f"extracted_data/{fsa}.json")
            r.print(
                f"[red]Completed FSA: {fsa}, failed codes: {len(failed_postal_codes)}, total bytes stored: {total_bytes_stored} bytes")

            # if (count > 0):
            #     break
            count += 1

            if not failure_in_fsa:
                continue

            print("Retrying failed postal codes...")
            # while (len(failed_postal_codes[fsa]) > 0):
            #     print(f"Retrying failed postal codes: {failed_postal_codes}")
            #     for ldu in failed_postal_codes[fsa]:
            #         isSuccess = extract_doctors(fsa, ldu)
            #         if (isSuccess):
            #             # if the function returns True, remove the postal code from the failed_postal_codes dictionary
            #             failed_postal_codes[fsa].remove(ldu)
            #             print(f"Removed {fsa}-{ldu} from failed_postal_codes")

            # remove the FSA from the failed_postal_codes dictionary if all LDUs are successful
            if len(failed_postal_codes[fsa]) == 0:
                del failed_postal_codes[fsa]
                print(f"Removed {fsa} from failed_postal_codes")

    # print skipped doctors
    if len(skipped_doctors) > 0:
        print(f"Skipped doctors ({len(skipped_doctors)}): {skipped_doctors}")
        with open("skipped_doctors.txt", "w") as f:
            for doctor in skipped_doctors:
                f.write(f"{doctor}\n")
        skipped_doctors.clear()

    print(f"Total doctors extracted: {num_doctors_extracted}")
    # M2J 1V1
