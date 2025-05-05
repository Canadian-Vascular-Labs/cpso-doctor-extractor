import requests
# from bs4 import BeautifulSoup
import pandas as pd
import time
import json
# from tqdm import tqdm
import os

# Base URL for CPSO doctor lookup
BASE_URL = "https://register.cpso.on.ca/Get-Search-Results/"
postal_codes_path = 'data/postal.xlsx'

# start_cpso = 80000
start_cpso = 80010
# end_cpso = start_cpso + 10
end_cpso = start_cpso
file_name = f"doctors_info_{start_cpso}-{end_cpso}.csv"
safety_save = 2  # save every 2 doctors


def extract_status_info(soup):
    member_status = soup.find("div", {"class": "col-5 status-section"})
    if (member_status is None):
        print("Status not available")
        raise AttributeError
    spans = member_status.find_all("span")

    status = spans[1].text.strip()
    if (status != "Active"):
        raise AttributeError

    registration_class = spans[4].text.strip()

    # print("Registration Class: ", registration_class)
    return status, registration_class


def extract_name(soup):
    name = soup.find(
        "h1", {"class": "doctor-name scrp-contactname-value"}).text.strip()

    last_name, first_name = name.split(", ")
    # print(f'Name: L-{last_name}, F-{first_name}')
    return last_name, first_name


@staticmethod
def extract_address_info(lines):
    practice_info = {}
    if (lines[0] == "Address not Available"):
        print("Address not available")
        raise AttributeError

    addrs = 0

    for (idx, line) in enumerate(lines):
        # City, Province, Postal Code

        tokens = line.split(" ")
        # print("tokens: ", tokens)

        if "Ontario" in line or (len(tokens) > 3 and len(tokens[-1]) == 3 and len(tokens[-2]) == 3 and len(tokens[-3]) == 2):
            while (addrs < 4):
                addrs += 1
                practice_info['Address' + str(addrs)] = None

            city_province_postal = list(filter(None, line.split(" ")))
            practice_info["City"] = None
            practice_info["Province"] = None
            practice_info["Postal Code"] = None
            if (len(city_province_postal) >= 4):
                city = " ".join(city_province_postal[0:-3])
                province = city_province_postal[-3]
                postal_code = city_province_postal[-2] + \
                    " " + city_province_postal[-1]
                if (city is None):
                    print(
                        f"City, Province, Postal Code not available, received: {line}")
                    raise AttributeError
                practice_info['City'] = city
                practice_info['Province'] = "Ontario" if province == "ON" else province
                practice_info['Postal Code'] = postal_code
            break

        addrs += 1
        practice_info['Address' + str(addrs)] = line

    idx += 1

    if (len(lines) > idx):
        if (lines[idx] == "Business Email:"):
            practice_info['Email'] = lines[idx + 1]  # email
            idx += 2
        else:
            practice_info['Email'] = None

        if (lines[idx] == "Phone:"):
            practice_info['Phone'] = lines[idx + 1]  # phone
            idx += 2
        else:
            practice_info['Phone'] = None

        if (lines[idx] == "Extension:"):
            practice_info['Ext'] = lines[idx + 1]
            idx += 2
        else:
            practice_info['Ext'] = None

        if (lines[idx] == "Fax:"):
            practice_info['Fax'] = lines[idx + 1]
            idx += 2
        else:
            practice_info['Fax'] = None

    return practice_info, lines[idx:]


def extract_practice_information(soup):
    practice_tag = soup.find("div", {"id": "practice-information"})

    lines = [line.strip()
             for line in practice_tag.stripped_strings][2:]

    practice_info_dict, lines = extract_address_info(lines)

    # print("processesd lines: ", practice_info_dict)
    # print("remaining lines: ", lines)
    lines = lines[1:]

    # EXTRACT ADDITIONAL ADDRESSES
    # adhoc_num = 1
    # while (len(lines) > 0 and lines[0] == "Address:"):
    #     addr_dict, lines = extract_address_info(lines[1:])
    #     for key in addr_dict:
    #         practice_info_dict[f'AdHoc{adhoc_num}{key}'] = addr_dict[key]
    #     # print("Additional Address: ", addr_dict)
    #     adhoc_num += 1

    return practice_info_dict


def extract_specialties(soup):  # helper function to extract doctor specialties
    specialties = []
   # <div class="scrp-specialtyname-value"
    specialties_tag = soup.find_all(
        "div", {"class": "scrp-specialtyname-value"})  # repeats data 8x
    num_specialties = len(specialties_tag) / 4
    for i in range(int(num_specialties)):
        specialty = specialties_tag[i].text.strip()
        specialties.append(specialty)
        #   "No Specialty Reported" = "Family Medicine"?
    if (len(specialties) == 0):
        # specialties.append("Family Medicine")
        specialties.append("No Specialty Reported")
    # print(f'Specialties: {specialties}')

    return specialties


def get_doctor_info(postal_code):  # helper function to extract doctor info
    # convert postal code 'XXX XXX' to 'XXX+XXX' for URL
    # postal_code = postal_code.replace(" ", "+")

    # postal_code = 'M2K 1E1' # no doctors: -1
    # https://register.cpso.on.ca/Get-Search-Results/?cbx-includeinactive=on&postalCode=M1J+2E4
    url = f"{BASE_URL}?cbx-includeinactive=on&postalCode={postal_code}"
    response = requests.get(url, timeout=10)  # 10 seconds max per request

    if response.status_code != 200:
        print(f"Failed to fetch data for postal code: {postal_code}")
        return None
    else:
        print(f"Successfully fetched data for postal code: {postal_code}")

    data = response.json()

    # soup = BeautifulSoup(response.text, 'html.parser')
    print("data: ", data)
    # check totalcount field for number of doctors
    total_count = data["totalcount"]
    print("Total Count: ", total_count)
    exit()
    soup = None

    # status = extract_status_info(soup)
    # # print("Status: ", status[1])
    # hasConcerns = True
    # if ("Independent Practice" in status[1]):
    #     hasConcerns = False

    # dict = {}
    # dict['CPSO Number'] = cpso_number
    # last_name, first_name = extract_name(soup)
    # dict['LName'] = last_name
    # dict['FName'] = first_name
    # specialties = extract_specialties(soup)
    # dict['Specialties'] = specialties

    practice_info_dict = extract_practice_information(soup)
    for key in practice_info_dict:
        dict[key] = practice_info_dict[key]

    dict['Concerns'] = status[1] if hasConcerns else None

    return dict


def move_cursor(row: int, col: int):
    """Move the terminal cursor to a specific row and column using ANSI escape codes."""
    print(f"\033[{row};{col}H", end="", flush=True)
    pass


def move_to_progress_bar():
    move_cursor(2, 0)


def move_to_status():
    move_cursor(3, 0)


def clear_line():
    print("\033[K", end="", flush=True)
    pass


def clear_screen():
    print("\033[2J", end="", flush=True)
    pass


def extract_info():
    postal_codes = pd.read_excel(postal_codes_path)
    # Convert the data into a list of tuples (postal code, count)
    postal_codes_and_count = list(
        postal_codes.itertuples(index=False, name=None))

    doctors_data = []

    move_to_progress_bar()
    # N1R 7E3
    # codes = [['N1R 7E3', 1]]
    codes = [['N1R 7E3', 1]]

    for postal_code, count in codes:
        # for postal_code, count in tqdm(postal_codes_and_count):
        try:
            time.sleep(1)  # Avoid getting blocked
            move_cursor(1, 0)
            print(
                f"Fetching data for Postal Code: {postal_code}, expected count: {count}")
            info = get_doctor_info(postal_code)
            # if info:
            #     doctors_data.append(info)
            #     print(f"✅ {postal_code}")
            exit()
            # move_to_status()
            # clear_line()

            # if info:
            #     doctors_data.append(info)
            #     print(f"✅ {cpso}: {info['LName']}, {info['FName']}")
            # else:
            #     raise AttributeError

            if len(doctors_data) % safety_save == 0:
                save_to_csv(doctors_data)

        except AttributeError:
            move_to_status()
            # print(f"❌ Could not extract data for CPSO #: {cpso}, SKIPPING")

        move_to_progress_bar()

    return doctors_data


def save_to_csv(data):
    file_exists = os.path.isfile(file_name)
    move_cursor(5, 0)
    clear_line()
    print("****************************************************")
    clear_line()
    print(f"Saving data for: {len(data)} doctors")
    # time.sleep(2)
    df = pd.DataFrame(data)
    df.to_csv(file_name, mode="a", header=not file_exists, index=False)
    move_cursor(6, 0)
    clear_line()
    print("Doctor information saved successfully!")
    data.clear()


if __name__ == "__main__":

    # delete the file if it already exists
    if os.path.exists(file_name):
        print("File already exists, deleting it...")
        os.remove(file_name)
    start_time = time.time()
    # this will perform intermitent saves to the csv file
    doctors_data = extract_info()
    end_time = time.time()

    save_to_csv(doctors_data)
