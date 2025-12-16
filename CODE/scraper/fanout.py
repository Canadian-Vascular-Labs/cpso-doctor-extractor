# fanout.py
from string import ascii_uppercase

def build_param_sets(postal, specialties, fan_level, params):
    base = {"postalCode": postal, "cbx-includeinactive": "on"}
    fName = params.get("firstName", "")
    lName = params.get("lastName", "")
    
    if fan_level == 0:
        yield base

    elif fan_level == 2:
        yield {**base, "doctorType": "Family Doctor"}
        for spec in specialties:
            yield {
                **base,
                "doctorType": "Specialist",
                "SpecialistType": spec,
            }

    elif fan_level == 3:
        for l in ascii_uppercase:
            yield {**base, "firstName": fName + l, "lastName": lName}

    elif fan_level == 4:
        for l in ascii_uppercase:
            yield {**base, "firstName": fName, "lastName": lName + l}
