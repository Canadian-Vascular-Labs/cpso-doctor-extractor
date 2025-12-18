from string import ascii_uppercase

def build_param_sets(fsa, ldus, specialties, fan_level, params):
    base = {
        "postalCode": fsa,
        "cbx-includeinactive": "on",
    }

    # initial query
    if fan_level == 0:
        yield base

    elif fan_level == 1:
        # fanout with ldu: (fsa) + (ldu)
        for ldu in ldus:
            yield {**base, **params, "postalCode": f"{fsa} {ldu}"}

    elif fan_level == 2:
        for l in ascii_uppercase:
            yield {**base, **params, "lastName": l}

    elif fan_level == 3:
        for l in ascii_uppercase:
            yield {**base, **params, "firstName": l}

    elif fan_level == 4:
        yield {**base, **params, "doctorType": "Family Doctor"}
        yield {**base, **params, "doctorType": "Specialist"}

    elif fan_level == 5:
        if params.get("doctorType") != "Specialist":
            return

        for spec in specialties:
            yield {
                **base,
                **params,
                "SpecialistType": spec,
            }