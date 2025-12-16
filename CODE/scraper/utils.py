# utils.py
import string

DIGITS = "0123456789"
LETTERS = string.ascii_uppercase

VALID_FSA_RANGES = {
    "K": ("0A", "7M"),
    "L": ("0A", "9Z"),
    "M": ("1B", "9W"),
    "N": ("0A", "9V"),
    "P": ("0A", "9N"),
}

def generate_valid_ontario_fsas():
    fsas = []
    for prefix, (start, end) in VALID_FSA_RANGES.items():
        for d in DIGITS:
            for l in LETTERS:
                if start <= f"{d}{l}" <= end:
                    fsas.append(f"{prefix}{d}{l}")
    return fsas
