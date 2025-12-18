# config.py

DEFAULT_CONCURRENCY = 20
POLITE_SLEEP = 0.12

DB_QUEUE_BACKPRESSURE_HIGH = 2000
DB_QUEUE_BACKPRESSURE_LOW = 500

BATCH_FALLBACK_DIR = "db_fallback"
CHECKPOINT_PATH = "checkpoint.json"

BASE_URL = "https://register.cpso.on.ca/Get-Search-Results/"

VALID_FSA_RANGES = {
    "K": ("0A", "7M"),
    "L": ("0A", "9Z"),
    "M": ("1B", "9W"),
    "N": ("0A", "9V"),
    "P": ("0A", "9N"),
}
