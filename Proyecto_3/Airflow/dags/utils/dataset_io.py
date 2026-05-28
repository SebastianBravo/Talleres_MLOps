import json
import logging
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DATA_API_URL = os.environ.get("DATA_API_URL", "http://api-source:80")
GROUP_ID = int(os.environ.get("DATA_API_GROUP", "7"))


class BatchExhaustedError(Exception):
    """Raised when the API signals that all available batches have been collected."""


def _build_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_batch_from_api():
    """
    Fetches the next available batch from the data API.

    Returns (records, batch_number).  The API manages batch sequencing
    internally — no batch parameter is needed.

    Raises BatchExhaustedError when the API signals all batches have been
    collected.  Raises RuntimeError on unexpected HTTP/network errors.
    """
    session = _build_session()
    url = f"{DATA_API_URL}/data?group_number={GROUP_ID}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"API request failed: {exc}") from exc

    data = response.json()

    if isinstance(data, dict) and "detail" in data:
        raise BatchExhaustedError(data["detail"])

    if not isinstance(data, dict) or "data" not in data:
        raise RuntimeError(f"Unexpected API response shape: {type(data)}")

    records = data["data"]
    batch_number = data.get("batch_number", 0)

    logger.info("Fetched %d records for batch %d", len(records), batch_number)
    return records, batch_number


def save_batch_to_tmpfile(records, run_id):
    """Persists batch records to a temp file and returns the path."""
    tmp_dir = "/tmp/airflow_batches"
    os.makedirs(tmp_dir, exist_ok=True)
    batch_file = os.path.join(tmp_dir, f"{run_id}_batch.json")
    with open(batch_file, "w", encoding="utf-8") as fh:
        json.dump(records, fh)
    return batch_file


def load_batch_from_tmpfile(batch_file):
    """Loads batch records from a previously saved temp file."""
    with open(batch_file, encoding="utf-8") as fh:
        return json.load(fh)
