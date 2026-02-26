"""
fetch_works.py — Download works from OpenAlex for a set of source IDs.

Source IDs are loaded from resolved_sources.json (produced by resolve_sources.py).
If that file is absent, SOURCE_IDS in the CONFIG section is used as a fallback.
"""

import argparse
import json
import shutil
import time
import tempfile
import os
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# CONFIG — edit before running
# ---------------------------------------------------------------------------
SOURCE_IDS = ["S1234567890", "S0987654321"]  # fallback if sources.txt absent
EMAIL      = "tascott@ucdavis.edu"
OUTPUT_DIR = Path("../data/oa_works")
PER_PAGE   = 200                            # OpenAlex max
KEY_FILE   = Path("../../alex_key")         # contains: api_key='...'
# ---------------------------------------------------------------------------

RESOLVED_FILE = Path("../data/resolved_sources.json")
PROGRESS_FILE = "progress.json"
BASE_URL      = "https://api.openalex.org/works"


def load_api_key(key_file: Path) -> str:
    """
    Read the API key from a file containing a line like:
        api_key='abc123'
    Returns the key value. Raises FileNotFoundError or ValueError if missing/malformed.
    """
    with key_file.open() as f:
        for line in f:
            line = line.strip()
            if line.startswith("api_key"):
                # handle api_key='value' or api_key="value" or api_key=value
                value = line.split("=", 1)[1].strip().strip("'\""  )
                if value:
                    return value
    raise ValueError(f"No api_key entry found in {key_file}")


def load_source_ids() -> list[str]:
    """
    Return the list of source IDs to process.
    Reads resolved_sources.json if it exists; falls back to hardcoded SOURCE_IDS.
    Deduplicates while preserving order.
    """
    if RESOLVED_FILE.exists():
        with RESOLVED_FILE.open() as f:
            data = json.load(f)
        seen = set()
        ids = []
        for entry in data.get("sources", []):
            sid = entry["source_id"]
            if sid not in seen:
                seen.add(sid)
                ids.append(sid)
        print(f"Loaded {len(ids)} source IDs from {RESOLVED_FILE}")
        return ids
    print(f"{RESOLVED_FILE} not found — using SOURCE_IDS from config.")
    return SOURCE_IDS


def load_progress(output_dir: Path) -> dict:
    """Read progress.json; return {} if absent."""
    path = output_dir / PROGRESS_FILE
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def save_progress(progress: dict, output_dir: Path) -> None:
    """Write progress.json atomically via a temp file."""
    path = output_dir / PROGRESS_FILE
    fd, tmp_path = tempfile.mkstemp(dir=output_dir, prefix=".progress_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(progress, f, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        os.unlink(tmp_path)
        raise


def fetch_page(
    session: requests.Session,
    source_id: str,
    cursor: str,
    email: str,
    per_page: int,
    api_key: str,
) -> dict:
    """
    Perform a single GET to /works for the given source and cursor.
    Returns the parsed JSON response dict.
    Raises RuntimeError on non-200 status.
    """
    params = {
        "filter":   f"primary_location.source.id:{source_id}",
        "per-page": per_page,
        "cursor":   cursor,
        "mailto":   email,
        "api_key":  api_key,
    }
    response = session.get(BASE_URL, params=params, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP {response.status_code} fetching source={source_id} cursor={cursor!r}: "
            f"{response.text[:200]}"
        )
    return response.json()


def fetch_source(
    session: requests.Session,
    source_id: str,
    output_dir: Path,
    progress: dict,
    email: str,
    per_page: int,
    api_key: str,
) -> None:
    """
    Download all pages for one source, saving each page to disk.
    - Skips sources already marked 'complete' in progress.
    - Resumes from saved cursor if status is 'in_progress'.
    """
    state = progress.get(source_id, {})

    if state.get("status") == "complete":
        print(f"[{source_id}] Already complete ({state.get('pages', '?')} pages). Skipping.")
        return

    source_dir = output_dir / source_id
    source_dir.mkdir(parents=True, exist_ok=True)

    if state.get("status") == "in_progress":
        cursor = state["next_cursor"]
        page_num = state["pages"] + 1
        print(f"[{source_id}] Resuming from cursor (page {page_num}).")
    else:
        cursor = "*"
        page_num = 1
        print(f"[{source_id}] Starting from page 1.")

    while True:
        data = fetch_page(session, source_id, cursor, email, per_page, api_key)

        meta = data.get("meta", {})
        page_file = source_dir / f"page_{page_num:04d}.json"
        with page_file.open("w") as f:
            json.dump(data, f)

        result_count = len(data.get("results", []))
        print(f"[{source_id}] Page {page_num:4d} saved — {result_count} works")

        next_cursor = meta.get("next_cursor")

        if next_cursor is None:
            progress[source_id] = {"status": "complete", "pages": page_num}
            save_progress(progress, output_dir)
            print(f"[{source_id}] Complete. Total pages: {page_num}")
            break

        progress[source_id] = {
            "status":      "in_progress",
            "next_cursor": next_cursor,
            "pages":       page_num,
        }
        save_progress(progress, output_dir)

        page_num += 1
        cursor = next_cursor
        time.sleep(0.1)  # polite pool: ~10 req/sec


def clobber_sources(source_ids: list[str], output_dir: Path, progress: dict) -> None:
    """
    For each source ID, delete its output directory and remove it from progress
    so it will be fully re-fetched on this run.
    """
    for source_id in source_ids:
        source_dir = output_dir / source_id
        if source_dir.exists():
            shutil.rmtree(source_dir)
            print(f"[{source_id}] Clobbered — deleted {source_dir}")
        if source_id in progress:
            del progress[source_id]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch works from OpenAlex for a set of source IDs."
    )
    parser.add_argument(
        "--clobber",
        action="store_true",
        help="Delete existing data and re-fetch all sources in the current list.",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    progress = load_progress(OUTPUT_DIR)
    api_key  = load_api_key(KEY_FILE)

    source_ids = load_source_ids()

    if args.clobber:
        print(f"--clobber: resetting {len(source_ids)} sources.")
        clobber_sources(source_ids, OUTPUT_DIR, progress)
        save_progress(progress, OUTPUT_DIR)

    session = requests.Session()
    session.headers.update({
        "User-Agent":      f"mailto:{EMAIL}",
        "Accept-Encoding": "gzip, deflate",  # exclude brotli to avoid decoder errors
    })

    for source_id in source_ids:
        fetch_source(session, source_id, OUTPUT_DIR, progress, EMAIL, PER_PAGE, api_key)

    print("All sources done.")


if __name__ == "__main__":
    main()
