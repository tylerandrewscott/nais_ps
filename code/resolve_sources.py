"""
resolve_sources.py — Match journal names to OpenAlex source IDs.

Reads journal name lists from SOURCE_FILES, queries OpenAlex for each name,
and writes results to RESOLVED_FILE and SOURCES_TXT.

Re-running skips already-resolved names.
Use --force to re-query all names from scratch.
"""

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SOURCE_FILES = {
    "nais": Path("../data/nais_sources"),
    "ps":   Path("../data/ps_sources"),
    "ep":   Path("../data/ep_sources"),
}
EMAIL         = "tascott@ucdavis.edu"
KEY_FILE      = Path("../../alex_key")
RESOLVED_FILE = Path("../data/resolved_sources.json")
SOURCES_TXT   = Path("sources.txt")        # read by fetch_works.py
# ---------------------------------------------------------------------------

BASE_URL = "https://api.openalex.org/sources"


def load_api_key(key_file: Path) -> str:
    with key_file.open() as f:
        for line in f:
            line = line.strip()
            if line.startswith("api_key"):
                value = line.split("=", 1)[1].strip().strip("'\"")
                if value:
                    return value
    raise ValueError(f"No api_key entry found in {key_file}")


def load_names(file_path: Path) -> list[str]:
    """Read journal names, one per line, ignoring blank lines."""
    with file_path.open() as f:
        return [line.rstrip() for line in f if line.strip()]


def load_resolved(resolved_file: Path) -> dict:
    """Load existing resolved_sources.json, or return an empty structure."""
    if resolved_file.exists():
        with resolved_file.open() as f:
            return json.load(f)
    return {"sources": [], "unmatched": []}


def save_resolved(data: dict, resolved_file: Path) -> None:
    """Write resolved_sources.json atomically."""
    resolved_file.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=resolved_file.parent, prefix=".resolved_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, resolved_file)
    except Exception:
        os.unlink(tmp)
        raise


def write_sources_txt(data: dict, sources_txt: Path) -> None:
    """Write unique source IDs to sources.txt for fetch_works.py."""
    ids = sorted({entry["source_id"] for entry in data["sources"]})
    with sources_txt.open("w") as f:
        for sid in ids:
            f.write(sid + "\n")
    print(f"Wrote {len(ids)} source IDs to {sources_txt}")


def search_source(
    session: requests.Session,
    name: str,
    email: str,
    api_key: str,
) -> dict | None:
    """
    Search OpenAlex for a source by display name.
    Returns the top result dict, or None if no results found.
    """
    params = {
        "search":   name,
        "per-page": 1,
        "mailto":   email,
        "api_key":  api_key,
    }
    response = session.get(BASE_URL, params=params, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP {response.status_code} searching {name!r}: {response.text[:200]}"
        )
    results = response.json().get("results", [])
    return results[0] if results else None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve journal names to OpenAlex source IDs."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-query all names, even if already resolved.",
    )
    args = parser.parse_args()

    api_key = load_api_key(KEY_FILE)
    session = requests.Session()
    session.headers.update({"User-Agent": f"mailto:{EMAIL}"})

    data = load_resolved(RESOLVED_FILE)

    resolved_names  = {e["query_name"] for e in data["sources"]}
    unmatched_names = {e["query_name"] for e in data["unmatched"]}

    new_matches   = 0
    new_unmatched = 0

    for group, file_path in SOURCE_FILES.items():
        names = load_names(file_path)
        print(f"\n[{group}] {len(names)} journals from {file_path}")

        for name in names:
            if not args.force and (name in resolved_names or name in unmatched_names):
                tag = "resolved" if name in resolved_names else "unmatched"
                print(f"  SKIP      {name!r} (already {tag})")
                continue

            result = search_source(session, name, EMAIL, api_key)
            time.sleep(0.1)

            # Remove any existing entry for this name before updating
            data["sources"]   = [e for e in data["sources"]   if e["query_name"] != name]
            data["unmatched"] = [e for e in data["unmatched"] if e["query_name"] != name]

            if result is None:
                print(f"  NO MATCH  {name!r}")
                data["unmatched"].append({"group": group, "query_name": name})
                new_unmatched += 1
            else:
                source_id     = result["id"].split("/")[-1]   # strip URL prefix → S123...
                openalex_name = result.get("display_name", "")
                print(f"  MATCH     {name!r}")
                print(f"            → {source_id}  ({openalex_name!r})")
                data["sources"].append({
                    "group":         group,
                    "query_name":    name,
                    "source_id":     source_id,
                    "openalex_name": openalex_name,
                })
                new_matches += 1

            save_resolved(data, RESOLVED_FILE)

    write_sources_txt(data, SOURCES_TXT)

    print(f"\nDone.  New matches: {new_matches}  |  New unmatched: {new_unmatched}")

    if data["unmatched"]:
        print(f"\nUnmatched journals ({len(data['unmatched'])}) — review manually:")
        for entry in data["unmatched"]:
            print(f"  [{entry['group']}] {entry['query_name']!r}")


if __name__ == "__main__":
    main()
