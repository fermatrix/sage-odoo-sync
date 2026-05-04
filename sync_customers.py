import csv
import os
from datetime import datetime
from typing import Dict, List, Tuple
import html
import re


DELIMITER = ";"


def truthy(value: str) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "t"}


def normalize_name(value: str) -> str:
    if not value:
        return ""
    v = html.unescape(value).lower()
    v = v.replace("&", " and ")
    v = re.sub(r"[^a-z0-9]+", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return v


def sanitize_external_id(value: str) -> str:
    if not value:
        return ""
    v = str(value).strip()
    if not v:
        return ""
    # Odoo XML IDs cannot contain spaces or punctuation such as commas.
    v = re.sub(r"[^A-Za-z0-9_.-]+", "_", v)
    v = v.strip("_")
    return v


def parse_date(value: str):
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    # Strip common time suffixes if present.
    if " " in raw:
        raw = raw.split(" ", 1)[0].strip()
    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%m-%d-%Y",
        "%d-%m-%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None



def read_csv(path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        delimiter = DELIMITER
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            if getattr(dialect, "delimiter", None) in {";", ","}:
                delimiter = dialect.delimiter
        except Exception:
            delimiter = DELIMITER
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = [dict(r) for r in reader]
        return reader.fieldnames or [], rows


def write_csv(path: str, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=DELIMITER)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def load_env_file(path: str) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def get_env_value(env: Dict[str, str], key: str, fallback: str = "") -> str:
    return os.environ.get(key) or env.get(key) or fallback
