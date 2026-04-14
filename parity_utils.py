from typing import Dict
import os

from sync_customers import read_csv


def load_country_parity(parity_path: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not os.path.exists(parity_path):
        return mapping
    _, rows = read_csv(parity_path)
    for r in rows:
        raw = (r.get("sage_country_raw") or "").strip()
        code = (r.get("odoo_country_code") or "").strip()
        if raw and code:
            mapping[raw] = code
    return mapping


def load_country_name_to_code(countries_odoo_path: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not os.path.exists(countries_odoo_path):
        return mapping
    _, rows = read_csv(countries_odoo_path)
    for r in rows:
        name = (r.get("OdooName") or "").strip()
        code = (r.get("OdooCode") or "").strip()
        if name and code:
            mapping[name] = code
    return mapping


def load_state_parity(state_parity_path: str) -> Dict[str, Dict[str, str]]:
    mapping: Dict[str, Dict[str, str]] = {}
    if not os.path.exists(state_parity_path):
        return mapping
    _, rows = read_csv(state_parity_path)
    for r in rows:
        raw = (r.get("sage_state_raw") or "").strip()
        if not raw:
            continue
        mapping[raw] = {
            "state_name": (r.get("odoo_state_name") or "").strip(),
            "country_name": (r.get("odoo_country_name") or "").strip(),
        }
    return mapping


def normalize_country(raw: str, country_parity: Dict[str, str], country_name_to_code: Dict[str, str]) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw in country_parity:
        return country_parity[raw]
    if raw in country_name_to_code:
        return country_name_to_code[raw]
    upper = raw.upper()
    if upper in {"USA", "U.S.A.", "US", "UNITED STATES", "UNITED STATES OF AMERICA"}:
        return "US"
    if upper in {"CANADA", "CA", "CAN"}:
        return "CA"
    return raw


def normalize_state(raw: str, state_parity: Dict[str, Dict[str, str]]) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    info = state_parity.get(raw, {})
    return info.get("state_name") or raw
