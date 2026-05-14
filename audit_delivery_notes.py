import argparse
import csv
import os
import re
from collections import defaultdict
from typing import Dict, List, Tuple

from sync_customers import DELIMITER, get_env_value, load_env_file
from sync_parity import OdooClient


def _profile_env(env: Dict[str, str], profile: str, key_suffix: str) -> str:
    profile_key = f"ODOO_{profile.upper()}_{key_suffix}"
    generic_key = f"ODOO_{key_suffix}"
    return get_env_value(env, profile_key) or get_env_value(env, generic_key)


def _extract_sage_invoice_ref(note: str) -> str:
    text = str(note or "")
    for raw_ln in text.splitlines():
        ln = re.sub(r"(?is)<[^>]+>", "", raw_ln or "").strip()
        if not ln:
            continue
        m = re.match(r"(?i)^sage\s*invoice\s*:\s*([A-Za-z0-9/\-]+)\s*$", ln)
        if m:
            return m.group(1).strip()
    return ""


def _fetch_all_pickings(client: OdooClient, batch_size: int = 500) -> List[Dict[str, object]]:
    fields = [
        "id",
        "name",
        "state",
        "sale_id",
        "note",
        "origin",
        "carrier_id",
        "scheduled_date",
        "date_done",
        "create_date",
        "write_date",
    ]
    offset = 0
    all_rows: List[Dict[str, object]] = []
    while True:
        rows = client.search_read("stock.picking", [], fields, limit=batch_size, offset=offset)
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
    return all_rows


def _sale_pair(raw_sale) -> Tuple[str, str]:
    if isinstance(raw_sale, list) and raw_sale:
        sid = str(raw_sale[0])
        sname = str(raw_sale[1]) if len(raw_sale) > 1 else ""
        return sid, sname
    return "", ""


def run(args: argparse.Namespace) -> int:
    env = load_env_file(args.env_file)
    url = _profile_env(env, args.profile, "URL")
    db = _profile_env(env, args.profile, "DB")
    user = _profile_env(env, args.profile, "USER")
    apikey = _profile_env(env, args.profile, "APIKEY")

    if not (url and db and user and apikey):
        print("ERROR: missing Odoo credentials (URL/DB/USER/APIKEY)")
        return 2

    client = OdooClient(url, db, user, apikey)

    out_dir = args.out_dir or os.path.join(args.root_dir, "_master_odoo")
    os.makedirs(out_dir, exist_ok=True)
    out_all = args.out_all or os.path.join(out_dir, "pickings_notes_all.csv")
    out_dups = args.out_duplicates or os.path.join(out_dir, "pickings_notes_duplicates.csv")

    rows = _fetch_all_pickings(client, batch_size=args.batch_size)
    enriched: List[Dict[str, str]] = []
    for r in rows:
        sale_id, sale_name = _sale_pair(r.get("sale_id"))
        note = str(r.get("note") or "")
        sage_ref = _extract_sage_invoice_ref(note)
        carrier = r.get("carrier_id") or []
        carrier_name = carrier[1] if isinstance(carrier, list) and len(carrier) > 1 else ""
        enriched.append(
            {
                "PickingId": str(r.get("id") or ""),
                "PickingName": str(r.get("name") or ""),
                "State": str(r.get("state") or ""),
                "SaleOrderId": sale_id,
                "SaleOrderName": sale_name,
                "SageInvoiceRef": sage_ref,
                "Note": note.replace("\r\n", "\n").replace("\r", "\n"),
                "Carrier": str(carrier_name or ""),
                "Origin": str(r.get("origin") or ""),
                "ScheduledDate": str(r.get("scheduled_date") or ""),
                "DateDone": str(r.get("date_done") or ""),
                "CreateDate": str(r.get("create_date") or ""),
                "WriteDate": str(r.get("write_date") or ""),
            }
        )

    with open(out_all, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "PickingId",
                "PickingName",
                "State",
                "SaleOrderId",
                "SaleOrderName",
                "SageInvoiceRef",
                "Carrier",
                "Origin",
                "ScheduledDate",
                "DateDone",
                "CreateDate",
                "WriteDate",
                "Note",
            ],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        for row in sorted(enriched, key=lambda x: (x["SaleOrderName"], x["PickingName"], x["PickingId"])):
            writer.writerow(row)

    by_so_and_ref: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in enriched:
        so_id = row["SaleOrderId"].strip()
        ref = row["SageInvoiceRef"].strip()
        if not so_id or not ref:
            continue
        by_so_and_ref[(so_id, ref)].append(row)

    duplicates: List[Dict[str, str]] = []
    for (so_id, ref), rows_group in by_so_and_ref.items():
        if len(rows_group) <= 1:
            continue
        for row in rows_group:
            dup = dict(row)
            dup["DuplicateGroupCount"] = str(len(rows_group))
            duplicates.append(dup)

    with open(out_dups, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "SaleOrderId",
                "SaleOrderName",
                "SageInvoiceRef",
                "DuplicateGroupCount",
                "PickingId",
                "PickingName",
                "State",
                "Carrier",
                "Origin",
                "ScheduledDate",
                "DateDone",
                "CreateDate",
                "WriteDate",
                "Note",
            ],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        for row in sorted(
            duplicates,
            key=lambda x: (x["SaleOrderName"], x["SageInvoiceRef"], x["PickingName"], x["PickingId"]),
        ):
            writer.writerow(row)

    print(f"OK: exported pickings -> {out_all} ({len(enriched)} rows)")
    print(f"OK: duplicate Sage notes -> {out_dups} ({len(duplicates)} rows)")
    print(f"Summary: duplicate groups={(len(set((r['SaleOrderId'], r['SageInvoiceRef']) for r in duplicates)))}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit duplicated Sage notes in Odoo Delivery Orders.")
    parser.add_argument("--profile", default="STUDIOOPTYX", help="Odoo profile suffix (default: STUDIOOPTYX)")
    parser.add_argument("--env-file", default=".env", help="Path to .env with Odoo credentials")
    parser.add_argument("--root-dir", default=r"ENZO-Sage50", help="Project root (default: ENZO-Sage50)")
    parser.add_argument("--out-dir", default="", help="Output directory (default: <root>/_master_odoo)")
    parser.add_argument("--out-all", default="", help="Full export CSV path")
    parser.add_argument("--out-duplicates", default="", help="Duplicates export CSV path")
    parser.add_argument("--batch-size", type=int, default=500, help="search_read batch size")
    return parser


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))

