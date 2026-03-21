import argparse
import glob
import os
from typing import Dict, List

from sync_customers import read_csv, write_csv


def _pick_latest(paths: List[str]) -> str:
    if not paths:
        return ""
    return max(paths, key=lambda p: os.path.getmtime(p))


def build_product_sync(args: argparse.Namespace) -> int:
    year_month = args.year_month
    base_dir = args.base_dir
    items_master = args.items_master
    items_sync = args.items_sync
    out_path = args.out_path.replace("{year_month}", year_month)

    if not os.path.exists(items_master):
        print(f"ERROR: items master not found: {items_master}")
        return 2
    if not os.path.exists(items_sync):
        print(f"ERROR: products sync not found: {items_sync}")
        return 2

    inv_glob = os.path.join(base_dir, "**", f"{year_month}_invoice_lines.csv")
    cn_glob = os.path.join(base_dir, "**", f"{year_month}_credit_note_lines.csv")
    inv_paths = glob.glob(inv_glob, recursive=True)
    cn_paths = glob.glob(cn_glob, recursive=True)
    if not inv_paths:
        print(f"ERROR: invoice lines not found for {year_month} (search: {inv_glob})")
        return 2
    if not cn_paths:
        print(f"ERROR: credit note lines not found for {year_month} (search: {cn_glob})")
        return 2

    invoice_lines = _pick_latest(inv_paths)
    credit_lines = _pick_latest(cn_paths)
    if len(inv_paths) > 1:
        print(f"WARNING: multiple invoice_lines found, using latest: {invoice_lines}")
    if len(cn_paths) > 1:
        print(f"WARNING: multiple credit_note_lines found, using latest: {credit_lines}")

    # Load invoice + credit note lines
    _, inv_rows = read_csv(invoice_lines)
    _, cn_rows = read_csv(credit_lines)
    item_records = set()
    for row in inv_rows + cn_rows:
        rec = (row.get("ItemRecordNumber") or "").strip()
        if rec:
            item_records.add(rec)
    item_records.discard("0")

    # Load items master + sync
    _, master_rows = read_csv(items_master)
    master_by_record = { (r.get("ItemRecordNumber") or "").strip(): r for r in master_rows }

    _, sync_rows = read_csv(items_sync)
    sync_by_record = { (r.get("ItemRecordNumber") or "").strip(): r for r in sync_rows }

    fieldnames = [
        "ItemRecordNumber",
        "ItemID",
        "ItemDescription",
        "SalesDescription",
        "Barcode",
        "OdooVariantId",
        "OdooName",
        "OdooVariantName",
        "OdooItemCode",
        "OdooColor",
        "Reason",
    ]
    rows_out: List[Dict[str, str]] = []
    for rec in sorted(item_records, key=lambda x: int(x) if x.isdigit() else x):
        sync = sync_by_record.get(rec)
        reason = ""
        if not sync:
            reason = "NO_SYNC"
        else:
            if not (sync.get("OdooVariantId") or "").strip():
                reason = "NO_ODOO"
        if not reason:
            continue
        master = master_by_record.get(rec, {})
        rows_out.append({
            "ItemRecordNumber": rec,
            "ItemID": (sync.get("ItemID") or master.get("ItemID") or "").strip(),
            "ItemDescription": (sync.get("ItemDescription") or master.get("ItemDescription") or "").strip(),
            "SalesDescription": (master.get("SalesDescription") or "").strip(),
            "Barcode": (master.get("UPC_SKU") or "").strip(),
            "OdooVariantId": (sync.get("OdooVariantId") or "").strip(),
            "OdooName": (sync.get("OdooName") or "").strip(),
            "OdooVariantName": (sync.get("OdooVariantName") or "").strip(),
            "OdooItemCode": (sync.get("OdooItemCode") or "").strip(),
            "OdooColor": (sync.get("OdooColor") or "").strip(),
            "Reason": reason,
        })

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    write_csv(out_path, fieldnames, rows_out)
    print(f"OK: product sync rows: {len(rows_out)} -> {out_path}")
    return 0


def build_items_sync_new(args: argparse.Namespace) -> int:
    items_sync = args.items_sync
    out_path = args.out_path
    barcode_digits = None if args.barcode_digits == 0 else args.barcode_digits
    invoice_base_dir = args.invoice_base_dir

    if not os.path.exists(items_sync):
        print(f"ERROR: products sync not found: {items_sync}")
        return 2

    fields, rows = read_csv(items_sync)
    if not fields:
        print(f"ERROR: products sync has no headers: {items_sync}")
        return 2

    # Build invoiced set for 2026 (Feb + Mar)
    invoiced = set()
    if invoice_base_dir:
        targets = [
            os.path.join(invoice_base_dir, "**", "2026_02_invoice_lines.csv"),
            os.path.join(invoice_base_dir, "**", "2026_03_invoice_lines.csv"),
        ]
        matched = []
        for pattern in targets:
            matched.extend(glob.glob(pattern, recursive=True))
        if not matched:
            print("WARNING: no invoice_lines found for 2026_02 or 2026_03")
        for path in matched:
            try:
                _, inv_rows = read_csv(path)
            except Exception:
                continue
            for r in inv_rows:
                rec = (r.get("ItemRecordNumber") or "").strip()
                if rec:
                    invoiced.add(rec)

    def is_barcode_ok(value: str) -> bool:
        v = (value or "").strip()
        if not v:
            return False
        if barcode_digits is None:
            return True
        return v.isdigit() and len(v) >= barcode_digits

    def is_excluded_sale_desc(value: str) -> bool:
        v = (value or "").strip().upper()
        if not v:
            return False
        return v.startswith("DERAPAGE") or v.startswith("ECLIPSE") or v.startswith("90 PIECE")

    filtered = []
    for r in rows:
        if (r.get("OdooVariantId") or "").strip():
            continue
        if not is_barcode_ok(r.get("Barcode", "")):
            continue
        if is_excluded_sale_desc(r.get("ItemDescriptionForSale", "")):
            continue
        if invoiced:
            r["Invoiced2026"] = "X" if (r.get("ItemRecordNumber") or "").strip() in invoiced else ""
        filtered.append(r)

    out_fields = list(fields)
    if "Invoiced2026" not in out_fields:
        if "LastLookupAt" in out_fields:
            idx = out_fields.index("LastLookupAt")
            out_fields.insert(idx, "Invoiced2026")
        else:
            out_fields.append("Invoiced2026")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    write_csv(out_path, out_fields, filtered)
    print(f"OK: items sync NEW rows: {len(filtered)} -> {out_path}")
    return 0
