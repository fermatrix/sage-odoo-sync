import argparse
import glob
import os
from datetime import datetime
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

    def is_force_nobarcode_item(r: Dict[str, str]) -> bool:
        return (r.get("ItemID") or "").strip().upper() == "NW77PLAQUE"

    filtered = []
    # Sort by Description for Sales so brands cluster together
    rows_sorted = sorted(rows, key=lambda r: (r.get("ItemDescriptionForSale") or "").upper())
    for r in rows_sorted:
        if (r.get("OdooVariantId") or "").strip():
            continue
        if is_force_nobarcode_item(r):
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


def build_products_sync_nobarcode_new(args: argparse.Namespace) -> int:
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

    def is_barcode_bad(value: str) -> bool:
        v = (value or "").strip()
        if not v:
            return True
        if barcode_digits is None:
            return False
        return (not v.isdigit()) or len(v) < barcode_digits

    def is_excluded_sale_desc(value: str) -> bool:
        v = (value or "").strip().upper()
        if not v:
            return False
        return v.startswith("DERAPAGE") or v.startswith("ECLIPSE") or v.startswith("90 PIECE")

    def is_force_nobarcode_item(r: Dict[str, str]) -> bool:
        return (r.get("ItemID") or "").strip().upper() == "NW77PLAQUE"

    filtered = []
    # Sort by Description for Sales so brands cluster together
    rows_sorted = sorted(rows, key=lambda r: (r.get("ItemDescriptionForSale") or "").upper())
    for r in rows_sorted:
        if (r.get("OdooVariantId") or "").strip():
            continue
        if not is_barcode_bad(r.get("Barcode", "")) and not is_force_nobarcode_item(r):
            continue
        if is_excluded_sale_desc(r.get("ItemDescriptionForSale", "")):
            continue
        if invoiced:
            r["Invoiced2026"] = "X" if (r.get("ItemRecordNumber") or "").strip() in invoiced else ""
        filtered.append(r)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out_fields = list(fields)
    if "Invoiced2026" not in out_fields:
        if "LastLookupAt" in out_fields:
            idx = out_fields.index("LastLookupAt")
            out_fields.insert(idx, "Invoiced2026")
        else:
            out_fields.append("Invoiced2026")
    write_csv(out_path, out_fields, filtered)
    print(f"OK: products sync nobarcode NEW rows: {len(filtered)} -> {out_path}")
    return 0


def build_products_import(args: argparse.Namespace) -> int:
    try:
        from openpyxl import load_workbook
    except Exception:
        load_workbook = None

    if load_workbook is None:
        print("ERROR: openpyxl not available for XLSX export")
        return 2

    sync_path = args.sync_path
    template_path = args.template_path

    if not os.path.exists(sync_path):
        print(f"ERROR: products sync NEW not found: {sync_path}")
        return 2
    if not os.path.exists(template_path):
        print(f"ERROR: template not found: {template_path}")
        return 2

    master_root = os.path.dirname(sync_path)
    odoo_imports = os.path.join(master_root, "odoo_imports")
    os.makedirs(odoo_imports, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d")
    out_xlsx = os.path.join(odoo_imports, f"{stamp}_products_NEW.xlsx")

    _, rows = read_csv(sync_path)

    wb = load_workbook(template_path)
    ws = wb["Products"] if "Products" in wb.sheetnames else wb.active
    # Keep only the first (active) sheet to avoid carrying template extras.
    for sheet_name in list(wb.sheetnames):
        if wb[sheet_name] is not ws:
            wb.remove(wb[sheet_name])
    header_map = {}
    for col_idx in range(1, ws.max_column + 1):
        header = ws.cell(row=1, column=col_idx).value
        if header:
            header_map[str(header).strip()] = col_idx

    def set_cell(col_name: str, value) -> None:
        col_idx = header_map.get(col_name)
        if col_idx:
            ws.cell(row=row_idx, column=col_idx, value=value)

    def adjust_formula(formula: str, row_idx: int) -> str:
        import re
        # Replace any cell reference row number with the current row index,
        # ignoring absolute row ($) so it always tracks the current record.
        # Example: A2 -> A{row_idx}, $B$2 -> $B{row_idx}, C$5 -> C{row_idx}
        def repl(match):
            col = match.group(1)
            return f"{col}{row_idx}"
        return re.sub(r"(\$?[A-Z]{1,3})(\$?)(\d+)", repl, formula)

    # Capture default formulas from row 2, if present
    formula_map = {}
    for col_idx in range(1, ws.max_column + 1):
        v = ws.cell(row=2, column=col_idx).value
        if isinstance(v, str) and v.startswith("="):
            header = ws.cell(row=1, column=col_idx).value
            if header:
                formula_map[str(header).strip()] = v

    # Load brand-specific formulas table (semicolon-separated)
    formulas_by_brand = {}
    formulas_path = os.path.join(master_root, "odoo_templates", "products_formulas.csv")
    if os.path.exists(formulas_path):
        try:
            import csv
            with open(formulas_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    brand = (row.get("brand") or "").strip()
                    if not brand:
                        continue
                    formulas_by_brand[brand] = row
        except Exception:
            formulas_by_brand = {}

    ws.delete_rows(2, ws.max_row)
    row_idx = 2

    def clean_spaces(value: str) -> str:
        return " ".join((value or "").strip().split())

    def extract_brand(desc: str) -> str:
        v = (desc or "").strip()
        if not v:
            return ""
        upper = v.upper()
        multi = ["NW 77TH", "ERKERS 1879", "THE NEIGHBORS"]
        for m in multi:
            if upper.startswith(m):
                return m
        return v.split(" ", 1)[0].strip()

    def strip_brand(desc: str, brand: str) -> str:
        if not desc:
            return ""
        v = desc.strip()
        if brand:
            b = brand.upper()
            if v.upper().startswith(b + " "):
                v = v[len(brand):].strip()
            else:
                # Handle simplified brand names in description (e.g., ERKERS 1879 -> ERKERS)
                aliases = {
                    "ERKERS 1879": "ERKERS",
                    "THE NEIGHBORS": "NEIGHBORS",
                }
                alias = aliases.get(b)
                if alias and v.upper().startswith(alias + " "):
                    v = v[len(alias):].strip()
        return clean_spaces(v)

    def extract_collection(name_processed: str) -> tuple[str, str]:
        import re
        v = name_processed or ""
        # Capture leading "{words} COLLECTION|SERIES" as collection
        m = re.match(r"^\s*([A-Z0-9&/\- ]+?\b(?:COLLECTION|SERIES)\b)\s*(.*)$", v, re.IGNORECASE)
        if not m:
            return "", clean_spaces(v)
        collection = clean_spaces(m.group(1))
        remainder = clean_spaces(m.group(2))
        return collection, remainder

    def extract_color_code(desc: str) -> str:
        import re
        text = desc or ""
        m = re.search(r"\bC-([A-Z0-9]+)\b", text, re.IGNORECASE)
        if not m:
            return ""
        return f"C-{m.group(1)}"

    def is_sunglass(desc: str) -> bool:
        v = (desc or "").upper()
        if "SUNGLASS" in v:
            return True
        if " SG " in f" {v} ":
            return True
        return False

    def compute_brand_code(desc_processed: str, color_code: str) -> str:
        v = desc_processed or ""
        if color_code:
            v = v.replace(color_code, "")
        return clean_spaces(v)

    def sanitize_model_code(value: str) -> str:
        import re
        v = value or ""
        v = re.sub(r"\bMOD\b", "", v, flags=re.IGNORECASE)
        v = re.sub(r"\bSPECIAL\s+RESERVE\b", "", v, flags=re.IGNORECASE)
        v = re.sub(r"\bSLIDER\s+SUNGLASS\b", "", v, flags=re.IGNORECASE)
        v = v.replace("(", " ").replace(")", " ")
        return clean_spaces(v)

    def product_code_for_brand(brand: str, brand_code: str) -> str:
        b = (brand or "").upper()
        model_code = sanitize_model_code(brand_code or "")
        model_code = model_code.replace(" ", "_")
        if not model_code:
            return ""
        if b == "ERKERS 1879":
            return model_code
        if b == "BA&SH":
            return f"BA_{model_code}"
        if b == "MONOQOOL":
            return f"MQ_{model_code}"
        if b in {"THE NEIGHBORS", "NEIGHBORS"}:
            return model_code
        if b == "TOCCO":
            return f"TO_{model_code}"
        if b == "NW 77TH":
            return f"NW_{model_code}"
        return model_code

    rows_sorted = sorted(rows, key=lambda r: (r.get("ItemDescriptionForSale") or "").upper())
    for r in rows_sorted:
        item_id = (r.get("ItemID") or "").strip()
        barcode = (r.get("Barcode") or "").strip()
        desc_sales = (r.get("ItemDescriptionForSale") or "").strip()
        desc_item = (r.get("ItemDescription") or "").strip()

        brand = extract_brand(desc_sales)
        desc_processed = strip_brand(desc_item, brand).replace("C- ", "C-")
        name_processed = strip_brand(desc_sales, brand).replace("C- ", "C-")
        collection, name_processed = extract_collection(name_processed)
        color_code = extract_color_code(desc_item) or extract_color_code(desc_sales)
        brand_code = compute_brand_code(desc_processed, color_code)
        product_code_odoo = product_code_for_brand(brand, brand_code)
        category = f"{brand} / {'Sunglass' if is_sunglass(desc_sales) else 'Optical'}" if brand else ""

        set_cell("x", "E")
        set_cell("id", item_id)
        set_cell("barcode", barcode)
        set_cell("if_favorite", "FALSE")
        set_cell("is_storable", "TRUE")
        set_cell("Description for Sales", desc_sales)
        set_cell("Item Description", desc_item)
        set_cell("brand", brand)
        set_cell("collection", collection)
        set_cell("name_processed", name_processed)
        set_cell("description_processed", desc_processed)
        set_cell("color_code", color_code)
        set_cell("brand_code", brand_code)
        set_cell("product_code_odoo", product_code_odoo)
        set_cell("category", category)

        # Apply formulas for calculated columns (brand-specific if available)
        formula_cols = {
            "name_clean",
            "model",
            "brand_model",
            "color",
            "size",
            "brand_code",
            "color_color_code",
            "search_sting",
        }
        brand_formulas = formulas_by_brand.get(brand, {})
        for col_name in formula_cols:
            formula = brand_formulas.get(col_name) if brand_formulas else None
            if not formula:
                formula = formula_map.get(col_name)
            if formula:
                set_cell(col_name, adjust_formula(formula, row_idx))

        row_idx += 1

    wb.save(out_xlsx)
    print(f"OK: products NEW import rows: {len(rows)} -> {out_xlsx}")
    return 0
