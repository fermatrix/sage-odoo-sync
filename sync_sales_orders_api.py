import argparse
import csv
import os
import re
import textwrap
from collections import Counter
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sync_customers import DELIMITER, get_env_value, load_env_file, read_csv
from sync_parity import OdooClient
from parity_utils import (
    load_country_name_to_code,
    load_country_parity,
    load_state_parity,
    normalize_country,
)


def parse_decimal(raw: str) -> float:
    value = (raw or "").strip()
    if not value:
        return 0.0
    value = value.replace(".", "").replace(",", ".") if value.count(",") == 1 and value.count(".") > 1 else value
    value = value.replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return 0.0


def filter_non_importable_sage_rows(
    source_lines: List[Dict[str, str]],
    header: Dict[str, str],
) -> List[Dict[str, str]]:
    """
    Remove Sage technical row 0 that usually repeats header description + order total.
    Typical pattern:
      - ItemRecordNumber = 0
      - RowNumber = 0
      - Quantity = 0
      - RowDescription == header Description
      - Amount ~= header MainAmount
    """
    out: List[Dict[str, str]] = []
    header_desc = (header.get("Description") or "").strip().upper()
    header_amount = parse_decimal(header.get("MainAmount") or "")
    for line in source_lines:
        item_record = (line.get("ItemRecordNumber") or "").strip()
        row_number = (line.get("RowNumber") or "").strip()
        qty = parse_decimal(line.get("Quantity") or "")
        row_desc = (line.get("RowDescription") or "").strip().upper()
        row_amount = parse_decimal(line.get("Amount") or "")
        looks_like_header_technical_row = (
            item_record == "0"
            and row_number == "0"
            and abs(qty) < 0.0001
            and bool(row_desc)
            and row_desc == header_desc
            and abs(row_amount - header_amount) <= 0.02
        )
        if looks_like_header_technical_row:
            continue
        out.append(line)
    return out


def profile_env(env: Dict[str, str], profile: str, key_suffix: str) -> str:
    profile_key = f"ODOO_{profile.upper()}_{key_suffix}"
    generic_key = f"ODOO_{key_suffix}"
    return get_env_value(env, profile_key) or get_env_value(env, generic_key)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Create Sage Sales Orders in Odoo via API",
        allow_abbrev=False,
    )
    p.add_argument("--root-dir", default=r"ENZO-Sage50")
    p.add_argument("--profile", default="STUDIOOPTYX")
    p.add_argument("--env-file", default=".env")
    p.add_argument(
        "--headers-path",
        default=r"ENZO-Sage50\13_2026\01_02_Feb\2026_02_sales_orders_headers.csv",
    )
    p.add_argument(
        "--lines-path",
        default=r"ENZO-Sage50\13_2026\01_02_Feb\2026_02_sales_orders_lines.csv",
    )
    p.add_argument("--customers-sync", default=r"ENZO-Sage50\_master\customers_sync.csv")
    p.add_argument(
        "--products-sync",
        default=r"ENZO-Sage50\_master\products_sync.csv",
        help="Deprecated/ignored. Sales orders mapping never uses products_sync.",
    )
    p.add_argument("--items-master", default=r"ENZO-Sage50\_master_sage\items.csv")
    p.add_argument(
        "--items-odoo",
        default=r"ENZO-Sage50\_master_odoo\items_odoo.csv",
        help="Deprecated/ignored. Odoo variants are read live from API.",
    )
    p.add_argument(
        "--employees-sync",
        default=r"ENZO-Sage50\_master\employees_sync.csv",
        help="Deprecated/ignored. Sales rep resolution does not use employees_sync.",
    )
    p.add_argument("--employees-master", default=r"ENZO-Sage50\_master_sage\employees.csv")
    p.add_argument(
        "--limit",
        default="",
        help="How many orders to process. Also supports 'start,count' (example: 12,1). Empty = no limit.",
    )
    p.add_argument("--offset", type=int, default=0, help="Skip N candidate orders first")
    p.add_argument(
        "--log-path",
        default=r"ENZO-Sage50\_master\orders_api_log.csv",
        help="Append execution results here",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve and validate but do not create orders in Odoo",
    )
    p.add_argument(
        "--reference",
        default="",
        help="Process one or many Sage references separated by comma (example: 357702 or 357702,357703)",
    )
    p.add_argument(
        "--ignore-references",
        default="",
        help=(
            "Comma-separated Sage references to ignore. "
            "If empty, loaded from env SALES_ORDER_IGNORE_REFERENCES "
            "(or profile key <PROFILE>_IGNORE_REFERENCES)."
        ),
    )
    p.add_argument(
        "--load",
        default="",
        help=(
            "Auto-load Sales Orders by period/date from Sage exports. "
            "Formats: DD/MM/YYYY, MM/YYYY, YYYY (fiscal year Feb->Jan), "
            "or ranges: DD/MM/YYYY-DD/MM/YYYY, MM/YYYY-DD/MM/YYYY"
        ),
    )
    p.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow partial processing when a required mapping is missing (default: stop on first critical error)",
    )
    p.add_argument(
        "--skip",
        action="store_true",
        help="Continue after errors (alias of --allow-partial)",
    )
    p.add_argument(
        "--gaps",
        action="store_true",
        help=(
            "Process only Sage sales orders missing in Odoo. "
            "Stops automatically before the first trailing never-imported block."
        ),
    )
    p.add_argument(
        "--shipping-relaxed",
        action="store_true",
        help=(
            "Keep strict mode by default. When enabled, allow a slightly relaxed "
            "shipping match (state parity + street synonyms like #/SUITE/STE)."
        ),
    )
    p.add_argument(
        "--freight-variant-id",
        type=int,
        default=0,
        help="Odoo product.product id to use for Sage freight/shipping amount lines (ItemRecordNumber=0).",
    )
    p.add_argument(
        "--freight-product-name",
        default="MISC SHIPPING",
        help="Fallback product name lookup for freight when --freight-variant-id is not provided.",
    )
    p.add_argument(
        "--concept-variant-id",
        type=int,
        default=0,
        help="Odoo product.product id to use for Sage concept rows (ItemRecordNumber=0 with qty/amount/description).",
    )
    p.add_argument(
        "--concept-product-name",
        default="MISC CHARGE",
        help="Fallback product name lookup for concept rows when --concept-variant-id is not provided.",
    )
    p.add_argument(
        "--content-verify",
        action="store_true",
        help=(
            "Verify existing Odoo order line SKUs against Sage SKUs (via ItemRecordNumber -> ItemID) "
            "without creating/updating orders."
        ),
    )
    p.add_argument(
        "--content-repair",
        action="store_true",
        help=(
            "When used with --content-verify, auto-repair mismatches by reopening ORDER to QUOTE, "
            "updating order_lines and pricelist_id."
        ),
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        help="Confirm orders after create/update/repair. Default behavior leaves orders in QUOTE/draft.",
    )
    return p


def load_customers_map(path: str) -> Dict[str, Dict[str, str]]:
    _, rows = read_csv(path)
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        key = (r.get("CustomerRecordNumber") or "").strip()
        if not key:
            continue
        out[key] = r
    return out


def load_products_map(path: str) -> Dict[str, Dict[str, str]]:
    _, rows = read_csv(path)
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        key = (r.get("ItemRecordNumber") or "").strip()
        if not key:
            continue
        out[key] = r
    return out


def load_employees_map(path: str) -> Dict[str, Dict[str, str]]:
    _, rows = read_csv(path)
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        key = (r.get("EmpRecordNumber") or "").strip()
        if key:
            out[key] = r
    return out


def load_order_data(headers_path: str, lines_path: str):
    _, headers = read_csv(headers_path)
    _, lines = read_csv(lines_path)
    lines_by_postorder: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in lines:
        lines_by_postorder[(row.get("PostOrder") or "").strip()].append(row)
    for key in lines_by_postorder:
        lines_by_postorder[key].sort(key=lambda r: int((r.get("RowNumber") or "0").strip() or 0))
    return headers, lines_by_postorder


def _parse_load_spec(spec: str) -> Tuple[str, object]:
    raw = (spec or "").strip()
    if not raw:
        return "", None
    if "-" in raw:
        left, right = [x.strip() for x in raw.split("-", 1)]
        start = _parse_load_point_to_start(left)
        end_exclusive = _parse_load_point_to_end_exclusive(right)
        if end_exclusive <= start:
            raise ValueError(f"Invalid --load range: {raw}. End must be after start.")
        return "range", (start, end_exclusive)
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", raw):
        day, month, year = raw.split("/")
        return "day", date(int(year), int(month), int(day))
    if re.fullmatch(r"\d{2}/\d{4}", raw):
        month, year = raw.split("/")
        return "month", (int(year), int(month))
    if re.fullmatch(r"\d{4}", raw):
        fiscal_year = int(raw)
        start = date(fiscal_year, 2, 1)
        end = date(fiscal_year + 1, 2, 1)
        return "fiscal_year", (start, end)
    raise ValueError(
        f"Unsupported --load format: {raw}. Use DD/MM/YYYY, MM/YYYY or YYYY."
    )


def _first_day_next_month(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def _parse_load_point_to_start(raw: str) -> date:
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", raw):
        day, month, year = raw.split("/")
        return date(int(year), int(month), int(day))
    if re.fullmatch(r"\d{2}/\d{4}", raw):
        month, year = raw.split("/")
        return date(int(year), int(month), 1)
    if re.fullmatch(r"\d{4}", raw):
        year = int(raw)
        return date(year, 2, 1)  # Fiscal year start
    raise ValueError(f"Unsupported range boundary: {raw}")


def _parse_load_point_to_end_exclusive(raw: str) -> date:
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", raw):
        day, month, year = raw.split("/")
        return date(int(year), int(month), int(day)) + timedelta(days=1)
    if re.fullmatch(r"\d{2}/\d{4}", raw):
        month, year = raw.split("/")
        return _first_day_next_month(int(year), int(month))
    if re.fullmatch(r"\d{4}", raw):
        year = int(raw)
        return date(year + 1, 2, 1)  # Fiscal year end exclusive
    raise ValueError(f"Unsupported range boundary: {raw}")


def _parse_limit_offset(limit_arg: str, offset_arg: int) -> Tuple[Optional[int], int]:
    raw = str(limit_arg or "").strip()
    if not raw:
        return None, max(0, int(offset_arg or 0))
    if "," in raw:
        # Syntax: start,count (1-based start ordinal)
        left, right = raw.split(",", 1)
        start_ordinal = int(left.strip())
        count = int(right.strip())
        return max(1, count), max(0, start_ordinal - 1)
    return max(1, int(raw)), max(0, int(offset_arg or 0))


def _parse_reference_filter(raw: str) -> set:
    text = (raw or "").strip()
    if not text:
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


def resolve_customer_reference_from_sage(header: Dict[str, str]) -> str:
    # Primary source is PurchOrder. Some legacy flows (notably Buying Groups)
    # use CustomerInvoiceNo as operational PO/customer reference.
    po = (header.get("PurchOrder") or "").strip()
    if po:
        return po
    return (header.get("CustomerInvoiceNo") or "").strip()


def _iter_sales_order_pairs(root_dir: str) -> List[Tuple[str, str]]:
    base_dir = os.path.join(root_dir, "13_2026")
    fallback_root = os.path.join(root_dir)
    search_roots = [base_dir] if os.path.isdir(base_dir) else [fallback_root]
    pairs: List[Tuple[str, str]] = []
    for search_root in search_roots:
        for current_root, _, files in os.walk(search_root):
            for filename in files:
                if not filename.endswith("_sales_orders_headers.csv"):
                    continue
                headers_path = os.path.join(current_root, filename)
                lines_name = filename.replace("_sales_orders_headers.csv", "_sales_orders_lines.csv")
                lines_path = os.path.join(current_root, lines_name)
                if os.path.exists(lines_path):
                    pairs.append((headers_path, lines_path))
    return sorted(pairs, key=lambda p: p[0])


def _assert_semicolon_csv(path: str) -> None:
    """Fail fast when a CSV is not semicolon-delimited.

    Sage source files must be generated with ';' to keep the pipeline stable
    and auditable. We never auto-rewrite delimiters here.
    """
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            detected = dialect.delimiter
        except Exception:
            detected = ";"
    if detected != ";":
        raise ValueError(
            "Invalid delimiter detected in Sage Sales Orders file. "
            f"Expected ';' but got '{detected}' in: {path}"
        )


def _matches_load(transaction_date: str, mode: str, value: object) -> bool:
    if not mode:
        return True
    tx_date = datetime.strptime(transaction_date.strip(), "%Y-%m-%d").date()
    if mode == "day":
        return tx_date == value
    if mode == "month":
        year, month = value
        return tx_date.year == year and tx_date.month == month
    if mode == "fiscal_year":
        start, end = value
        return start <= tx_date < end
    if mode == "range":
        start, end = value
        return start <= tx_date < end
    return False


def load_order_data_auto(root_dir: str, load_spec: str):
    mode, value = _parse_load_spec(load_spec)
    pairs = _iter_sales_order_pairs(root_dir)
    if not pairs:
        raise FileNotFoundError("No *_sales_orders_headers.csv / *_sales_orders_lines.csv pairs found")

    all_headers: List[Dict[str, str]] = []
    all_lines_by_postorder: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    matched_files = 0

    for headers_path, lines_path in pairs:
        _assert_semicolon_csv(headers_path)
        _assert_semicolon_csv(lines_path)
        _, header_rows = read_csv(headers_path)
        selected_headers = []
        post_orders = set()
        for row in header_rows:
            tx_date = (row.get("TransactionDate") or "").strip()
            if not tx_date:
                continue
            try:
                if not _matches_load(tx_date, mode, value):
                    continue
            except Exception:
                continue
            selected_headers.append(row)
            po = (row.get("PostOrder") or "").strip()
            if po:
                post_orders.add(po)

        if not selected_headers:
            continue

        _, line_rows = read_csv(lines_path)
        for row in line_rows:
            po = (row.get("PostOrder") or "").strip()
            if po in post_orders:
                all_lines_by_postorder[po].append(row)
        all_headers.extend(selected_headers)
        matched_files += 1

    for key in all_lines_by_postorder:
        all_lines_by_postorder[key].sort(key=lambda r: int((r.get("RowNumber") or "0").strip() or 0))

    print(f"INFO: auto-load matched files={matched_files}, headers={len(all_headers)}")
    return all_headers, all_lines_by_postorder


def _fetch_existing_sale_orders_by_name(
    client: OdooClient,
    references: List[str],
) -> Dict[str, Dict[str, object]]:
    refs = [r.strip() for r in references if str(r or "").strip()]
    if not refs:
        return {}
    out: Dict[str, Dict[str, object]] = {}
    chunk_size = 200
    for i in range(0, len(refs), chunk_size):
        chunk = refs[i:i + chunk_size]
        rows = client.search_read(
            "sale.order",
            [("name", "in", chunk)],
            ["id", "name", "state", "partner_shipping_id"],
            limit=10000,
            offset=0,
        )
        for row in rows:
            name = str(row.get("name") or "").strip()
            if name and name not in out:
                out[name] = row
    return out


def load_products_map_from_masters(
    items_master_path: str,
    client: OdooClient,
) -> Dict[str, Dict[str, str]]:
    if not os.path.exists(items_master_path):
        return {}

    _, sage_rows = read_csv(items_master_path)
    sage_by_record: Dict[str, Dict[str, str]] = {}
    for r in sage_rows:
        rec = (r.get("ItemRecordNumber") or "").strip()
        if rec:
            sage_by_record[rec] = r

    # Live read from Odoo to avoid stale local snapshots.
    odoo_rows: List[Dict[str, object]] = []
    offset = 0
    limit = 2000
    while True:
        batch = client.models.execute_kw(
            client.db,
            client.uid,
            client.apikey,
            "product.product",
            "search_read",
            [[]],
            {
                "fields": ["id", "default_code", "name", "active", "product_tmpl_id"],
                "limit": limit,
                "offset": offset,
                "context": {"active_test": False},
            },
        )
        if not batch:
            break
        odoo_rows.extend(batch)
        offset += len(batch)

    odoo_by_code: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for r in odoo_rows:
        code = str(r.get("default_code") or "").strip().upper()
        if code:
            odoo_by_code[code].append({
                "OdooVariantId": str(r.get("id") or ""),
                "OdooVariantExternalId": "",
                "OdooName": str(r.get("name") or "").strip(),
                "OdooItemCode": code,
                "OdooColor": "",
                "OdooTemplateId": (
                    str((r.get("product_tmpl_id") or [None])[0] or "")
                    if isinstance(r.get("product_tmpl_id"), list)
                    else ""
                ),
                "OdooTemplateExternalId": "",
                "Active": str(bool(r.get("active", True))),
            })

    def _pick_variant(rows: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
        if not rows:
            return None
        if len(rows) == 1:
            return rows[0]
        active = [r for r in rows if str(r.get("Active") or "").strip().lower() in {"true", "1"}]
        if len(active) == 1:
            return active[0]
        if active:
            rows = active
        return rows[0]

    out: Dict[str, Dict[str, str]] = {}
    for rec, sage in sage_by_record.items():
        item_id = (sage.get("ItemID") or "").strip().upper()
        if not item_id:
            continue
        picked = _pick_variant(odoo_by_code.get(item_id, []))
        if not picked:
            continue
        out[rec] = {
            "ItemRecordNumber": rec,
            "ItemID": (sage.get("ItemID") or "").strip(),
            "ItemDescription": (sage.get("ItemDescription") or "").strip(),
            "ItemDescriptionForSale": (sage.get("ItemDescriptionForSale") or "").strip(),
            "OdooVariantId": (picked.get("OdooVariantId") or "").strip(),
            "OdooVariantExternalId": (picked.get("OdooVariantExternalId") or "").strip(),
            "OdooName": (picked.get("OdooName") or "").strip(),
            "OdooItemCode": (picked.get("OdooItemCode") or "").strip(),
            "OdooColor": (picked.get("OdooColor") or "").strip(),
            "OdooTemplateId": (picked.get("OdooTemplateId") or "").strip(),
            "OdooTemplateExternalId": (picked.get("OdooTemplateExternalId") or "").strip(),
            "MapSource": "master_sage_odoo",
        }
    return out


def find_payment_terms(client: OdooClient) -> Dict[str, int]:
    terms = client.search_read(
        "account.payment.term",
        [],
        ["id", "name"],
        limit=9999,
        offset=0,
    )
    out: Dict[str, int] = {}
    for t in terms:
        name = (t.get("name") or "").strip()
        if not name:
            continue
        out[name.lower()] = int(t["id"])
    return out


def find_sales_taxes_by_code(client: OdooClient) -> Dict[str, int]:
    rows = client.search_read(
        "account.tax",
        [("type_tax_use", "=", "sale"), ("active", "=", True)],
        ["id", "name"],
        limit=9999,
        offset=0,
    )
    out: Dict[str, int] = {}
    wanted = {"SO", "CA", "TN", "PACIFIC", "ILLINOIS"}
    for r in rows:
        name = (r.get("name") or "").strip()
        if not name:
            continue
        upper = name.upper()
        for code in wanted:
            if code in out:
                continue
            if upper.startswith(f"{code} -") or upper.startswith(f"{code} "):
                out[code] = int(r["id"])
    return out


def resolve_term_id(terms_map: Dict[str, int], sage_term: str) -> Optional[int]:
    raw = (sage_term or "").strip()
    if not raw:
        return None
    direct = terms_map.get(raw.lower())
    if direct:
        return direct
    # Soft fallback: contains in either direction
    low = raw.lower()
    for name, tid in terms_map.items():
        if low in name or name in low:
            return tid
    return None


def build_order_lines(
    source_lines: List[Dict[str, str]],
    products_map: Dict[str, Dict[str, str]],
    freight_variant_id: int = 0,
    concept_variant_id: int = 0,
    header_main_amount: float = 0.0,
) -> Dict[str, object]:
    def _looks_like_misc_shipping(raw_desc: str) -> bool:
        t = (raw_desc or "").strip().upper()
        if not t:
            return False
        markers = [
            "SHIPPING",
            "FREIGHT",
            "SHIP",
            "UPS",
            "US MAIL",
            "USPS",
            "AIRBORNE",
            "COURIER",
            "DELIVERY",
            "CUSTOMS",
            "EXPEDITION",
        ]
        return any(m in t for m in markers)

    def _format_sage_line_description(raw_desc: str) -> str:
        text = (raw_desc or "").strip()
        if not text:
            return ""
        # For Sales Orders we keep Sage RAW description verbatim in second line.
        # Odoo may render its own product label as the first line in some views.
        return f"\n{text}"

    out: List[Dict[str, object]] = []
    note_lines: List[Dict[str, object]] = []
    source_total = 0.0
    skipped_reasons: List[str] = []
    has_shipping_line = False
    shipping_line_indexes: List[int] = []
    tax_total_source = 0.0
    tax_authority_codes: List[str] = []
    source_row_count = 0
    source_product_row_count = 0
    variant_debug_rows: List[Dict[str, str]] = []
    for line in source_lines:
        source_row_count += 1
        row_desc_raw = (line.get("RowDescription") or "").strip()
        row_desc_formatted = _format_sage_line_description(row_desc_raw)
        row_desc_upper = row_desc_raw.upper()
        item_record = (line.get("ItemRecordNumber") or "").strip()
        is_bogo_transaction = item_record == "8521" or row_desc_upper.startswith("BOGO TRANSACTION")
        if is_bogo_transaction:
            # Keep BOGO marker as top note in Odoo, not as product line.
            note_lines.append({
                "display_type": "line_note",
                "name": row_desc_raw or "BOGO TRANSACTION",
            })
            continue
        if not item_record or item_record == "0":
            row_desc = row_desc_upper
            code = (line.get("TaxAuthorityCode") or "").strip().upper()
            looks_like_tax = bool(code) or ("TAX" in row_desc)
            if looks_like_tax:
                tax_amount = abs(parse_decimal(line.get("Amount") or ""))
                if tax_amount > 0:
                    tax_total_source += tax_amount
                    if code and code not in tax_authority_codes:
                        tax_authority_codes.append(code)
                continue
            # Non-tax technical rows can still carry misc shipping or concept amounts.
            amount_signed = parse_decimal(line.get("Amount") or "")
            # For free-text concept-only SOs, Sage may store detail rows as
            # positive while header MainAmount is negative (credit style).
            # Preserve accounting sign by aligning technical item=0 amounts
            # with header sign when needed.
            if header_main_amount < 0 and amount_signed > 0:
                amount_signed = -amount_signed
            is_misc_shipping = abs(amount_signed) > 0 and _looks_like_misc_shipping(row_desc_raw)
            if is_misc_shipping:
                if freight_variant_id:
                    source_total += amount_signed
                    source_product_row_count += 1
                    out.append({
                        "product_id": int(freight_variant_id),
                        "name": (row_desc_formatted or "MISC SHIPPING"),
                        "product_uom_qty": 1.0,
                        "price_unit": round(amount_signed, 2),
                        "tax_ids": [(5, 0, 0)],
                    })
                    variant_debug_rows.append({
                        "product_id": str(freight_variant_id),
                        "item_record": "0",
                        "item_id": "FREIGHT_AMOUNT",
                        "row_desc": row_desc_raw,
                    })
                    has_shipping_line = True
                    shipping_line_indexes.append(len(out) - 1)
                else:
                    skipped_reasons.append(
                        f"Misc shipping line ({row_desc_raw}): missing freight variant id configuration"
                    )
            elif abs(amount_signed) > 0:
                # Operational/business concept row from Sage (not a stock product).
                # Replicate it in Odoo as a service concept line. If description is
                # empty, keep explicit placeholder requested by business.
                qty = parse_decimal(line.get("Quantity") or "")
                if qty <= 0:
                    qty = 1.0
                concept_name = (row_desc_formatted or row_desc_raw or "<empty description>")
                if concept_variant_id:
                    source_total += amount_signed
                    source_product_row_count += 1
                    out.append({
                        "product_id": int(concept_variant_id),
                        "name": concept_name,
                        "product_uom_qty": qty,
                        "price_unit": round(amount_signed / qty if qty else amount_signed, 2),
                        "tax_ids": [(5, 0, 0)],
                    })
                    variant_debug_rows.append({
                        "product_id": str(concept_variant_id),
                        "item_record": "0",
                        "item_id": "SAGE_CONCEPT",
                        "row_desc": concept_name,
                    })
                else:
                    skipped_reasons.append(
                        f"Concept line ({concept_name}): missing concept variant id configuration"
                    )
            elif row_desc_raw:
                # Preserve non-tax/non-amount operational text rows from Sage.
                # Example: DROP SHIP / LAB routing instructions used by business.
                note_lines.append({
                    "display_type": "line_note",
                    "name": row_desc_raw,
                })
            else:
                row_number = (line.get("RowNumber") or "").strip()
                amount_raw = (line.get("Amount") or "").strip()
                # Accept/ignore the common Sage technical header row:
                # RowNumber=0, ItemRecordNumber=0, Description empty, Amount=0.
                # This row is not a business line and should not block processing.
                if row_number == "0" and parse_decimal(amount_raw or "0") == 0:
                    pass
                else:
                    # Strict rule requested by business:
                    # if this technical/comment-like row has no recognizable content,
                    # stop the process to avoid silent information loss.
                    skipped_reasons.append(
                        "ItemRecordNumber 0 row without transferable description "
                        f"(RowNumber={line.get('RowNumber') or ''}, PostOrder={line.get('PostOrder') or ''})"
                    )
            continue
        source_amount = parse_decimal(line.get("Amount") or "")
        if abs(source_amount) > 0:
            source_total += source_amount
        source_product_row_count += 1
        product_sync = products_map.get(item_record)
        if not product_sync:
            row_desc = row_desc_raw
            skipped_reasons.append(
                f"ItemRecordNumber {item_record} ({row_desc}): not found in master mapping (items.csv -> items_odoo.csv)"
            )
            continue
        variant_id = int((product_sync.get("OdooVariantId") or "0").strip() or 0)
        if not variant_id:
            item_id = (product_sync.get("ItemID") or "").strip()
            item_desc = (product_sync.get("ItemDescriptionForSale") or line.get("RowDescription") or "").strip()
            skipped_reasons.append(
                f"ItemRecordNumber {item_record} / ItemID {item_id} ({item_desc}): missing OdooVariantId"
            )
            continue
        qty = parse_decimal(line.get("Quantity") or "")
        if qty <= 0:
            # Accept explicit zero-quantity rows in Sage SO as non-billable
            # technical lines (do not block order creation/repair).
            amount_signed = parse_decimal(line.get("Amount") or "")
            if qty == 0 and abs(amount_signed) <= 0.000001:
                continue
            skipped_reasons.append(f"ItemRecordNumber {item_record}: invalid quantity {line.get('Quantity')}")
            continue
        price_unit = parse_decimal(line.get("UnitCost") or "")
        if price_unit == 0:
            amount_signed = parse_decimal(line.get("Amount") or "")
            price_unit = amount_signed / qty if qty else 0
        item_id = (product_sync.get("ItemID") or "").strip().upper()
        item_desc = (product_sync.get("ItemDescription") or "").strip().upper()
        row_desc = row_desc_upper
        is_shipping = (
            "SHIPPING" in item_id
            or "SHIPPING" in item_desc
            or "SHIPPING" in row_desc
        )
        out.append({
            "product_id": variant_id,
            "name": row_desc_formatted or row_desc_raw,
            "product_uom_qty": qty,
            "price_unit": round(price_unit, 2),
            # Always set taxes explicitly: avoid implicit product default taxes in Odoo.
            "tax_ids": [(5, 0, 0)],
        })
        variant_debug_rows.append({
            "product_id": str(variant_id),
            "item_record": item_record,
            "item_id": (product_sync.get("ItemID") or "").strip(),
            "row_desc": (line.get("RowDescription") or "").strip(),
        })
        if is_shipping:
            has_shipping_line = True
            shipping_line_indexes.append(len(out) - 1)
    # Put notes first (BOGO marker requested by business).
    if note_lines:
        out = note_lines + out
        shipping_line_indexes = [idx + len(note_lines) for idx in shipping_line_indexes]
    prepared_product_line_count = len([l for l in out if not str(l.get("display_type") or "").strip()])
    prepared_total = round(
        sum(
            (float(l.get("product_uom_qty") or 0.0) * float(l.get("price_unit") or 0.0))
            for l in out
            if not str(l.get("display_type") or "").strip()
        ),
        2,
    )
    return {
        "lines": out,
        "source_total": round(source_total, 2),
        "prepared_total": prepared_total,
        "skipped_reasons": skipped_reasons,
        "has_shipping_line": has_shipping_line,
        "shipping_line_indexes": shipping_line_indexes,
        "tax_total_source": round(tax_total_source, 2),
        "tax_authority_codes": tax_authority_codes,
        "source_row_count": source_row_count,
        "source_product_row_count": source_product_row_count,
        "prepared_product_line_count": prepared_product_line_count,
        "variant_debug_rows": variant_debug_rows,
    }


def norm_text(value: str) -> str:
    return " ".join((value or "").strip().upper().split())


def resolve_shipping_partner_id(
    client: OdooClient,
    partner_id: int,
    ship_to_name: str,
    ship_to_address1: str,
    ship_to_address2: str,
    ship_to_city: str,
    ship_to_state: str,
    ship_to_zip: str,
    relaxed: bool = False,
) -> tuple[int, bool, str]:
    deliveries = client.search_read(
        "res.partner",
        [("parent_id", "=", partner_id), ("type", "=", "delivery")],
        ["id", "name", "street", "street2", "city", "zip", "state_id", "type"],
        limit=200,
        offset=0,
    )

    tgt_name = norm_text(ship_to_name)
    tgt_street = norm_text(ship_to_address1)
    tgt_street2 = norm_text(ship_to_address2)
    tgt_city = norm_text(ship_to_city)
    tgt_state = norm_text(ship_to_state)
    tgt_zip = norm_text(ship_to_zip)

    us_state_by_code = {
        "AL": "ALABAMA", "AK": "ALASKA", "AZ": "ARIZONA", "AR": "ARKANSAS", "CA": "CALIFORNIA",
        "CO": "COLORADO", "CT": "CONNECTICUT", "DE": "DELAWARE", "FL": "FLORIDA", "GA": "GEORGIA",
        "HI": "HAWAII", "ID": "IDAHO", "IL": "ILLINOIS", "IN": "INDIANA", "IA": "IOWA",
        "KS": "KANSAS", "KY": "KENTUCKY", "LA": "LOUISIANA", "ME": "MAINE", "MD": "MARYLAND",
        "MA": "MASSACHUSETTS", "MI": "MICHIGAN", "MN": "MINNESOTA", "MS": "MISSISSIPPI",
        "MO": "MISSOURI", "MT": "MONTANA", "NE": "NEBRASKA", "NV": "NEVADA", "NH": "NEW HAMPSHIRE",
        "NJ": "NEW JERSEY", "NM": "NEW MEXICO", "NY": "NEW YORK", "NC": "NORTH CAROLINA",
        "ND": "NORTH DAKOTA", "OH": "OHIO", "OK": "OKLAHOMA", "OR": "OREGON", "PA": "PENNSYLVANIA",
        "RI": "RHODE ISLAND", "SC": "SOUTH CAROLINA", "SD": "SOUTH DAKOTA", "TN": "TENNESSEE",
        "TX": "TEXAS", "UT": "UTAH", "VT": "VERMONT", "VA": "VIRGINIA", "WA": "WASHINGTON",
        "WV": "WEST VIRGINIA", "WI": "WISCONSIN", "WY": "WYOMING",
    }
    ca_state_by_code = {
        "AB": "ALBERTA", "BC": "BRITISH COLUMBIA", "MB": "MANITOBA", "NB": "NEW BRUNSWICK",
        "NL": "NEWFOUNDLAND AND LABRADOR", "NS": "NOVA SCOTIA", "NT": "NORTHWEST TERRITORIES",
        "NU": "NUNAVUT", "ON": "ONTARIO", "PE": "PRINCE EDWARD ISLAND", "QC": "QUEBEC",
        "SK": "SASKATCHEWAN", "YT": "YUKON",
    }

    def _soft_street(value: str) -> str:
        s = norm_text(value)
        # Normalize common suite tokens to reduce false negatives.
        s = s.replace("SUITE", "STE")
        s = s.replace(" APARTMENT ", " APT ")
        s = s.replace("#", " STE ")
        s = " ".join(s.split())
        return s

    def score(row: Dict[str, object]) -> int:
        st = row.get("state_id") or []
        row_state_name = ""
        row_state_code = ""
        if isinstance(st, list) and len(st) > 1:
            raw_state = (st[1] or "").strip()
            row_state_name = raw_state.split("(", 1)[0].strip()
            if "(" in raw_state and ")" in raw_state:
                row_state_code = raw_state.split("(", 1)[1].split(")", 1)[0].strip()
        points = 0
        if tgt_street and norm_text(row.get("street", "")) == tgt_street:
            points += 6
        if tgt_city and norm_text(row.get("city", "")) == tgt_city:
            points += 3
        if tgt_zip and norm_text(row.get("zip", "")) == tgt_zip:
            points += 3
        if tgt_state:
            if row_state_code and tgt_state == norm_text(row_state_code):
                points += 2
            elif row_state_name and (tgt_state == norm_text(row_state_name) or tgt_state in norm_text(row_state_name)):
                points += 2
        if tgt_name and norm_text(row.get("name", "")) == tgt_name:
            points += 1
        if tgt_street2 and norm_text(row.get("street2", "")) == tgt_street2:
            points += 1
        if relaxed:
            # Slightly relaxed extras (do not apply in strict mode).
            row_street_soft = _soft_street(str(row.get("street", "")))
            tgt_street_soft = _soft_street(ship_to_address1)
            if tgt_street_soft and row_street_soft and row_street_soft == tgt_street_soft and not (
                tgt_street and norm_text(row.get("street", "")) == tgt_street
            ):
                points += 2

            row_state_name_norm = norm_text(row_state_name)
            tgt_state_name = us_state_by_code.get(tgt_state) or ca_state_by_code.get(tgt_state) or ""
            if tgt_state_name and row_state_name_norm == norm_text(tgt_state_name):
                # Small boost only when parity maps code->name exactly.
                points += 1
        return points

    candidates = list(deliveries)
    # If no delivery match exists, fallback to any child contact at same address
    # (useful when Sage "Ship To" points to a person/contact instead of a delivery type).
    others = client.search_read(
        "res.partner",
        [("parent_id", "=", partner_id), ("type", "!=", "delivery")],
        ["id", "name", "street", "street2", "city", "zip", "state_id", "type"],
        limit=200,
        offset=0,
    )
    candidates.extend(others)
    if not candidates:
        return partner_id, False, "no child contacts/delivery addresses found in Odoo"

    ranked = sorted(candidates, key=score, reverse=True)
    best = ranked[0]
    best_score = score(best)
    st = best.get("state_id") or []
    best_state = ""
    if isinstance(st, list) and len(st) > 1:
        best_state = str(st[1] or "").strip()
    max_score = 12 if relaxed else 9
    best_summary = (
        f"id={best.get('id')} type={best.get('type')} score={best_score}/{max_score} "
        f"name='{(best.get('name') or '').strip()}' "
        f"street='{(best.get('street') or '').strip()}' "
        f"street2='{(best.get('street2') or '').strip()}' "
        f"city='{(best.get('city') or '').strip()}' "
        f"state='{best_state}' "
        f"zip='{(best.get('zip') or '').strip()}'"
    )
    # Require strong address match; otherwise keep parent customer address.
    # Special strict-mode case: when Sage ship-to has no city/state/zip/street2,
    # allow an exact (street + name) match to pass.
    strict_sparse_geo = (
        not relaxed
        and bool(tgt_street)
        and not tgt_city
        and not tgt_state
        and not tgt_zip
    )
    if relaxed:
        threshold = 8
    elif strict_sparse_geo:
        # Without geo keys in Sage (city/state/zip empty), accept strong
        # textual match (street + name, and optionally street2).
        threshold = 8
    else:
        threshold = 9
    if best_score >= threshold:
        return int(best["id"]), True, best_summary
    return partner_id, False, best_summary


def resolve_invoice_partner_id(client: OdooClient, partner_id: int) -> int:
    invoice_contacts = client.search_read(
        "res.partner",
        [("parent_id", "=", partner_id), ("type", "=", "invoice")],
        ["id", "name"],
        limit=200,
        offset=0,
    )
    if not invoice_contacts:
        return partner_id
    invoice_contacts = sorted(invoice_contacts, key=lambda r: int(r.get("id") or 0))
    return int(invoice_contacts[0]["id"])


def load_address_parity_maps(root_dir: str) -> Dict[str, Dict[str, object]]:
    master = os.path.join(root_dir, "_master")
    master_odoo = os.path.join(root_dir, "_master_odoo")
    country_parity_path = os.path.join(master, "_parity_country.csv")
    state_parity_path = os.path.join(master, "_parity_state.csv")
    countries_odoo_path = os.path.join(master_odoo, "countries_odoo.csv")
    country_parity = load_country_parity(country_parity_path)
    country_name_to_code = load_country_name_to_code(countries_odoo_path)
    state_parity = load_state_parity(state_parity_path)
    return {
        "country_parity": country_parity,
        "country_name_to_code": country_name_to_code,
        "state_parity": state_parity,
    }


def fetch_country_state_indexes(client: OdooClient) -> Dict[str, Dict[str, int]]:
    countries = client.search_read("res.country", [], ["id", "code", "name"], limit=9999, offset=0)
    by_code: Dict[str, int] = {}
    for c in countries:
        code = (c.get("code") or "").strip().upper()
        cid = int(c.get("id") or 0)
        if code and cid:
            by_code[code] = cid

    states = client.search_read("res.country.state", [], ["id", "name", "code", "country_id"], limit=9999, offset=0)
    by_country_code_and_state: Dict[Tuple[str, str], int] = {}
    by_state_only: Dict[str, int] = {}
    for s in states:
        sid = int(s.get("id") or 0)
        if not sid:
            continue
        code = (s.get("code") or "").strip().upper()
        name = (s.get("name") or "").strip().upper()
        country = s.get("country_id") or []
        country_label = (country[1] if isinstance(country, list) and len(country) > 1 else "").strip()
        country_code = ""
        if "(" in country_label and ")" in country_label:
            country_code = country_label.split("(", 1)[1].split(")", 1)[0].strip().upper()
        if not country_code:
            # fallback from country id
            country_id = country[0] if isinstance(country, list) and country else 0
            for cc, cid in by_code.items():
                if int(cid) == int(country_id):
                    country_code = cc
                    break
        if code and country_code:
            by_country_code_and_state[(country_code, code)] = sid
        if name and country_code:
            by_country_code_and_state[(country_code, name)] = sid
        if code:
            by_state_only.setdefault(code, sid)
    return {
        "country_by_code": by_code,
        "state_by_country_and_state": by_country_code_and_state,
        "state_by_state": by_state_only,
    }


def create_shipping_address_on_the_fly(
    client: OdooClient,
    parent_id: int,
    ship_to_name: str,
    ship_to_address1: str,
    ship_to_address2: str,
    ship_to_city: str,
    ship_to_state: str,
    ship_to_zip: str,
    ship_to_country: str,
    parity_maps: Dict[str, Dict[str, object]],
    indexes: Dict[str, Dict[str, int]],
) -> tuple[Optional[int], str]:
    country_parity = parity_maps.get("country_parity", {})
    country_name_to_code = parity_maps.get("country_name_to_code", {})
    state_parity = parity_maps.get("state_parity", {})
    country_by_code = indexes.get("country_by_code", {})
    state_by_country_and_state = indexes.get("state_by_country_and_state", {})
    state_by_state = indexes.get("state_by_state", {})

    raw_state = (ship_to_state or "").strip()
    raw_country = (ship_to_country or "").strip()

    normalized_country = normalize_country(raw_country, country_parity, country_name_to_code)
    state_info = state_parity.get(raw_state, {})
    state_name = (state_info.get("state_name") or raw_state).strip()
    inferred_country_name = (state_info.get("country_name") or "").strip()
    if not normalized_country and inferred_country_name:
        normalized_country = normalize_country(inferred_country_name, country_parity, country_name_to_code)

    country_code = (normalized_country or "").strip().upper()
    country_id = country_by_code.get(country_code) if country_code else None
    state_id = None
    if state_name:
        key_code = state_name.strip().upper()
        if country_code:
            state_id = state_by_country_and_state.get((country_code, key_code))
            if not state_id and raw_state:
                state_id = state_by_country_and_state.get((country_code, raw_state.strip().upper()))
        if not state_id:
            state_id = state_by_state.get(raw_state.strip().upper()) if raw_state else None

    vals: Dict[str, object] = {
        "parent_id": int(parent_id),
        "type": "delivery",
        "name": (ship_to_name or "").strip() or "Shipping Address",
        "street": (ship_to_address1 or "").strip(),
        "street2": (ship_to_address2 or "").strip(),
        "city": (ship_to_city or "").strip(),
        "zip": (ship_to_zip or "").strip(),
    }
    if country_id:
        vals["country_id"] = int(country_id)
    if state_id:
        vals["state_id"] = int(state_id)

    new_id = client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "res.partner",
        "create",
        [vals],
    )
    return int(new_id), (
        "Shipping address created on the fly "
        f"(id={new_id}; "
        f"name='{(vals.get('name') or '')}'; "
        f"street='{(vals.get('street') or '')}'; "
        f"street2='{(vals.get('street2') or '')}'; "
        f"city='{(vals.get('city') or '')}'; "
        f"state='{raw_state}'; "
        f"zip='{(vals.get('zip') or '')}'; "
        f"country='{country_code or ''}')"
    )


def to_login(employee_id: str) -> str:
    return (employee_id or "").strip().lower().replace(" ", "_")


def _norm_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def resolve_sales_user_live(
    emp: Dict[str, str],
    user_by_login: Dict[str, Dict[str, object]],
    user_by_name_norm: Dict[str, Dict[str, object]],
) -> Tuple[str, Optional[Dict[str, object]]]:
    """Resolve salesperson using live Odoo users only (no local Odoo mappings)."""
    emp_id = (emp.get("EmployeeID") or "").strip()
    login_candidate = to_login(emp_id)
    if login_candidate:
        u = user_by_login.get(login_candidate)
        if u:
            return login_candidate, u

    # Fallback: exact name match against current Odoo users (live truth)
    emp_name = (emp.get("EmployeeName") or "").strip()
    if emp_name:
        u = user_by_name_norm.get(_norm_text(emp_name))
        if u:
            return (u.get("login") or "").strip().lower(), u

    return login_candidate, None


def append_log(path: str, rows: List[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = [
        "Timestamp",
        "Status",
        "Reference",
        "PostOrder",
        "TransactionDate",
        "CustomerRecordNumber",
        "CustomerOdooId",
        "OrderOdooId",
        "OrderState",
        "OrderPath",
        "NoSalesRep",
        "LineCount",
        "Details",
    ]
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=DELIMITER)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _fmt_date(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw.split(" ", 1)[0]


def _line_sig_from_prepared(line_vals: Dict[str, object]) -> Dict[str, object]:
    display_type = str(line_vals.get("display_type") or "")
    if display_type:
        return {
            "display_type": display_type,
            "product_id": 0,
            "name": str(line_vals.get("name") or ""),
            "qty": 0.0,
            "price": 0.0,
            "tax_ids": [],
        }
    tax_ids: List[int] = []
    tax_cmds = line_vals.get("tax_ids") or []
    if isinstance(tax_cmds, list) and tax_cmds:
        first = tax_cmds[0]
        if isinstance(first, tuple) and len(first) >= 3 and isinstance(first[2], list):
            tax_ids = sorted(int(x) for x in first[2])
    return {
        "display_type": "",
        "product_id": int(line_vals.get("product_id") or 0),
        "name": str(line_vals.get("name") or ""),
        "qty": round(float(line_vals.get("product_uom_qty") or 0.0), 4),
        "price": round(float(line_vals.get("price_unit") or 0.0), 4),
        "tax_ids": tax_ids,
    }


def _line_sig_from_existing(line_row: Dict[str, object]) -> Dict[str, object]:
    display_type = str(line_row.get("display_type") or "")
    product = line_row.get("product_id") or []
    tax_ids = line_row.get("tax_ids") or []
    return {
        "display_type": display_type,
        "product_id": int(product[0]) if isinstance(product, list) and product else 0,
        "name": str(line_row.get("name") or ""),
        "qty": round(float(line_row.get("product_uom_qty") or 0.0), 4) if not display_type else 0.0,
        "price": round(float(line_row.get("price_unit") or 0.0), 4) if not display_type else 0.0,
        "tax_ids": sorted(int(x) for x in tax_ids) if isinstance(tax_ids, list) else [],
    }


def _base_sig_from_vals(vals: Dict[str, object]) -> Dict[str, object]:
    return {
        "partner_id": int(vals.get("partner_id") or 0),
        "date_order": _fmt_date(vals.get("date_order")),
        "validity_date": _fmt_date(vals.get("validity_date")),
        "commitment_date": _fmt_date(vals.get("commitment_date")),
        "require_signature": bool(vals.get("require_signature")),
        "origin": str(vals.get("origin") or "").strip(),
        "client_order_ref": str(vals.get("client_order_ref") or "").strip(),
        "note": str(vals.get("note") or "").strip(),
        "pricelist_id": int(vals.get("pricelist_id") or 0),
        "payment_term_id": int(vals.get("payment_term_id") or 0),
        "user_id": int(vals.get("user_id") or 0),
        "team_id": int(vals.get("team_id") or 0),
        "partner_invoice_id": int(vals.get("partner_invoice_id") or 0),
        "partner_shipping_id": int(vals.get("partner_shipping_id") or 0),
    }


def _existing_order_sig(client: OdooClient, order_id: int) -> Dict[str, object]:
    rows = client.search_read(
        "sale.order",
        [("id", "=", order_id)],
        [
            "id",
            "partner_id",
            "date_order",
            "validity_date",
            "commitment_date",
            "require_signature",
            "origin",
            "client_order_ref",
            "note",
            "pricelist_id",
            "payment_term_id",
            "user_id",
            "team_id",
            "partner_invoice_id",
            "partner_shipping_id",
            "order_line",
        ],
        limit=1,
        offset=0,
    )
    if not rows:
        return {"base": {}, "lines": []}
    row = rows[0]
    base = {
        "partner_id": int((row.get("partner_id") or [0])[0]) if isinstance(row.get("partner_id"), list) else 0,
        "date_order": _fmt_date(row.get("date_order")),
        "validity_date": _fmt_date(row.get("validity_date")),
        "commitment_date": _fmt_date(row.get("commitment_date")),
        "require_signature": bool(row.get("require_signature")),
        "origin": str(row.get("origin") or "").strip(),
        "client_order_ref": str(row.get("client_order_ref") or "").strip(),
        "note": str(row.get("note") or "").strip(),
        "pricelist_id": int((row.get("pricelist_id") or [0])[0]) if isinstance(row.get("pricelist_id"), list) else 0,
        "payment_term_id": int((row.get("payment_term_id") or [0])[0]) if isinstance(row.get("payment_term_id"), list) else 0,
        "user_id": int((row.get("user_id") or [0])[0]) if isinstance(row.get("user_id"), list) else 0,
        "team_id": int((row.get("team_id") or [0])[0]) if isinstance(row.get("team_id"), list) else 0,
        "partner_invoice_id": int((row.get("partner_invoice_id") or [0])[0]) if isinstance(row.get("partner_invoice_id"), list) else 0,
        "partner_shipping_id": int((row.get("partner_shipping_id") or [0])[0]) if isinstance(row.get("partner_shipping_id"), list) else 0,
    }
    line_ids = row.get("order_line") or []
    lines: List[Dict[str, object]] = []
    if line_ids:
        line_rows = client.models.execute_kw(
            client.db,
            client.uid,
            client.apikey,
            "sale.order.line",
            "read",
            [line_ids],
            {"fields": ["id", "display_type", "product_id", "name", "product_uom_qty", "price_unit", "tax_ids"]},
        )
        line_by_id = {int(l["id"]): l for l in line_rows}
        for lid in line_ids:
            lrow = line_by_id.get(int(lid))
            if lrow:
                lines.append(_line_sig_from_existing(lrow))
    return {"base": base, "lines": lines}


def _print_order_progress(entry: Dict[str, str], index: int) -> None:
    status = (entry.get("Status") or "").strip()
    label = {
        "OK": "CREATED",
        "OK_WARN": "CREATED_WARN",
        "OK_UPDATE": "UPDATED",
        "OK_UPDATE_WARN": "UPDATED_WARN",
        "NO_CHANGES": "NO_CHANGES",
        "NO_CHANGES_WARN": "NO_CHANGES_WARN",
        "ERROR": "ERROR",
        "DRY_RUN": "DRY_RUN_CREATE",
        "DRY_RUN_WARN": "DRY_RUN_CREATE_WARN",
        "DRY_RUN_UPDATE": "DRY_RUN_UPDATE",
        "DRY_RUN_UPDATE_WARN": "DRY_RUN_UPDATE_WARN",
    }.get(status, status or "INFO")
    ref = (entry.get("Reference") or "").strip()
    order_id = (entry.get("OrderOdooId") or "").strip()
    details = (entry.get("Details") or "").strip()
    has_no_sales_rep = str(entry.get("NoSalesRep") or "").strip().lower() in {"1", "true", "yes", "y"}
    raw_state = str(entry.get("OrderState") or "").strip().lower()
    explicit_path = str(entry.get("OrderPath") or "").strip()
    state_label = ""
    if raw_state in {"sale", "done"}:
        state_label = "ORDER"
    elif raw_state in {"draft", "sent"}:
        state_label = "QUOTE"
    details_lower = details.lower()
    just_confirmed = (
        label in {"CREATED", "CREATED_WARN", "UPDATED", "UPDATED_WARN"}
        and "confirmed" in details_lower
        and state_label == "ORDER"
    )
    shown_state = explicit_path or ("QUOTE > ORDER" if just_confirmed else state_label)

    tx_date = _fmt_date(entry.get("TransactionDate") or "")
    tx_date_out = tx_date
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", tx_date):
        y, m, d = tx_date.split("-")
        tx_date_out = f"{d}/{m}/{y}"

    line = f"[{index:03d}]"
    if tx_date_out:
        line += f" - {tx_date_out}"
    line += f" - [{label}] sage {ref}"
    if order_id:
        line += f" | odoo {order_id}"
    if shown_state:
        line += f" | {shown_state}"
    if has_no_sales_rep:
        line += " (no sales rep)"
    print(line)
    # Avoid redundant second line for no-change outcomes.
    normalized = details.strip().lower()
    suppress = normalized in {"no changes in content", "no changes in content; confirmed"}
    if details and not suppress:
        details_for_parts = details
        pretty_shipping = _pretty_shipping_mismatch(details)
        if pretty_shipping:
            for ln in pretty_shipping:
                print(ln)
            consumed = _shipping_mismatch_raw(details)
            if consumed:
                details_for_parts = details.replace(consumed, "")
                details_for_parts = details_for_parts.strip(" ;")
        parts = [p.strip() for p in details_for_parts.split(";") if p.strip() and p.strip().lower() != "no sales rep"]
        for part in parts:
            pretty_rows = _pretty_sage_source_rows(part)
            if pretty_rows:
                for ln in pretty_rows:
                    print(ln)
                continue
            wrapped = textwrap.fill(
                part,
                width=120,
                initial_indent="        - ",
                subsequent_indent="          ",
            )
            pretty_created = _pretty_shipping_created(part)
            if pretty_created:
                for ln in pretty_created:
                    print(ln)
            else:
                print(wrapped)
    print("")


def _pretty_shipping_mismatch(part: str) -> Optional[List[str]]:
    if "Missing exact shipping address match in Odoo " not in part:
        return None
    pattern = re.compile(
        r"Missing exact shipping address match in Odoo\s*\(\s*"
        r"customer_odoo_id=(?P<cid>\d+);\s*"
        r"(?:customer_name='(?P<cname>[^']*)';\s*)?"
        r"sage_ship_to=name='(?P<s_name>[^']*)',\s*street='(?P<s_street>[^']*)',\s*street2='(?P<s_street2>[^']*)',\s*"
        r"city='(?P<s_city>[^']*)',\s*state='(?P<s_state>[^']*)',\s*zip='(?P<s_zip>[^']*)';\s*"
        r"best_candidate=id=(?P<b_id>\d+)\s*type=(?P<b_type>\S+)\s*score=(?P<b_score>\d+/\d+)\s*"
        r"name='(?P<b_name>[^']*)'\s*street='(?P<b_street>[^']*)'\s*street2='(?P<b_street2>[^']*)'\s*"
        r"city='(?P<b_city>[^']*)'\s*state='(?P<b_state>[^']*)'\s*zip='(?P<b_zip>[^']*)'\s*\)"
    )
    m = pattern.search(part)
    if not m:
        return None
    g = m.groupdict()

    sage_line3 = f"{g['s_city']} - {g['s_state']} ({g['s_zip']})".strip()
    odoo_line3 = f"{g['b_city']} - {g['b_state']} ({g['b_zip']})".strip()
    score_raw = (g.get("b_score") or "").strip()
    score_num = -1
    try:
        score_num = int(score_raw.split("/", 1)[0])
    except Exception:
        score_num = -1

    lines = [
        "        - Shipping address mismatch",
        "",
        f"          Customer: {((g.get('cname') or '').strip() or (g.get('b_name') or '').strip() or 'Unknown')} (odoo id: {g['cid']})",
        "",
        "          Sage Ship To:",
        "",
        f"            {g['s_name']}",
        f"            {g['s_street']}" + (f", {g['s_street2']}" if g["s_street2"] else ""),
        f"            {sage_line3}",
        "",
    ]
    if score_num <= 2:
        lines.extend([
            "          No best candidate found in Odoo",
            "",
        ])
    else:
        lines.extend([
            "          Best candidate in Odoo:",
            "",
            f"            {g['b_name']} [id: {g['b_id']}, type: {g['b_type']}, score: {g['b_score']}]",
            f"            {g['b_street']}" + (f", {g['b_street2']}" if g["b_street2"] else ""),
            f"            {odoo_line3}",
        ])
    return lines


def _shipping_mismatch_raw(part: str) -> Optional[str]:
    if "Missing exact shipping address match in Odoo " not in part:
        return None
    pattern = re.compile(
        r"Missing exact shipping address match in Odoo\s*\(\s*"
        r"customer_odoo_id=\d+;\s*"
        r"(?:customer_name='[^']*';\s*)?"
        r"sage_ship_to=name='[^']*',\s*street='[^']*',\s*street2='[^']*',\s*"
        r"city='[^']*',\s*state='[^']*',\s*zip='[^']*';\s*"
        r"best_candidate=id=\d+\s*type=\S+\s*score=\d+/\d+\s*"
        r"name='[^']*'\s*street='[^']*'\s*street2='[^']*'\s*"
        r"city='[^']*'\s*state='[^']*'\s*zip='[^']*'\s*\)"
    )
    m = pattern.search(part)
    return m.group(0) if m else None


def _pretty_shipping_created(part: str) -> Optional[List[str]]:
    if "Shipping address created on the fly" not in part:
        return None
    pattern = re.compile(
        r"Shipping address created on the fly\s*\("
        r"id=(?P<id>\d+);\s*"
        r"name='(?P<name>[^']*)';\s*"
        r"street='(?P<street>[^']*)';\s*"
        r"street2='(?P<street2>[^']*)';\s*"
        r"city='(?P<city>[^']*)';\s*"
        r"state='(?P<state>[^']*)';\s*"
        r"zip='(?P<zip>[^']*)';\s*"
        r"country='(?P<country>[^']*)'\)"
    )
    m = pattern.search(part)
    if not m:
        return None
    g = m.groupdict()
    line3 = f"{g['city']} - {g['state']} ({g['zip']})".strip()
    if g.get("country"):
        line3 += f" [{g['country']}]"
    return [
        "        - Shipping address created on the fly",
        "",
        f"          Odoo delivery id: {g['id']}",
        "",
        f"            {g['name']}",
        f"            {g['street']}" + (f", {g['street2']}" if g["street2"] else ""),
        f"            {line3}",
    ]


def _confirm_order_if_needed(client: OdooClient, order_id: int, target_date_order: str) -> bool:
    rows = client.search_read("sale.order", [("id", "=", order_id)], ["id", "state"], limit=1, offset=0)
    if not rows:
        return False
    state = (rows[0].get("state") or "").strip()
    if state in {"draft", "sent"}:
        client.models.execute_kw(
            client.db,
            client.uid,
            client.apikey,
            "sale.order",
            "action_confirm",
            [[order_id]],
        )
        # Odoo may set date_order to "now" during confirmation; restore Sage order date.
        if target_date_order:
            client.models.execute_kw(
                client.db,
                client.uid,
                client.apikey,
                "sale.order",
                "write",
                [[order_id], {"date_order": f"{target_date_order} 00:00:00"}],
            )
        return True
    return False


def _reopen_order_to_draft_if_needed(client: OdooClient, order_id: int) -> bool:
    rows = client.search_read("sale.order", [("id", "=", order_id)], ["id", "state"], limit=1, offset=0)
    if not rows:
        return False
    state = (rows[0].get("state") or "").strip()
    if state in {"draft", "sent"}:
        return True
    # Standard safe flow to edit a confirmed SO.
    client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "sale.order",
        "action_cancel",
        [[order_id]],
    )
    client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "sale.order",
        "action_draft",
        [[order_id]],
    )
    # Cleanup cancelled deliveries immediately so if repair fails later
    # the order does not keep stale cancelled pickings attached.
    _cleanup_cancelled_pickings_for_order(
        client=client,
        order_id=order_id,
        delete_when_no_active=True,
    )
    rows_after = client.search_read("sale.order", [("id", "=", order_id)], ["id", "state"], limit=1, offset=0)
    if not rows_after:
        return False
    return (rows_after[0].get("state") or "").strip() in {"draft", "sent"}


def _cleanup_cancelled_pickings_for_order(
    client: OdooClient,
    order_id: int,
    delete_when_no_active: bool = False,
) -> Tuple[int, int]:
    pickings = client.search_read(
        "stock.picking",
        [("sale_id", "=", int(order_id))],
        ["id", "state", "name"],
        limit=500,
        offset=0,
    )
    if not pickings:
        return 0, 0
    active = [p for p in pickings if str(p.get("state") or "").strip() != "cancel"]
    cancelled = [p for p in pickings if str(p.get("state") or "").strip() == "cancel"]
    if not cancelled:
        return 0, 0
    # Default behavior: cleanup only duplicates (there is at least one active delivery).
    # Optional behavior for repair flow: also cleanup when there is no active delivery.
    if not active and not delete_when_no_active:
        return 0, len(cancelled)
    deleted = 0
    kept = 0
    for p in cancelled:
        pid = int(p.get("id") or 0)
        if pid <= 0:
            continue
        try:
            client.models.execute_kw(
                client.db,
                client.uid,
                client.apikey,
                "stock.picking",
                "unlink",
                [[pid]],
            )
            deleted += 1
        except Exception:
            kept += 1
    return deleted, kept


def _delete_all_pickings_for_order(
    client: OdooClient,
    order_id: int,
) -> Tuple[int, int]:
    pickings = client.search_read(
        "stock.picking",
        [("sale_id", "=", int(order_id))],
        ["id", "state"],
        limit=500,
        offset=0,
    )
    if not pickings:
        return 0, 0
    deleted = 0
    kept = 0
    for p in pickings:
        pid = int(p.get("id") or 0)
        if pid <= 0:
            continue
        state = str(p.get("state") or "").strip()
        try:
            if state != "cancel":
                try:
                    client.models.execute_kw(
                        client.db,
                        client.uid,
                        client.apikey,
                        "stock.picking",
                        "action_cancel",
                        [[pid]],
                    )
                except Exception:
                    pass
            client.models.execute_kw(
                client.db,
                client.uid,
                client.apikey,
                "stock.picking",
                "unlink",
                [[pid]],
            )
            deleted += 1
        except Exception:
            kept += 1
    return deleted, kept


def _diff_order_sig(current_sig: Dict[str, object], desired_sig: Dict[str, object]) -> List[str]:
    reasons: List[str] = []
    current_base = current_sig.get("base") or {}
    desired_base = desired_sig.get("base") or {}
    for key in [
        "partner_id",
        "date_order",
        "validity_date",
        "commitment_date",
        "pricelist_id",
        "payment_term_id",
        "user_id",
        "team_id",
        "partner_invoice_id",
        "partner_shipping_id",
    ]:
        if current_base.get(key) != desired_base.get(key):
            reasons.append(key)
    current_lines = current_sig.get("lines") or []
    desired_lines = desired_sig.get("lines") or []
    if current_lines != desired_lines:
        reasons.append("order_lines")
    return reasons


def _existing_order_skus(client: OdooClient, order_id: int) -> Dict[str, float]:
    lines = client.search_read(
        "sale.order.line",
        [["order_id", "=", order_id], ["display_type", "=", False]],
        ["product_id", "product_uom_qty"],
        limit=5000,
        offset=0,
    )
    product_ids = [
        int((row.get("product_id") or [0])[0])
        for row in lines
        if isinstance(row.get("product_id"), list) and row.get("product_id")
    ]
    sku_by_pid: Dict[int, str] = {}
    if product_ids:
        for i in range(0, len(product_ids), 500):
            chunk = product_ids[i:i + 500]
            products = client.search_read(
                "product.product",
                [["id", "in", chunk]],
                ["id", "default_code"],
                limit=5000,
                offset=0,
            )
            for p in products:
                sku_by_pid[int(p.get("id") or 0)] = str(p.get("default_code") or "").strip().upper()
    out: Dict[str, float] = defaultdict(float)
    for row in lines:
        pid = int((row.get("product_id") or [0])[0]) if isinstance(row.get("product_id"), list) and row.get("product_id") else 0
        sku = (sku_by_pid.get(pid) or "").strip().upper()
        qty = float(row.get("product_uom_qty") or 0.0)
        out[sku] += qty
    return out


def _pretty_sage_source_rows(part: str) -> Optional[List[str]]:
    marker = "Sage source rows:"
    if marker not in part:
        return None
    raw = part.split(marker, 1)[1].strip()
    if not raw:
        return None
    chunks = [c.strip() for c in raw.split("__ROWSEP__") if c.strip()]
    if not chunks:
        return None
    out = ["        - Sage source rows:"]
    for chunk in chunks:
        m = re.match(
            r"^(?P<idx>\d+)\s*\|\s*(?P<desc>.*?)\s*x(?P<qty>[^|]+)\s*\|\s*(?P<amount>.+)$",
            chunk,
        )
        if m:
            idx = m.group("idx").strip()
            desc = m.group("desc").strip()
            qty = m.group("qty").strip()
            amount = m.group("amount").strip()
            out.append(f"          #{idx}  {desc} x{qty} = {amount}")
        else:
            out.append(f"          {chunk}")
    return out


def run(args: argparse.Namespace) -> int:
    if args.content_repair and not args.content_verify:
        # Repair logic is implemented inside content-verify mode.
        args.content_verify = True
        print("INFO: --content-repair enables --content-verify automatically")
    if not args.confirm:
        print("INFO: confirmation disabled (orders remain in QUOTE/draft unless --confirm is provided)")

    max_orders, start_offset = _parse_limit_offset(args.limit, args.offset)
    continue_on_error = bool(args.allow_partial or args.skip)
    env = load_env_file(args.env_file)
    url = profile_env(env, args.profile, "URL")
    db = profile_env(env, args.profile, "DB")
    user = profile_env(env, args.profile, "USER")
    apikey = profile_env(env, args.profile, "APIKEY")
    if not (url and db and user and apikey):
        print(f"ERROR: missing Odoo credentials for profile {args.profile}")
        return 2

    client = OdooClient(url=url, db=db, user=user, apikey=apikey)
    customers_map = load_customers_map(args.customers_sync)
    products_map_master = load_products_map_from_masters(args.items_master, client)
    # Strict rule: sales orders mapping must come only from Sage/Odoo masters.
    products_map = dict(products_map_master)
    if (args.products_sync or "").strip():
        print("INFO: --products-sync is ignored by design")
    if (args.items_odoo or "").strip():
        print("INFO: --items-odoo is ignored by design (live Odoo lookup)")
    if products_map_master:
        print("INFO: products mapping source=master_sage_odoo " f"(master={len(products_map_master)})")
    else:
        print("ERROR: products mapping source is empty (items.csv + items_odoo.csv required)")
        return 2
    if (args.employees_sync or "").strip():
        print("INFO: --employees-sync is ignored by design")
    employees_map = load_employees_map(args.employees_master)
    if (args.load or "").strip():
        headers, lines_by_postorder = load_order_data_auto(args.root_dir, args.load)
    else:
        headers, lines_by_postorder = load_order_data(args.headers_path, args.lines_path)
    # Process in true chronological order (oldest first), not CSV physical order.
    def _header_sort_key(h: Dict[str, str]):
        raw_date = (h.get("TransactionDate") or "").strip()
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d")
        except ValueError:
            dt = datetime.max
        ref = (h.get("Reference") or "").strip()
        post = (h.get("PostOrder") or "").strip()
        ref_num = int(ref) if ref.isdigit() else 10**18
        post_num = int(post) if post.isdigit() else 10**18
        return (dt, ref_num, post_num)

    headers.sort(key=_header_sort_key)
    existing_orders_by_ref: Dict[str, Dict[str, object]] = {}
    gap_missing_flags: List[bool] = [False] * len(headers)
    gap_cut_index: Optional[int] = None
    if args.gaps:
        refs_for_gap = [
            (h.get("Reference") or "").strip()
            for h in headers
            if (h.get("Reference") or "").strip()
        ]
        existing_orders_by_ref = _fetch_existing_sale_orders_by_name(client, refs_for_gap)
        for idx, h in enumerate(headers):
            ref = (h.get("Reference") or "").strip()
            gap_missing_flags[idx] = bool(ref and ref not in existing_orders_by_ref)

        suffix_all_missing: List[bool] = [False] * len(headers)
        all_missing = True
        for idx in range(len(headers) - 1, -1, -1):
            all_missing = all_missing and gap_missing_flags[idx]
            suffix_all_missing[idx] = all_missing
        for idx, is_missing in enumerate(gap_missing_flags):
            if is_missing and suffix_all_missing[idx]:
                gap_cut_index = idx
                break

        missing_total = sum(1 for v in gap_missing_flags if v)
        print(
            "INFO: gaps mode "
            f"(headers={len(headers)}, missing_in_odoo={missing_total}, "
            f"cut_index={'none' if gap_cut_index is None else gap_cut_index + 1})"
        )

    terms_map = find_payment_terms(client)
    sales_taxes_by_code = find_sales_taxes_by_code(client)
    users_all = client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "res.users",
        "search_read",
        [[]],
        {"fields": ["id", "name", "login", "active"], "limit": 10000, "context": {"active_test": False}},
    )
    user_by_login = {
        (u.get("login") or "").strip().lower(): u
        for u in users_all
        if (u.get("login") or "").strip()
    }
    user_by_name_norm = {
        _norm_text(u.get("name") or ""): u
        for u in users_all
        if (u.get("name") or "").strip()
    }
    team_members_all = client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "crm.team.member",
        "search_read",
        [[]],
        {"fields": ["crm_team_id", "user_id"], "limit": 100000, "context": {"active_test": False}},
    )
    team_by_user_id: Dict[int, int] = {}
    for tm in team_members_all:
        user_rel = tm.get("user_id") or []
        team_rel = tm.get("crm_team_id") or []
        uid = int(user_rel[0]) if isinstance(user_rel, list) and user_rel else 0
        tid = int(team_rel[0]) if isinstance(team_rel, list) and team_rel else 0
        if uid <= 0 or tid <= 0:
            continue
        if uid not in team_by_user_id:
            team_by_user_id[uid] = tid
    generic_sales_team_id = 0
    generic_sales_team = client.search_read(
        "crm.team",
        [("name", "=", "Sales")],
        ["id", "name"],
        limit=1,
        offset=0,
    )
    if generic_sales_team:
        generic_sales_team_id = int(generic_sales_team[0].get("id") or 0)
    freight_variant_id = int(args.freight_variant_id or 0)
    if freight_variant_id <= 0:
        wanted_name = (args.freight_product_name or "Freight").strip()
        if wanted_name:
            candidates = client.search_read(
                "product.product",
                [("name", "=", wanted_name)],
                ["id", "name", "active"],
                limit=20,
                offset=0,
            )
            if not candidates:
                candidates = client.search_read(
                    "product.product",
                    [("name", "ilike", wanted_name)],
                    ["id", "name", "active"],
                    limit=20,
                    offset=0,
                )
            if candidates:
                active = [c for c in candidates if bool(c.get("active", True))]
                pick = active[0] if active else candidates[0]
                freight_variant_id = int(pick.get("id") or 0)
    concept_variant_id = int(args.concept_variant_id or 0)
    if concept_variant_id <= 0:
        concept_name = (args.concept_product_name or "MISC CHARGE").strip()
        if concept_name:
            candidates = client.search_read(
                "product.product",
                [("name", "=", concept_name)],
                ["id", "name", "active"],
                limit=20,
                offset=0,
            )
            if not candidates:
                candidates = client.search_read(
                    "product.product",
                    [("name", "ilike", concept_name)],
                    ["id", "name", "active"],
                    limit=20,
                    offset=0,
                )
            if candidates:
                active = [c for c in candidates if bool(c.get("active", True))]
                pick = active[0] if active else candidates[0]
                concept_variant_id = int(pick.get("id") or 0)
    address_parity_maps = load_address_parity_maps(args.root_dir)
    country_state_indexes = fetch_country_state_indexes(client)

    logs: List[Dict[str, str]] = []
    created = 0
    seen_candidates = 0
    processed_index = start_offset
    processed_count = 0
    references_filter = _parse_reference_filter(args.reference)
    ignore_raw = (args.ignore_references or "").strip()
    if not ignore_raw:
        ignore_raw = (
            get_env_value(env, f"{args.profile}_IGNORE_REFERENCES")
            or get_env_value(env, "SALES_ORDER_IGNORE_REFERENCES")
            or "363903"
        )
    ignored_references = _parse_reference_filter(ignore_raw)
    current_no_sales_rep = False
    invoice_partner_cache: Dict[int, int] = {}
    shipping_partner_cache: Dict[Tuple[int, str, str, str, str, str, str, bool], Tuple[int, bool, str]] = {}
    product_variant_exists_cache: Dict[int, bool] = {}
    dropshipper_cache: Dict[int, bool] = {}
    category_name_cache: Dict[int, str] = {}

    def _customer_has_dropshipper_tag(partner_id: int) -> bool:
        if partner_id in dropshipper_cache:
            return dropshipper_cache[partner_id]
        rows = client.search_read(
            "res.partner",
            [("id", "=", int(partner_id))],
            ["id", "category_id"],
            limit=1,
            offset=0,
        )
        has_tag = False
        if rows:
            category_ids = rows[0].get("category_id") or []
            for cat_id in category_ids:
                try:
                    cid = int(cat_id)
                except Exception:
                    continue
                if cid in category_name_cache:
                    cat_name = category_name_cache[cid]
                else:
                    cat_rows = client.search_read(
                        "res.partner.category",
                        [("id", "=", cid)],
                        ["id", "name"],
                        limit=1,
                        offset=0,
                    )
                    cat_name = str(cat_rows[0].get("name") or "").strip().lower() if cat_rows else ""
                    category_name_cache[cid] = cat_name
                if cat_name == "dropshipper":
                    has_tag = True
                    break
        dropshipper_cache[partner_id] = has_tag
        return has_tag

    def push_log(entry: Dict[str, str]) -> None:
        nonlocal processed_index, processed_count, current_no_sales_rep
        if "NoSalesRep" not in entry:
            entry["NoSalesRep"] = "1" if current_no_sales_rep else ""
        logs.append(entry)
        processed_count += 1
        processed_index += 1
        _print_order_progress(entry, processed_index)

    def _sage_rows_preview(rows: List[Dict[str, str]], limit: int = 8) -> str:
        parts: List[str] = []
        shown = 0
        visible_total = 0
        for i, row in enumerate(rows, start=1):
            rec = str(row.get("ItemRecordNumber") or "").strip()
            qty = str(row.get("Quantity") or "").strip()
            amount = str(row.get("Amount") or "").strip()
            desc = str(row.get("RowDescription") or "").strip()
            # Sage sometimes stores a hidden summary/header row (item=0, qty=0) that
            # does not appear on the visible sales order UI. Skip it in logs.
            if rec in {"", "0"} and qty in {"", "0", "0.0", "0,0"}:
                continue
            visible_total += 1
            if shown >= limit:
                continue
            qty_out = qty or "-"
            amount_out = amount or "-"
            desc_out = desc or "''"
            parts.append(f"{i} | {desc_out} x{qty_out} | {amount_out}")
            shown += 1
        extra = max(0, visible_total - shown)
        if extra > 0:
            parts.append(f"... (+{extra} more)")
        return " __ROWSEP__ ".join(parts)

    for header_index, h in enumerate(headers):
        if max_orders is not None and processed_count >= max_orders:
            break
        post_order = (h.get("PostOrder") or "").strip()
        reference = (h.get("Reference") or "").strip()
        if reference and reference in ignored_references:
            continue
        if references_filter and reference not in references_filter:
            continue
        if args.gaps:
            if args.reference:
                # If a specific reference is requested, honor it even in gaps mode.
                pass
            else:
                if gap_cut_index is not None and header_index >= gap_cut_index:
                    print(
                        f"INFO: gaps mode stop at ordinal {header_index + 1} "
                        "(start of trailing never-imported block)."
                    )
                    break
                if not gap_missing_flags[header_index]:
                    continue
        customer_record = (h.get("CustVendId") or "").strip()
        transaction_date = (h.get("TransactionDate") or "").strip()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        source_lines_raw = lines_by_postorder.get(post_order, [])
        source_lines = filter_non_importable_sage_rows(source_lines_raw, h)
        header_main_amount = parse_decimal(h.get("MainAmount") or "")
        prepared_info = build_order_lines(
            source_lines,
            products_map,
            freight_variant_id=freight_variant_id,
            concept_variant_id=concept_variant_id,
            header_main_amount=header_main_amount,
        )
        prepared_lines = prepared_info["lines"]
        source_row_count = int(prepared_info.get("source_row_count") or 0)
        source_product_row_count = int(prepared_info.get("source_product_row_count") or 0)
        variant_debug_rows = prepared_info.get("variant_debug_rows") or []
        has_product_lines = int(prepared_info.get("prepared_product_line_count") or 0) > 0
        tax_total_source = float(prepared_info.get("tax_total_source") or 0.0)
        tax_codes = [str(c).strip().upper() for c in (prepared_info.get("tax_authority_codes") or []) if str(c).strip()]
        tax_code = tax_codes[0] if tax_codes else ""
        tax_id = sales_taxes_by_code.get(tax_code) if tax_code else None
        if tax_total_source > 0 and tax_id:
            for line_vals in prepared_lines:
                if line_vals.get("display_type"):
                    continue
                line_vals["tax_ids"] = [(6, 0, [int(tax_id)])]
        ship_via = (h.get("ShipVia") or "").strip()
        has_shipping_line = bool(prepared_info.get("has_shipping_line"))
        shipping_line_indexes = prepared_info.get("shipping_line_indexes") or []
        if ship_via:
            if has_shipping_line:
                for idx in shipping_line_indexes:
                    if 0 <= idx < len(prepared_lines):
                        base_name = str(prepared_lines[idx].get("name") or "").strip()
                        if "Shipping Method:" not in base_name:
                            prepared_lines[idx]["name"] = f"{base_name} | Shipping Method: {ship_via}" if base_name else f"Shipping Method: {ship_via}"
            else:
                prepared_lines.append({
                    "display_type": "line_note",
                    "name": f"Shipping Method: {ship_via}",
                })
        allow_note_only_zero_total = (not has_product_lines) and (abs(header_main_amount) <= 0.02) and bool(prepared_lines)
        if not has_product_lines and not allow_note_only_zero_total:
            source_preview = _sage_rows_preview(source_lines_raw)
            push_log({
                "Timestamp": now,
                "Status": "ERROR",
                "Reference": reference,
                "PostOrder": post_order,
                "TransactionDate": transaction_date,
                "CustomerRecordNumber": customer_record,
                "CustomerOdooId": "",
                "OrderOdooId": "",
                "OrderState": "",
                "LineCount": "0",
                "Details": (
                    "No valid order lines after product mapping "
                    f"(source_rows={source_row_count}, source_product_rows={source_product_row_count}, "
                    f"skipped_rows={len(prepared_info.get('skipped_reasons') or [])})"
                    + (f"; Sage source rows: {source_preview}" if source_preview else "")
                ),
            })
            if not continue_on_error:
                break
            continue

        stale_variant_details: List[str] = []
        for dbg in variant_debug_rows:
            try:
                vid = int(str(dbg.get("product_id") or "0"))
            except ValueError:
                vid = 0
            if vid <= 0:
                continue
            if vid not in product_variant_exists_cache:
                found = client.search_read(
                    "product.product",
                    [("id", "=", vid)],
                    ["id"],
                    limit=1,
                    offset=0,
                )
                product_variant_exists_cache[vid] = bool(found)
            if not product_variant_exists_cache.get(vid):
                stale_variant_details.append(
                    f"variant_id={vid} item_record={dbg.get('item_record')} item_id={dbg.get('item_id')} row='{dbg.get('row_desc')}'"
                )
        customer_sync = customers_map.get(customer_record) or {}
        customer_odoo_id = (customer_sync.get("OdooId") or "").strip()
        customer_odoo_name = (
            (customer_sync.get("OdooName") or "").strip()
            or (customer_sync.get("CustomerName") or "").strip()
        )
        if not customer_odoo_id:
            push_log({
                "Timestamp": now,
                "Status": "ERROR",
                "Reference": reference,
                "PostOrder": post_order,
                "TransactionDate": transaction_date,
                "CustomerRecordNumber": customer_record,
                "CustomerOdooId": "",
                "OrderOdooId": "",
                "OrderState": "",
                "LineCount": str(len(prepared_lines)),
                "Details": "Missing customer mapping in Odoo",
            })
            if not continue_on_error:
                break
            continue

        seen_candidates += 1
        if seen_candidates <= start_offset:
            continue

        header_total = round(parse_decimal(h.get("MainAmount") or ""), 2)
        source_total = prepared_info["source_total"]
        prepared_total = prepared_info["prepared_total"]
        prepared_total_with_tax = round(prepared_total + tax_total_source, 2)
        skipped_reasons = prepared_info["skipped_reasons"]
        total_mismatch = abs(header_total - prepared_total_with_tax) > 0.02
        warning_parts = []
        info_parts: List[str] = []
        if total_mismatch:
            warning_parts.append(
                f"Total mismatch header={header_total:.2f} prepared={prepared_total_with_tax:.2f} "
                f"(lines={prepared_total:.2f}, source_tax={tax_total_source:.2f}) source_lines={source_total:.2f}"
            )
        if skipped_reasons:
            warning_parts.append("Skipped lines: " + " | ".join(skipped_reasons[:5]))

        if args.gaps and not args.reference:
            existing = existing_orders_by_ref.get(reference)
            exists = [existing] if existing else []
        else:
            exists = client.search_read(
                "sale.order",
                [("name", "=", reference)],
                ["id", "name", "state", "partner_shipping_id", "pricelist_id"],
                limit=1,
                offset=0,
            )
        order_state = str(exists[0].get("state") or "").strip() if exists else ""
        existing_shipping_id = 0
        if exists:
            raw_existing_shipping = exists[0].get("partner_shipping_id")
            if isinstance(raw_existing_shipping, list) and raw_existing_shipping:
                existing_shipping_id = int(raw_existing_shipping[0] or 0)
            elif isinstance(raw_existing_shipping, int):
                existing_shipping_id = raw_existing_shipping
        emp_record_for_verify = (h.get("EmpRecordNumber") or "").strip()
        emp_for_verify = employees_map.get(emp_record_for_verify) or {}
        sales_login_for_verify, sales_user_for_verify = resolve_sales_user_live(
            emp_for_verify,
            user_by_login,
            user_by_name_norm,
        )
        desired_user_id_for_verify = int(sales_user_for_verify["id"]) if sales_user_for_verify else 0
        desired_team_id_for_verify = int(team_by_user_id.get(desired_user_id_for_verify) or 0) if desired_user_id_for_verify else 0
        if desired_user_id_for_verify and not desired_team_id_for_verify and generic_sales_team_id:
            desired_team_id_for_verify = generic_sales_team_id

        if args.content_verify:
            def _force_quote_on_error_if_needed() -> tuple[str, str]:
                if not (args.content_repair and exists):
                    return order_state, ""
                oid = int(exists[0].get("id") or 0)
                if oid <= 0:
                    return order_state, ""
                try:
                    if _reopen_order_to_draft_if_needed(client, oid):
                        return "draft", "ORDER > CANCEL > QUOTE"
                except Exception:
                    pass
                return order_state, ""

            if not exists:
                push_log({
                    "Timestamp": now,
                    "Status": "ERROR",
                    "Reference": reference,
                    "PostOrder": post_order,
                    "TransactionDate": transaction_date,
                    "CustomerRecordNumber": customer_record,
                    "CustomerOdooId": customer_odoo_id,
                    "OrderOdooId": "",
                    "OrderState": "",
                    "LineCount": str(len(prepared_lines)),
                    "Details": "Order not found in Odoo for content verification",
                })
                if not continue_on_error:
                    break
                continue
            identity_errors: List[str] = []
            if not emp_record_for_verify:
                identity_errors.append("Missing EmpRecordNumber in Sage order")
            elif emp_record_for_verify != "0":
                if not (emp_for_verify and (emp_for_verify.get("EmployeeID") or "").strip()):
                    identity_errors.append(f"Missing employee mapping for EmpRecordNumber {emp_record_for_verify}")
                elif not sales_user_for_verify:
                    identity_errors.append(f"Missing Odoo user for employee login {sales_login_for_verify}")
                elif not desired_team_id_for_verify and not generic_sales_team_id:
                    identity_errors.append(f"Missing sales team for Odoo user {sales_user_for_verify.get('name')}")

            desired_pricelist_id = int((customer_sync.get("OdooPricelistId") or "0").strip() or 0)
            expected_skus: Dict[str, float] = defaultdict(float)
            missing_item_records: List[str] = []
            for line in source_lines:
                item_record = (line.get("ItemRecordNumber") or "").strip()
                if not item_record or item_record == "0":
                    continue
                qty = parse_decimal(line.get("Quantity") or "")
                if qty <= 0:
                    continue
                product_sync = products_map.get(item_record) or {}
                sku = (product_sync.get("ItemID") or "").strip().upper()
                if not sku:
                    missing_item_records.append(item_record)
                    continue
                expected_skus[sku] += qty

            order_id = int(exists[0]["id"])
            forced_cleanup_note = ""
            if args.content_repair:
                # Requested operational mode: always move to QUOTE and remove
                # all existing delivery orders before validating/repairing.
                if not _reopen_order_to_draft_if_needed(client, order_id):
                    push_log({
                        "Timestamp": now,
                        "Status": "ERROR",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": str(order_id),
                        "OrderState": order_state,
                        "LineCount": str(len(prepared_lines)),
                        "Details": "Could not force order to QUOTE before content repair",
                    })
                    if not continue_on_error:
                        break
                    created += 1
                    continue
                deleted_pickings, kept_pickings = _delete_all_pickings_for_order(client, order_id)
                forced_cleanup_note = (
                    f"Forced QUOTE + deleted deliveries={deleted_pickings}"
                    + (f" (kept={kept_pickings})" if kept_pickings else "")
                )
            actual_skus = _existing_order_skus(client, order_id)
            order_sig = _existing_order_sig(client, order_id)
            raw_pl = exists[0].get("pricelist_id")
            current_pricelist_id = 0
            if isinstance(raw_pl, list) and raw_pl:
                current_pricelist_id = int(raw_pl[0] or 0)
            elif isinstance(raw_pl, int):
                current_pricelist_id = int(raw_pl or 0)
            pricelist_mismatch = bool(desired_pricelist_id and current_pricelist_id != desired_pricelist_id)
            expected_client_order_ref = resolve_customer_reference_from_sage(h)
            current_client_order_ref = str(order_sig.get("base", {}).get("client_order_ref") or "").strip()
            client_order_ref_mismatch = expected_client_order_ref != current_client_order_ref
            expected_date_order = transaction_date.strip()
            expected_validity_date = expected_date_order
            expected_commitment_date = (h.get("ShipByDate") or "").strip() or expected_date_order
            current_date_order = _fmt_date(order_sig.get("base", {}).get("date_order"))
            current_validity_date = _fmt_date(order_sig.get("base", {}).get("validity_date"))
            current_commitment_date = _fmt_date(order_sig.get("base", {}).get("commitment_date"))
            date_order_mismatch = expected_date_order != current_date_order
            validity_date_mismatch = expected_validity_date != current_validity_date
            commitment_date_mismatch = expected_commitment_date != current_commitment_date
            current_user_id = int(order_sig.get("base", {}).get("user_id") or 0)
            current_team_id = int(order_sig.get("base", {}).get("team_id") or 0)
            if emp_record_for_verify == "0":
                salesperson_mismatch = current_user_id != 0
                sales_team_mismatch = current_team_id != 0
            else:
                salesperson_mismatch = bool(desired_user_id_for_verify and current_user_id != desired_user_id_for_verify)
                sales_team_mismatch = bool(desired_team_id_for_verify and current_team_id != desired_team_id_for_verify)

            expected_line_sigs = [_line_sig_from_prepared(l) for l in prepared_lines]
            actual_line_sigs = order_sig.get("lines", []) or []
            line_content_mismatch = expected_line_sigs != actual_line_sigs

            missing: List[str] = []
            extra: List[str] = []
            all_skus = sorted(set(expected_skus.keys()) | set(actual_skus.keys()))
            for sku in all_skus:
                exp_qty = float(expected_skus.get(sku, 0.0))
                got_qty = float(actual_skus.get(sku, 0.0))
                if abs(exp_qty - got_qty) <= 0.0001:
                    continue
                if exp_qty > got_qty:
                    missing.append(f"{sku or '<blank>'} exp={exp_qty:g} got={got_qty:g}")
                else:
                    extra.append(f"{sku or '<blank>'} exp={exp_qty:g} got={got_qty:g}")

            has_content_issues = bool(
                missing_item_records
                or missing
                or extra
                or pricelist_mismatch
                or client_order_ref_mismatch
                or date_order_mismatch
                or validity_date_mismatch
                or commitment_date_mismatch
                or salesperson_mismatch
                or sales_team_mismatch
                or identity_errors
                or line_content_mismatch
            )
            if has_content_issues and args.content_repair and not args.dry_run:
                try:
                    if not _reopen_order_to_draft_if_needed(client, order_id):
                        raise RuntimeError("Could not move order to draft for repair")

                    # Same guardrails as create/update flow: if any critical check fails,
                    # stop before confirmation and leave the order in QUOTE for manual review.
                    repair_blockers: List[str] = []
                    if skipped_reasons:
                        preview = " | ".join(skipped_reasons[:5])
                        suffix = "" if len(skipped_reasons) <= 5 else f" | ... (+{len(skipped_reasons)-5} more)"
                        repair_blockers.append(f"Missing product mapping: {preview}{suffix}")
                    if total_mismatch:
                        repair_blockers.append(
                            f"Order total mismatch (header={header_total:.2f}, prepared={prepared_total_with_tax:.2f}, "
                            f"lines={prepared_total:.2f}, source_tax={tax_total_source:.2f}, source_lines={source_total:.2f}; "
                            f"source_rows={source_row_count}, source_product_rows={source_product_row_count}, prepared_rows={len(prepared_lines)})"
                        )
                    if stale_variant_details:
                        preview = " | ".join(stale_variant_details[:5])
                        suffix = "" if len(stale_variant_details) <= 5 else f" | ... (+{len(stale_variant_details)-5} more)"
                        repair_blockers.append(
                            "Products mapped to deleted/missing Odoo variants: " + preview + suffix
                        )
                    if tax_total_source > 0 and not tax_id:
                        repair_blockers.append(
                            f"Missing sales tax mapping for Sage TaxAuthorityCode {tax_code or '(blank)'}"
                        )
                    if repair_blockers:
                        order_path = "QUOTE"
                        if order_state not in {"draft", "sent"}:
                            order_path = "ORDER > CANCEL > QUOTE"
                        push_log({
                            "Timestamp": now,
                            "Status": "ERROR",
                            "Reference": reference,
                            "PostOrder": post_order,
                            "TransactionDate": transaction_date,
                            "CustomerRecordNumber": customer_record,
                            "CustomerOdooId": customer_odoo_id,
                            "OrderOdooId": str(order_id),
                            "OrderState": "draft",
                            "OrderPath": order_path,
                            "LineCount": str(len(prepared_lines)),
                            "Details": "Repair blocked before confirmation (left as QUOTE): " + "; ".join(repair_blockers),
                        })
                        if not continue_on_error:
                            break
                        created += 1
                        continue

                    repair_vals: Dict[str, object] = {}
                    repaired_fields: List[str] = []
                    if missing or extra or line_content_mismatch:
                        repair_vals["order_line"] = [(5, 0, 0)] + [(0, 0, line_vals) for line_vals in prepared_lines]
                        repaired_fields.append("order_lines")
                    if pricelist_mismatch:
                        repair_vals["pricelist_id"] = int(desired_pricelist_id)
                        repaired_fields.append("pricelist_id")
                    if client_order_ref_mismatch:
                        repair_vals["client_order_ref"] = expected_client_order_ref
                        repaired_fields.append("client_order_ref")
                    if date_order_mismatch:
                        repair_vals["date_order"] = expected_date_order
                        repaired_fields.append("date_order")
                    if validity_date_mismatch:
                        repair_vals["validity_date"] = expected_validity_date
                        repaired_fields.append("validity_date")
                    if commitment_date_mismatch:
                        repair_vals["commitment_date"] = expected_commitment_date
                        repaired_fields.append("commitment_date")
                    if salesperson_mismatch:
                        repair_vals["user_id"] = desired_user_id_for_verify if emp_record_for_verify != "0" else False
                        repaired_fields.append("user_id")
                    if sales_team_mismatch:
                        repair_vals["team_id"] = desired_team_id_for_verify if emp_record_for_verify != "0" else False
                        repaired_fields.append("team_id")

                    if not repair_vals:
                        if identity_errors:
                            order_path = ""
                            if order_state not in {"draft", "sent"}:
                                order_path = "ORDER > CANCEL > QUOTE"
                            push_log({
                                "Timestamp": now,
                                "Status": "ERROR",
                                "Reference": reference,
                                "PostOrder": post_order,
                                "TransactionDate": transaction_date,
                                "CustomerRecordNumber": customer_record,
                                "CustomerOdooId": customer_odoo_id,
                                "OrderOdooId": str(order_id),
                                "OrderState": "draft",
                                "OrderPath": order_path,
                                "LineCount": str(len(prepared_lines)),
                                "Details": "; ".join(identity_errors),
                            })
                            if not continue_on_error:
                                break
                            created += 1
                            continue
                        raise RuntimeError("No repairable fields found for content mismatch")

                    client.models.execute_kw(
                        client.db,
                        client.uid,
                        client.apikey,
                        "sale.order",
                        "write",
                        [[order_id], repair_vals],
                    )
                    confirmed = False
                    if args.confirm and not identity_errors:
                        confirmed = _confirm_order_if_needed(client, order_id, transaction_date.strip())

                    # Re-check after repair to guarantee content is aligned.
                    actual_skus_after = _existing_order_skus(client, order_id)
                    missing_after: List[str] = []
                    extra_after: List[str] = []
                    all_skus_after = sorted(set(expected_skus.keys()) | set(actual_skus_after.keys()))
                    for sku in all_skus_after:
                        exp_qty = float(expected_skus.get(sku, 0.0))
                        got_qty = float(actual_skus_after.get(sku, 0.0))
                        if abs(exp_qty - got_qty) <= 0.0001:
                            continue
                        if exp_qty > got_qty:
                            missing_after.append(f"{sku or '<blank>'} exp={exp_qty:g} got={got_qty:g}")
                        else:
                            extra_after.append(f"{sku or '<blank>'} exp={exp_qty:g} got={got_qty:g}")

                    row_after = client.search_read(
                        "sale.order",
                        [("id", "=", order_id)],
                        ["id", "state", "pricelist_id", "client_order_ref"],
                        limit=1,
                        offset=0,
                    )
                    state_after = order_state
                    pricelist_after = current_pricelist_id
                    if row_after:
                        state_after = str(row_after[0].get("state") or "").strip()
                        pl_after = row_after[0].get("pricelist_id")
                        if isinstance(pl_after, list) and pl_after:
                            pricelist_after = int(pl_after[0] or 0)
                        elif isinstance(pl_after, int):
                            pricelist_after = int(pl_after or 0)

                    pricelist_after_mismatch = bool(
                        desired_pricelist_id and pricelist_after != desired_pricelist_id
                    )
                    client_order_ref_after = current_client_order_ref
                    if row_after:
                        client_order_ref_after = str(row_after[0].get("client_order_ref") or "").strip()
                    client_order_ref_after_mismatch = expected_client_order_ref != client_order_ref_after
                    order_sig_after = _existing_order_sig(client, order_id)
                    date_order_after = _fmt_date(order_sig_after.get("base", {}).get("date_order"))
                    validity_date_after = _fmt_date(order_sig_after.get("base", {}).get("validity_date"))
                    commitment_date_after = _fmt_date(order_sig_after.get("base", {}).get("commitment_date"))
                    date_order_after_mismatch = expected_date_order != date_order_after
                    validity_date_after_mismatch = expected_validity_date != validity_date_after
                    commitment_date_after_mismatch = expected_commitment_date != commitment_date_after
                    user_after = int(order_sig_after.get("base", {}).get("user_id") or 0)
                    team_after = int(order_sig_after.get("base", {}).get("team_id") or 0)
                    if emp_record_for_verify == "0":
                        salesperson_after_mismatch = user_after != 0
                        sales_team_after_mismatch = team_after != 0
                    else:
                        salesperson_after_mismatch = bool(desired_user_id_for_verify and user_after != desired_user_id_for_verify)
                        sales_team_after_mismatch = bool(desired_team_id_for_verify and team_after != desired_team_id_for_verify)
                    line_content_after_mismatch = (order_sig_after.get("lines", []) or []) != expected_line_sigs

                    if (
                        missing_after
                        or extra_after
                        or pricelist_after_mismatch
                        or client_order_ref_after_mismatch
                        or date_order_after_mismatch
                        or validity_date_after_mismatch
                        or commitment_date_after_mismatch
                        or salesperson_after_mismatch
                        or sales_team_after_mismatch
                        or identity_errors
                        or line_content_after_mismatch
                    ):
                        order_path = ""
                        if order_state not in {"draft", "sent"}:
                            order_path = "ORDER > CANCEL > QUOTE"
                        details = []
                        if missing_after:
                            details.append(
                                "Missing in Odoo after repair: "
                                + " | ".join(missing_after[:8])
                                + (f" | ... (+{len(missing_after)-8} more)" if len(missing_after) > 8 else "")
                            )
                        if extra_after:
                            details.append(
                                "Extra in Odoo after repair: "
                                + " | ".join(extra_after[:8])
                                + (f" | ... (+{len(extra_after)-8} more)" if len(extra_after) > 8 else "")
                            )
                        if pricelist_after_mismatch:
                            details.append(
                                f"Pricelist mismatch after repair (expected={desired_pricelist_id}, got={pricelist_after})"
                            )
                        if client_order_ref_after_mismatch:
                            details.append(
                                f"Customer PO mismatch after repair (expected='{expected_client_order_ref}', got='{client_order_ref_after}')"
                            )
                        if date_order_after_mismatch:
                            details.append(
                                f"date_order mismatch after repair (expected={expected_date_order}, got={date_order_after})"
                            )
                        if validity_date_after_mismatch:
                            details.append(
                                f"validity_date mismatch after repair (expected={expected_validity_date}, got={validity_date_after})"
                            )
                        if commitment_date_after_mismatch:
                            details.append(
                                f"commitment_date mismatch after repair (expected={expected_commitment_date}, got={commitment_date_after})"
                            )
                        if salesperson_after_mismatch:
                            details.append(f"Salesperson mismatch after repair (expected={desired_user_id_for_verify}, got={user_after})")
                        if sales_team_after_mismatch:
                            details.append(f"Sales Team mismatch after repair (expected={desired_team_id_for_verify}, got={team_after})")
                        if line_content_after_mismatch:
                            details.append("Order line descriptions/content mismatch after repair")
                        details.extend(identity_errors)
                        push_log({
                            "Timestamp": now,
                            "Status": "ERROR",
                            "Reference": reference,
                            "PostOrder": post_order,
                            "TransactionDate": transaction_date,
                            "CustomerRecordNumber": customer_record,
                            "CustomerOdooId": customer_odoo_id,
                            "OrderOdooId": str(order_id),
                            "OrderState": state_after,
                            "OrderPath": order_path,
                            "LineCount": str(len(prepared_lines)),
                            "Details": "; ".join(details),
                        })
                        if not continue_on_error:
                            break
                    else:
                        mismatch_labels: List[str] = []
                        if missing:
                            mismatch_labels.append("missing_sku_lines")
                        if extra:
                            mismatch_labels.append("extra_sku_lines")
                        if pricelist_mismatch:
                            mismatch_labels.append("pricelist_mismatch")
                        if client_order_ref_mismatch:
                            mismatch_labels.append("customer_po_mismatch")
                        if date_order_mismatch:
                            mismatch_labels.append("date_order_mismatch")
                        if validity_date_mismatch:
                            mismatch_labels.append("validity_date_mismatch")
                        if commitment_date_mismatch:
                            mismatch_labels.append("commitment_date_mismatch")
                        if salesperson_mismatch:
                            mismatch_labels.append("salesperson_mismatch")
                        if sales_team_mismatch:
                            mismatch_labels.append("sales_team_mismatch")
                        if line_content_mismatch:
                            mismatch_labels.append("line_content_mismatch")
                        deleted_pickings = 0
                        kept_pickings = 0
                        if confirmed:
                            deleted_pickings, kept_pickings = _cleanup_cancelled_pickings_for_order(
                                client, order_id
                            )
                        order_path = ""
                        if order_state not in {"draft", "sent"} and confirmed:
                            order_path = "ORDER > CANCEL > QUOTE > ORDER"
                        elif confirmed:
                            order_path = "QUOTE > ORDER"
                        push_log({
                            "Timestamp": now,
                            "Status": "OK_UPDATE_WARN",
                            "Reference": reference,
                            "PostOrder": post_order,
                            "TransactionDate": transaction_date,
                            "CustomerRecordNumber": customer_record,
                            "CustomerOdooId": customer_odoo_id,
                            "OrderOdooId": str(order_id),
                            "OrderState": ("sale" if confirmed else state_after),
                            "OrderPath": order_path,
                            "LineCount": str(len(prepared_lines)),
                            "Details": (
                                (
                                    f"Content repaired from detected mismatch ({', '.join(mismatch_labels)}): {', '.join(repaired_fields)}"
                                    + ("; confirmed" if confirmed else "")
                                )
                                + (
                                    (
                                        f"; removed canceled deliveries={deleted_pickings}"
                                        + (f"; canceled deliveries kept={kept_pickings}" if kept_pickings else "")
                                    )
                                    if confirmed and (deleted_pickings or kept_pickings)
                                    else ""
                                )
                            ),
                        })
                except Exception as exc:
                    order_path = ""
                    if order_state not in {"draft", "sent"}:
                        order_path = "ORDER > CANCEL > QUOTE"
                    push_log({
                        "Timestamp": now,
                        "Status": "ERROR",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": str(order_id),
                        "OrderState": order_state,
                        "OrderPath": order_path,
                        "LineCount": str(len(prepared_lines)),
                        "Details": f"Content repair failed: {exc}",
                    })
                    if not continue_on_error:
                        break
            elif has_content_issues:
                details = []
                if missing_item_records:
                    uniq = sorted(set(missing_item_records))
                    details.append(
                        "Missing SKU mapping from Sage ItemRecordNumber: "
                        + ", ".join(uniq[:10])
                        + (f" ... (+{len(uniq)-10} more)" if len(uniq) > 10 else "")
                    )
                if missing:
                    details.append(
                        "Missing in Odoo: "
                        + " | ".join(missing[:8])
                        + (f" | ... (+{len(missing)-8} more)" if len(missing) > 8 else "")
                    )
                if extra:
                    details.append(
                        "Extra in Odoo: "
                        + " | ".join(extra[:8])
                        + (f" | ... (+{len(extra)-8} more)" if len(extra) > 8 else "")
                    )
                if pricelist_mismatch:
                    details.append(
                        f"Pricelist mismatch expected={desired_pricelist_id} got={current_pricelist_id}"
                    )
                if client_order_ref_mismatch:
                    details.append(
                        f"Customer PO mismatch expected='{expected_client_order_ref}' got='{current_client_order_ref}'"
                    )
                if date_order_mismatch:
                    details.append(
                        f"date_order mismatch expected={expected_date_order} got={current_date_order}"
                    )
                if validity_date_mismatch:
                    details.append(
                        f"validity_date mismatch expected={expected_validity_date} got={current_validity_date}"
                    )
                if commitment_date_mismatch:
                    details.append(
                        f"commitment_date mismatch expected={expected_commitment_date} got={current_commitment_date}"
                    )
                if salesperson_mismatch:
                    details.append(
                        f"Salesperson mismatch expected={desired_user_id_for_verify or 0} got={current_user_id or 0}"
                    )
                if sales_team_mismatch:
                    details.append(
                        f"Sales Team mismatch expected={desired_team_id_for_verify or 0} got={current_team_id or 0}"
                    )
                details.extend(identity_errors)
                if line_content_mismatch:
                    details.append("Order line descriptions/content mismatch")
                push_log({
                    "Timestamp": now,
                    "Status": "ERROR",
                    "Reference": reference,
                    "PostOrder": post_order,
                    "TransactionDate": transaction_date,
                    "CustomerRecordNumber": customer_record,
                    "CustomerOdooId": customer_odoo_id,
                    "OrderOdooId": str(order_id),
                    "OrderState": order_state,
                    "LineCount": str(len(prepared_lines)),
                    "Details": "; ".join(details),
                })
                if not continue_on_error:
                    break
            else:
                confirmed_no_changes = False
                if args.confirm and order_state in {"draft", "sent"}:
                    try:
                        confirmed_no_changes = _confirm_order_if_needed(client, order_id, transaction_date.strip())
                    except Exception:
                        confirmed_no_changes = False
                push_log({
                    "Timestamp": now,
                    "Status": "NO_CHANGES",
                    "Reference": reference,
                    "PostOrder": post_order,
                    "TransactionDate": transaction_date,
                    "CustomerRecordNumber": customer_record,
                    "CustomerOdooId": customer_odoo_id,
                    "OrderOdooId": str(order_id),
                    "OrderState": ("sale" if confirmed_no_changes else order_state),
                    "OrderPath": ("QUOTE > ORDER" if confirmed_no_changes else ""),
                    "LineCount": str(len(prepared_lines)),
                            "Details": (
                                "Content verified (SKU/qty + PO + descriptions)"
                                + ("; confirmed" if confirmed_no_changes else "")
                                + (f"; {forced_cleanup_note}" if forced_cleanup_note else "")
                            ),
                        })
            created += 1
            continue

        pricelist_id = (customer_sync.get("OdooPricelistId") or "").strip()
        term_name = (h.get("TermsDescription") or "").strip()
        term_id = resolve_term_id(terms_map, term_name)
        emp_record = (h.get("EmpRecordNumber") or "").strip()
        emp = employees_map.get(emp_record) or {}
        sales_login, sales_user = resolve_sales_user_live(
            emp,
            user_by_login,
            user_by_name_norm,
        )
        sales_team_id = int(team_by_user_id.get(int(sales_user["id"])) or 0) if sales_user else 0
        if sales_user and not sales_team_id and generic_sales_team_id:
            sales_team_id = generic_sales_team_id
        date_order = transaction_date.strip()
        ship_by_date = (h.get("ShipByDate") or "").strip()
        partner_id_int = int(customer_odoo_id)
        ship_key = (
            partner_id_int,
            (h.get("ShipToName") or "").strip(),
            (h.get("ShipToAddress1") or "").strip(),
            (h.get("ShipToAddress2") or "").strip(),
            (h.get("ShipToCity") or "").strip(),
            (h.get("ShipToState") or "").strip(),
            (h.get("ShipToZIP") or "").strip(),
            bool(args.shipping_relaxed),
        )
        ship_to_country = (h.get("ShipToCountry") or "").strip()
        if ship_key in shipping_partner_cache:
            shipping_partner_id, shipping_exact, shipping_debug = shipping_partner_cache[ship_key]
        else:
            shipping_partner_id, shipping_exact, shipping_debug = resolve_shipping_partner_id(
                client=client,
                partner_id=partner_id_int,
                ship_to_name=ship_key[1],
                ship_to_address1=ship_key[2],
                ship_to_address2=ship_key[3],
                ship_to_city=ship_key[4],
                ship_to_state=ship_key[5],
                ship_to_zip=ship_key[6],
                relaxed=bool(args.shipping_relaxed),
            )
            shipping_partner_cache[ship_key] = (shipping_partner_id, shipping_exact, shipping_debug)

        critical_errors: List[str] = []
        if skipped_reasons:
            preview = " | ".join(skipped_reasons[:5])
            suffix = "" if len(skipped_reasons) <= 5 else f" | ... (+{len(skipped_reasons)-5} more)"
            critical_errors.append(f"Missing product mapping: {preview}{suffix}")
        if total_mismatch:
            critical_errors.append(
                f"Order total mismatch (header={header_total:.2f}, prepared={prepared_total_with_tax:.2f}, "
                f"lines={prepared_total:.2f}, source_tax={tax_total_source:.2f}, source_lines={source_total:.2f}; "
                f"source_rows={source_row_count}, source_product_rows={source_product_row_count}, prepared_rows={len(prepared_lines)})"
            )
        if stale_variant_details:
            preview = " | ".join(stale_variant_details[:5])
            suffix = "" if len(stale_variant_details) <= 5 else f" | ... (+{len(stale_variant_details)-5} more)"
            critical_errors.append(
                "Products mapped to deleted/missing Odoo variants: " + preview + suffix
            )
        if tax_total_source > 0 and not tax_id:
            critical_errors.append(
                f"Missing sales tax mapping for Sage TaxAuthorityCode {tax_code or '(blank)'}"
            )
        if term_name and not term_id:
            critical_errors.append(f"Missing payment term in Odoo: {term_name}")
        if not emp_record:
            critical_errors.append("Missing EmpRecordNumber in Sage order")
        elif emp_record != "0":
            if not (emp and (emp.get("EmployeeID") or "").strip()):
                critical_errors.append(f"Missing employee mapping for EmpRecordNumber {emp_record}")
            elif not sales_user:
                critical_errors.append(f"Missing Odoo user for employee login {sales_login}")
            elif not sales_team_id and not generic_sales_team_id:
                critical_errors.append(f"Missing sales team for Odoo user {sales_user.get('name')}")
        no_sales_rep = emp_record == "0"
        current_no_sales_rep = no_sales_rep
        shipping_preserved_from_existing = False
        shipping_created_on_the_fly = False
        shipping_created_detail = ""
        if not shipping_exact:
            # Strict-mode re-runs: if this order already exists with a concrete
            # shipping contact/address, keep it and avoid failing the whole row.
            if exists and existing_shipping_id and existing_shipping_id != partner_id_int and not args.shipping_relaxed:
                shipping_partner_id = existing_shipping_id
                shipping_preserved_from_existing = True
            else:
                has_dropshipper_tag = _customer_has_dropshipper_tag(partner_id_int)
                should_auto_create_shipping = has_dropshipper_tag
                if should_auto_create_shipping:
                    try:
                        new_shipping_id, created_detail = create_shipping_address_on_the_fly(
                            client=client,
                            parent_id=partner_id_int,
                            ship_to_name=ship_key[1],
                            ship_to_address1=ship_key[2],
                            ship_to_address2=ship_key[3],
                            ship_to_city=ship_key[4],
                            ship_to_state=ship_key[5],
                            ship_to_zip=ship_key[6],
                            ship_to_country=ship_to_country,
                            parity_maps=address_parity_maps,
                            indexes=country_state_indexes,
                        )
                        if new_shipping_id:
                            shipping_partner_id = int(new_shipping_id)
                            shipping_exact = True
                            shipping_created_on_the_fly = True
                            shipping_created_detail = created_detail
                            if has_dropshipper_tag:
                                info_parts.append("Shipping auto-create enabled by customer tag: dropshipper")
                    except Exception as exc:
                        critical_errors.append(f"Failed to create shipping address on the fly: {exc}")
                else:
                    customer_name_part = f"customer_name='{customer_odoo_name}'; " if customer_odoo_name else ""
                    critical_errors.append(
                        "Missing exact shipping address match in Odoo "
                        f"(customer_odoo_id={partner_id_int}; "
                        f"{customer_name_part}"
                        f"sage_ship_to=name='{ship_key[1]}', street='{ship_key[2]}', street2='{ship_key[3]}', "
                        f"city='{ship_key[4]}', state='{ship_key[5]}', zip='{ship_key[6]}'; "
                        f"best_candidate={shipping_debug})"
                    )
        if shipping_preserved_from_existing:
            warning_parts.append("Shipping preserved from existing Odoo order (strict mode)")
        if shipping_created_on_the_fly:
            info_parts.append(shipping_created_detail)

        def merge_details(base: str) -> str:
            parts: List[str] = []
            if base:
                parts.append(base)
            parts.extend(info_parts)
            return "; ".join([p for p in parts if p])

        if critical_errors:
            push_log({
                "Timestamp": now,
                "Status": "ERROR",
                "Reference": reference,
                "PostOrder": post_order,
                "TransactionDate": transaction_date,
                "CustomerRecordNumber": customer_record,
                "CustomerOdooId": customer_odoo_id,
                "OrderOdooId": str(exists[0].get("id", "")) if exists else "",
                "OrderState": order_state,
                "LineCount": str(len(prepared_lines)),
                "Details": "; ".join(critical_errors),
            })
            if not continue_on_error:
                break
            continue

        base_vals = {
            "partner_id": int(customer_odoo_id),
            "date_order": date_order,
            "validity_date": date_order,
            "commitment_date": ship_by_date or date_order,
            "require_signature": False,
            "origin": f"SAGE-SO-{reference}",
            "client_order_ref": resolve_customer_reference_from_sage(h),
            "note": "",
        }
        if pricelist_id:
            base_vals["pricelist_id"] = int(pricelist_id)
        if term_id:
            base_vals["payment_term_id"] = int(term_id)
        if sales_user:
            base_vals["user_id"] = int(sales_user["id"])
            if sales_team_id:
                base_vals["team_id"] = int(sales_team_id)
        elif emp_record == "0":
            # Explicitly clear salesperson when Sage sends EmpRecordNumber=0.
            base_vals["user_id"] = False
            base_vals["team_id"] = False
        if partner_id_int in invoice_partner_cache:
            base_vals["partner_invoice_id"] = invoice_partner_cache[partner_id_int]
        else:
            invoice_partner_id = resolve_invoice_partner_id(
                client=client,
                partner_id=partner_id_int,
            )
            invoice_partner_cache[partner_id_int] = invoice_partner_id
            base_vals["partner_invoice_id"] = invoice_partner_id
        base_vals["partner_shipping_id"] = shipping_partner_id

        if exists:
            order_id = int(exists[0]["id"])
            order_state = str(exists[0].get("state") or "").strip()
            update_vals = dict(base_vals)
            # Upsert behavior: replace lines with the latest Sage snapshot.
            update_vals["order_line"] = [(5, 0, 0)] + [(0, 0, line_vals) for line_vals in prepared_lines]
            current_sig = _existing_order_sig(client, order_id)
            desired_sig = {
                "base": _base_sig_from_vals(base_vals),
                "lines": [_line_sig_from_prepared(l) for l in prepared_lines],
            }
            no_changes = current_sig == desired_sig
            update_reasons = _diff_order_sig(current_sig, desired_sig) if not no_changes else []
            if args.dry_run:
                status = "DRY_RUN_UPDATE_WARN" if warning_parts else "DRY_RUN_UPDATE"
                detail = "Validated and ready to update"
                if no_changes:
                    status = "NO_CHANGES_WARN" if warning_parts else "NO_CHANGES"
                    detail = "No changes in content"
                elif order_state not in {"draft", "sent"}:
                    if set(update_reasons) == {"date_order"}:
                        status = "DRY_RUN_UPDATE"
                        detail = "Would update: date_order on confirmed order"
                    else:
                        status = "ERROR"
                        detail = f"Confirmed order differs ({', '.join(update_reasons)}); update blocked"
                push_log({
                    "Timestamp": now,
                    "Status": status,
                    "Reference": reference,
                    "PostOrder": post_order,
                    "TransactionDate": transaction_date,
                    "CustomerRecordNumber": customer_record,
                    "CustomerOdooId": customer_odoo_id,
                    "OrderOdooId": str(order_id),
                    "OrderState": order_state,
                    "LineCount": str(len(prepared_lines)),
                    "Details": (
                        (
                            merge_details(detail if no_changes else f"Would update: {', '.join(update_reasons)}")
                        )
                        if not warning_parts
                        else merge_details("; ".join(warning_parts))
                    ),
                })
                created += 1
            else:
                try:
                    if no_changes:
                        confirmed = _confirm_order_if_needed(client, order_id, date_order) if (args.confirm and not warning_parts) else False
                        push_log({
                            "Timestamp": now,
                            "Status": ("NO_CHANGES_WARN" if warning_parts else ("OK_UPDATE" if confirmed else "NO_CHANGES")),
                            "Reference": reference,
                            "PostOrder": post_order,
                            "TransactionDate": transaction_date,
                            "CustomerRecordNumber": customer_record,
                            "CustomerOdooId": customer_odoo_id,
                            "OrderOdooId": str(order_id),
                            "OrderState": ("sale" if confirmed else order_state),
                            "LineCount": str(len(prepared_lines)),
                            "Details": (
                                (
                                    merge_details(
                                        "No changes in content"
                                    + ("; confirmed" if confirmed else "")
                                    )
                                )
                                if not warning_parts
                                else merge_details("; ".join(warning_parts))
                            ),
                        })
                    elif order_state not in {"draft", "sent"}:
                        if set(update_reasons) == {"date_order"}:
                            client.models.execute_kw(
                                client.db,
                                client.uid,
                                client.apikey,
                                "sale.order",
                                "write",
                                [[order_id], {"date_order": f"{date_order} 00:00:00"}],
                            )
                            push_log({
                                "Timestamp": now,
                                "Status": "OK_UPDATE",
                                "Reference": reference,
                                "PostOrder": post_order,
                                "TransactionDate": transaction_date,
                                "CustomerRecordNumber": customer_record,
                                "CustomerOdooId": customer_odoo_id,
                                "OrderOdooId": str(order_id),
                                "OrderState": order_state,
                                "LineCount": str(len(prepared_lines)),
                                "Details": merge_details("Updated: date_order on confirmed order"),
                            })
                        else:
                            push_log({
                                "Timestamp": now,
                                "Status": "ERROR",
                                "Reference": reference,
                                "PostOrder": post_order,
                                "TransactionDate": transaction_date,
                                "CustomerRecordNumber": customer_record,
                                "CustomerOdooId": customer_odoo_id,
                                "OrderOdooId": str(order_id),
                                "OrderState": order_state,
                                "LineCount": str(len(prepared_lines)),
                                "Details": f"Confirmed order differs ({', '.join(update_reasons)}); update blocked",
                            })
                            if not continue_on_error:
                                break
                    else:
                        client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "sale.order",
                            "write",
                            [[order_id], update_vals],
                        )
                        confirmed = _confirm_order_if_needed(client, order_id, date_order) if (args.confirm and not warning_parts) else False
                        push_log({
                            "Timestamp": now,
                            "Status": "OK_UPDATE_WARN" if warning_parts else "OK_UPDATE",
                            "Reference": reference,
                            "PostOrder": post_order,
                            "TransactionDate": transaction_date,
                            "CustomerRecordNumber": customer_record,
                            "CustomerOdooId": customer_odoo_id,
                            "OrderOdooId": str(order_id),
                            "OrderState": ("sale" if confirmed else order_state),
                            "LineCount": str(len(prepared_lines)),
                            "Details": (
                                merge_details(
                                    f"Updated: {', '.join(update_reasons)}"
                                + ("; confirmed" if confirmed else "")
                                )
                                if not warning_parts
                                else merge_details(f"Updated: {', '.join(update_reasons)}; " + "; ".join(warning_parts))
                            ),
                        })
                    created += 1
                except Exception as exc:
                    exc_text = f"{type(exc).__name__}: {exc}"
                    if "Record does not exist or has been deleted." in str(exc) and "product.product(" in str(exc):
                        m = re.search(r"product\.product\((\d+),\)", str(exc))
                        missing_vid = m.group(1) if m else "?"
                        exc_text = (
                            f"Missing Odoo variant id {missing_vid} (deleted in Odoo). "
                            "Run refresh_odoo to rebuild items_odoo and retry this order."
                        )
                    push_log({
                        "Timestamp": now,
                        "Status": "ERROR",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": str(order_id),
                        "OrderState": order_state,
                        "LineCount": str(len(prepared_lines)),
                        "Details": exc_text,
                    })
                    if not continue_on_error:
                        break
        else:
            create_vals = dict(base_vals)
            create_vals["name"] = reference
            create_vals["order_line"] = [(0, 0, line_vals) for line_vals in prepared_lines]
            if args.dry_run:
                push_log({
                    "Timestamp": now,
                    "Status": "DRY_RUN_WARN" if warning_parts else "DRY_RUN",
                    "Reference": reference,
                    "PostOrder": post_order,
                    "TransactionDate": transaction_date,
                    "CustomerRecordNumber": customer_record,
                    "CustomerOdooId": customer_odoo_id,
                    "OrderOdooId": "",
                    "OrderState": "draft",
                    "LineCount": str(len(prepared_lines)),
                    "Details": (
                        merge_details("Validated and ready to create")
                        if not warning_parts
                        else merge_details("; ".join(warning_parts))
                    ),
                })
                created += 1
            else:
                try:
                    order_id = client.models.execute_kw(
                        client.db,
                        client.uid,
                        client.apikey,
                        "sale.order",
                        "create",
                        [create_vals],
                    )
                    confirmed = _confirm_order_if_needed(client, int(order_id), date_order) if (args.confirm and not warning_parts) else False
                    push_log({
                        "Timestamp": now,
                        "Status": "OK_WARN" if warning_parts else "OK",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": str(order_id),
                        "OrderState": ("sale" if confirmed else "draft"),
                        "LineCount": str(len(prepared_lines)),
                        "Details": (
                            (
                                merge_details(
                                    "Created sale.order"
                                + ("; confirmed" if confirmed else "")
                                )
                            )
                            if not warning_parts
                            else merge_details("; ".join(warning_parts))
                        ),
                    })
                    created += 1
                except Exception as exc:
                    exc_text = f"{type(exc).__name__}: {exc}"
                    if "Record does not exist or has been deleted." in str(exc) and "product.product(" in str(exc):
                        m = re.search(r"product\.product\((\d+),\)", str(exc))
                        missing_vid = m.group(1) if m else "?"
                        exc_text = (
                            f"Missing Odoo variant id {missing_vid} (deleted in Odoo). "
                            "Run refresh_odoo to rebuild items_odoo and retry this order."
                        )
                    push_log({
                        "Timestamp": now,
                        "Status": "ERROR",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": "",
                        "OrderState": "",
                        "LineCount": str(len(prepared_lines)),
                        "Details": exc_text,
                    })
                    if not continue_on_error:
                        break

        if warning_parts and not args.dry_run:
            print("STOP: warning detected. Order left in draft and process halted.")
            break

        if max_orders is not None and processed_count >= max_orders:
            break

    append_log(args.log_path, logs)
    status_counts: Dict[str, int] = {}
    for row in logs:
        st = (row.get("Status") or "").strip() or "UNKNOWN"
        status_counts[st] = status_counts.get(st, 0) + 1
    target_text = str(max_orders) if max_orders is not None else "all"
    print(f"Processed {len(logs)}/{target_text}.")
    if status_counts:
        no_changes = status_counts.get("NO_CHANGES", 0)
        warnings = (
            status_counts.get("NO_CHANGES_WARN", 0)
            + status_counts.get("OK_WARN", 0)
            + status_counts.get("OK_UPDATE_WARN", 0)
            + status_counts.get("DRY_RUN_WARN", 0)
            + status_counts.get("DRY_RUN_UPDATE_WARN", 0)
        )
        updated = status_counts.get("OK_UPDATE", 0)
        created_count = status_counts.get("OK", 0)
        errors = status_counts.get("ERROR", 0)
        print(
            "Summary: "
            f"{no_changes} No changes | "
            f"{warnings} Warning | "
            f"{updated} Updated | "
            f"{created_count} Created | "
            f"{errors} Error"
        )
    print(f"OK: log -> {args.log_path}")
    return 0


if __name__ == "__main__":
    parser = build_parser()
    raise SystemExit(run(parser.parse_args()))
