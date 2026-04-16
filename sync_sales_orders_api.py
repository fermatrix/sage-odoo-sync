import argparse
import csv
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

from sync_customers import DELIMITER, get_env_value, load_env_file, read_csv
from sync_parity import OdooClient


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


def profile_env(env: Dict[str, str], profile: str, key_suffix: str) -> str:
    profile_key = f"ODOO_{profile.upper()}_{key_suffix}"
    generic_key = f"ODOO_{key_suffix}"
    return get_env_value(env, profile_key) or get_env_value(env, generic_key)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Create Sage Sales Orders in Odoo via API")
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
    p.add_argument("--products-sync", default=r"ENZO-Sage50\_master\products_sync.csv")
    p.add_argument("--employees-sync", default=r"ENZO-Sage50\_master\employees_sync.csv")
    p.add_argument("--limit", type=int, default=1, help="How many orders to create")
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
        help="Process only one Sage order reference (example: 357702)",
    )
    p.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow partial processing when a required mapping is missing (default: stop on first critical error)",
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
) -> Dict[str, object]:
    out: List[Dict[str, object]] = []
    source_total = 0.0
    skipped_reasons: List[str] = []
    has_shipping_line = False
    shipping_line_indexes: List[int] = []
    for line in source_lines:
        item_record = (line.get("ItemRecordNumber") or "").strip()
        if not item_record or item_record == "0":
            continue
        source_amount = abs(parse_decimal(line.get("Amount") or ""))
        if source_amount > 0:
            source_total += source_amount
        product_sync = products_map.get(item_record)
        if not product_sync:
            skipped_reasons.append(f"ItemRecordNumber {item_record}: not found in products_sync")
            continue
        variant_id = int((product_sync.get("OdooVariantId") or "0").strip() or 0)
        if not variant_id:
            skipped_reasons.append(f"ItemRecordNumber {item_record}: missing OdooVariantId")
            continue
        qty = parse_decimal(line.get("Quantity") or "")
        if qty <= 0:
            skipped_reasons.append(f"ItemRecordNumber {item_record}: invalid quantity {line.get('Quantity')}")
            continue
        price_unit = parse_decimal(line.get("UnitCost") or "")
        if price_unit == 0:
            amount = abs(parse_decimal(line.get("Amount") or ""))
            price_unit = amount / qty if qty else 0
        item_id = (product_sync.get("ItemID") or "").strip().upper()
        item_desc = (product_sync.get("ItemDescription") or "").strip().upper()
        row_desc = (line.get("RowDescription") or "").strip().upper()
        is_shipping = (
            "SHIPPING" in item_id
            or "SHIPPING" in item_desc
            or "SHIPPING" in row_desc
        )
        out.append({
            "product_id": variant_id,
            "name": (line.get("RowDescription") or "").strip(),
            "product_uom_qty": qty,
            "price_unit": round(price_unit, 2),
        })
        if is_shipping:
            has_shipping_line = True
            shipping_line_indexes.append(len(out) - 1)
    prepared_total = round(sum((l["product_uom_qty"] * l["price_unit"]) for l in out), 2)
    return {
        "lines": out,
        "source_total": round(source_total, 2),
        "prepared_total": prepared_total,
        "skipped_reasons": skipped_reasons,
        "has_shipping_line": has_shipping_line,
        "shipping_line_indexes": shipping_line_indexes,
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
) -> tuple[int, bool]:
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
        return partner_id, False

    ranked = sorted(candidates, key=score, reverse=True)
    best = ranked[0]
    best_score = score(best)
    # Require strong address match; otherwise keep parent customer address.
    if best_score >= 9:
        return int(best["id"]), True
    return partner_id, False


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


def to_login(employee_id: str) -> str:
    return (employee_id or "").strip().lower().replace(" ", "_")


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


def run(args: argparse.Namespace) -> int:
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
    products_map = load_products_map(args.products_sync)
    employees_map = load_employees_map(args.employees_sync)
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
    terms_map = find_payment_terms(client)
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

    logs: List[Dict[str, str]] = []
    created = 0
    seen_candidates = 0

    for h in headers:
        post_order = (h.get("PostOrder") or "").strip()
        reference = (h.get("Reference") or "").strip()
        if args.reference and reference != args.reference.strip():
            continue
        customer_record = (h.get("CustVendId") or "").strip()
        transaction_date = (h.get("TransactionDate") or "").strip()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        source_lines = lines_by_postorder.get(post_order, [])
        prepared_info = build_order_lines(source_lines, products_map)
        prepared_lines = prepared_info["lines"]
        has_product_lines = bool(prepared_lines)
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
        if not has_product_lines:
            logs.append({
                "Timestamp": now,
                "Status": "ERROR",
                "Reference": reference,
                "PostOrder": post_order,
                "TransactionDate": transaction_date,
                "CustomerRecordNumber": customer_record,
                "CustomerOdooId": "",
                "OrderOdooId": "",
                "LineCount": "0",
                "Details": "No valid order lines after product mapping",
            })
            if not args.allow_partial:
                break
            continue
        customer_sync = customers_map.get(customer_record) or {}
        customer_odoo_id = (customer_sync.get("OdooId") or "").strip()
        if not customer_odoo_id:
            logs.append({
                "Timestamp": now,
                "Status": "ERROR",
                "Reference": reference,
                "PostOrder": post_order,
                "TransactionDate": transaction_date,
                "CustomerRecordNumber": customer_record,
                "CustomerOdooId": "",
                "OrderOdooId": "",
                "LineCount": str(len(prepared_lines)),
                "Details": "Missing customer mapping in Odoo",
            })
            if not args.allow_partial:
                break
            continue

        seen_candidates += 1
        if seen_candidates <= args.offset:
            continue

        header_total = round(abs(parse_decimal(h.get("MainAmount") or "")), 2)
        source_total = prepared_info["source_total"]
        prepared_total = prepared_info["prepared_total"]
        skipped_reasons = prepared_info["skipped_reasons"]
        total_mismatch = abs(header_total - prepared_total) > 0.02
        warning_parts = []
        if total_mismatch:
            warning_parts.append(
                f"Total mismatch header={header_total:.2f} prepared={prepared_total:.2f} source_lines={source_total:.2f}"
            )
        if skipped_reasons:
            warning_parts.append("Skipped lines: " + " | ".join(skipped_reasons[:5]))

        exists = client.search_read(
            "sale.order",
            [("name", "=", reference)],
            ["id", "name", "state"],
            limit=1,
            offset=0,
        )

        pricelist_id = (customer_sync.get("OdooPricelistId") or "").strip()
        term_name = (h.get("TermsDescription") or "").strip()
        term_id = resolve_term_id(terms_map, term_name)
        emp_record = (h.get("EmpRecordNumber") or "").strip()
        emp = employees_map.get(emp_record) or {}
        emp_id = (emp.get("EmployeeID") or "").strip()
        sales_login = to_login(emp_id)
        sales_user = user_by_login.get(sales_login) if sales_login else None
        date_order = transaction_date.strip()
        ship_by_date = (h.get("ShipByDate") or "").strip()
        shipping_partner_id, shipping_exact = resolve_shipping_partner_id(
            client=client,
            partner_id=int(customer_odoo_id),
            ship_to_name=(h.get("ShipToName") or "").strip(),
            ship_to_address1=(h.get("ShipToAddress1") or "").strip(),
            ship_to_address2=(h.get("ShipToAddress2") or "").strip(),
            ship_to_city=(h.get("ShipToCity") or "").strip(),
            ship_to_state=(h.get("ShipToState") or "").strip(),
            ship_to_zip=(h.get("ShipToZIP") or "").strip(),
        )

        critical_errors: List[str] = []
        if skipped_reasons:
            critical_errors.append("Missing product mapping in one or more lines")
        if total_mismatch:
            critical_errors.append(
                f"Order total mismatch (header={header_total:.2f}, prepared={prepared_total:.2f}, source_lines={source_total:.2f})"
            )
        if term_name and not term_id:
            critical_errors.append(f"Missing payment term in Odoo: {term_name}")
        if not emp_record:
            critical_errors.append("Missing EmpRecordNumber in Sage order")
        elif emp_record != "0":
            if not emp_id:
                critical_errors.append(f"Missing employee mapping for EmpRecordNumber {emp_record}")
            elif not sales_user:
                critical_errors.append(f"Missing Odoo user for employee login {sales_login}")
        else:
            warning_parts.append("EmpRecordNumber is 0 in Sage; salesperson cleared in Odoo (user_id=False)")
        if not shipping_exact:
            critical_errors.append("Missing exact shipping address match in Odoo")

        if critical_errors:
            logs.append({
                "Timestamp": now,
                "Status": "ERROR",
                "Reference": reference,
                "PostOrder": post_order,
                "TransactionDate": transaction_date,
                "CustomerRecordNumber": customer_record,
                "CustomerOdooId": customer_odoo_id,
                "OrderOdooId": str(exists[0].get("id", "")) if exists else "",
                "LineCount": str(len(prepared_lines)),
                "Details": "; ".join(critical_errors),
            })
            if not args.allow_partial:
                break
            continue

        base_vals = {
            "partner_id": int(customer_odoo_id),
            "date_order": date_order,
            "validity_date": date_order,
            "commitment_date": ship_by_date or date_order,
            "require_signature": False,
            "origin": f"SAGE-SO-{reference}",
            "client_order_ref": reference,
            "note": "",
        }
        if pricelist_id:
            base_vals["pricelist_id"] = int(pricelist_id)
        if term_id:
            base_vals["payment_term_id"] = int(term_id)
        if sales_user:
            base_vals["user_id"] = int(sales_user["id"])
        elif emp_record == "0":
            # Explicitly clear salesperson when Sage sends EmpRecordNumber=0.
            base_vals["user_id"] = False
        base_vals["partner_invoice_id"] = resolve_invoice_partner_id(
            client=client,
            partner_id=int(customer_odoo_id),
        )
        base_vals["partner_shipping_id"] = shipping_partner_id

        if exists:
            order_id = int(exists[0]["id"])
            update_vals = dict(base_vals)
            # Upsert behavior: replace lines with the latest Sage snapshot.
            update_vals["order_line"] = [(5, 0, 0)] + [(0, 0, line_vals) for line_vals in prepared_lines]
            if args.dry_run:
                logs.append({
                    "Timestamp": now,
                    "Status": "DRY_RUN_UPDATE_WARN" if warning_parts else "DRY_RUN_UPDATE",
                    "Reference": reference,
                    "PostOrder": post_order,
                    "TransactionDate": transaction_date,
                    "CustomerRecordNumber": customer_record,
                    "CustomerOdooId": customer_odoo_id,
                    "OrderOdooId": str(order_id),
                    "LineCount": str(len(prepared_lines)),
                    "Details": ("Validated and ready to update" if not warning_parts else "; ".join(warning_parts)),
                })
                created += 1
            else:
                try:
                    client.models.execute_kw(
                        client.db,
                        client.uid,
                        client.apikey,
                        "sale.order",
                        "write",
                        [[order_id], update_vals],
                    )
                    logs.append({
                        "Timestamp": now,
                        "Status": "OK_UPDATE_WARN" if warning_parts else "OK_UPDATE",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": str(order_id),
                        "LineCount": str(len(prepared_lines)),
                        "Details": ("Updated existing draft sale.order" if not warning_parts else "; ".join(warning_parts)),
                    })
                    created += 1
                except Exception as exc:
                    logs.append({
                        "Timestamp": now,
                        "Status": "ERROR",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": str(order_id),
                        "LineCount": str(len(prepared_lines)),
                        "Details": f"{type(exc).__name__}: {exc}",
                    })
        else:
            create_vals = dict(base_vals)
            create_vals["name"] = reference
            create_vals["order_line"] = [(0, 0, line_vals) for line_vals in prepared_lines]
            if args.dry_run:
                logs.append({
                    "Timestamp": now,
                    "Status": "DRY_RUN_WARN" if warning_parts else "DRY_RUN",
                    "Reference": reference,
                    "PostOrder": post_order,
                    "TransactionDate": transaction_date,
                    "CustomerRecordNumber": customer_record,
                    "CustomerOdooId": customer_odoo_id,
                    "OrderOdooId": "",
                    "LineCount": str(len(prepared_lines)),
                    "Details": ("Validated and ready to create" if not warning_parts else "; ".join(warning_parts)),
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
                    logs.append({
                        "Timestamp": now,
                        "Status": "OK_WARN" if warning_parts else "OK",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": str(order_id),
                        "LineCount": str(len(prepared_lines)),
                        "Details": ("Created draft sale.order" if not warning_parts else "; ".join(warning_parts)),
                    })
                    created += 1
                except Exception as exc:
                    logs.append({
                        "Timestamp": now,
                        "Status": "ERROR",
                        "Reference": reference,
                        "PostOrder": post_order,
                        "TransactionDate": transaction_date,
                        "CustomerRecordNumber": customer_record,
                        "CustomerOdooId": customer_odoo_id,
                        "OrderOdooId": "",
                        "LineCount": str(len(prepared_lines)),
                        "Details": f"{type(exc).__name__}: {exc}",
                    })

        if created >= args.limit:
            break

    append_log(args.log_path, logs)
    print(f"OK: orders processed={len(logs)} created_or_validated={created}")
    print(f"OK: log -> {args.log_path}")
    return 0


if __name__ == "__main__":
    parser = build_parser()
    raise SystemExit(run(parser.parse_args()))
