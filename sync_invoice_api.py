import argparse
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Tuple
import re

from sync_customers import get_env_value, load_env_file, read_csv
from sync_delivery_orders_api import (
    _find_existing_for_invoice_tag,
    _invoice_tag,
    _load_by_load,
    _load_item_record_to_sku,
    _parse_load_spec,
    _parse_limit_offset,
    _parse_reference_filter,
    _picking_line,
    _pickings_for_sale_order,
    parse_decimal,
    profile_env,
)
from sync_parity import OdooClient


def _load_sku_to_product_info(client: OdooClient) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    offset = 0
    while True:
        rows = client.models.execute_kw(
            client.db,
            client.uid,
            client.apikey,
            "product.product",
            "search_read",
            [[]],
            {
                "fields": ["id", "default_code", "type", "categ_id", "name"],
                "limit": 2000,
                "offset": offset,
                "context": {"active_test": False},
            },
        )
        if not rows:
            break
        for r in rows:
            sku = str(r.get("default_code") or "").strip().upper()
            if sku and sku not in out:
                out[sku] = {
                    "id": int(r.get("id") or 0),
                    "type": str(r.get("type") or "").strip(),
                    "categ_id": r.get("categ_id"),
                    "name": str(r.get("name") or "").strip(),
                }
        offset += len(rows)
    return out


def _resolve_freight_product_id(client: OdooClient) -> int:
    # Prefer explicit SKU/default_code.
    rows = client.search_read(
        "product.product",
        [("default_code", "=", "FREIGHT")],
        ["id", "default_code", "name", "active"],
        limit=1,
        offset=0,
    )
    if rows:
        return int(rows[0].get("id") or 0)
    # Fallback by name.
    rows = client.search_read(
        "product.product",
        [("name", "ilike", "FREIGHT")],
        ["id", "default_code", "name", "active"],
        limit=1,
        offset=0,
    )
    return int(rows[0].get("id") or 0) if rows else 0


def _parse_tx_date(raw: str) -> Optional[date]:
    text = (raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            from datetime import datetime
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _tx_date_in_load(tx_raw: str, load_spec: str) -> bool:
    kind, payload = _parse_load_spec(load_spec)
    tx = _parse_tx_date(tx_raw)
    if tx is None:
        return False
    if kind == "day":
        return tx == payload
    if kind == "month":
        y, m = payload
        return tx.year == y and tx.month == m
    if kind in {"fiscal_year", "range"}:
        start, end = payload
        return start <= tx < end
    return True


def _invoice_ref_sort_key(invoice_ref: str) -> Tuple[str, int, str]:
    ref = (invoice_ref or "").strip().upper()
    if "-" not in ref:
        return ref, 0, ""
    base, suffix = ref.rsplit("-", 1)
    if len(suffix) == 1 and suffix.isalpha():
        return base, ord(suffix) - ord("A") + 1, suffix
    return ref, 0, ""


def _print_result(index: int, tx_date: str, status: str, so_ref: str, inv_ref: str, detail: str) -> None:
    prefix = f"[{index:03d}]"
    if tx_date:
        prefix += f" - {tx_date}"
    if inv_ref and inv_ref == so_ref:
        line = f"{prefix} - [{status}] SO/Invoice {so_ref}"
    else:
        line = f"{prefix} - [{status}] SO {so_ref}"
        if inv_ref:
            line += f" | Invoice {inv_ref}"
    print(line)
    if detail:
        for part in [p.strip() for p in detail.splitlines() if p.strip()]:
            print(f"        - {part}")
    print("")


def _attach_invoice_status(detail_lines: List[str], invoice_status_text: str) -> List[str]:
    if not detail_lines:
        return [invoice_status_text]
    out = list(detail_lines)
    text = invoice_status_text.strip()
    if text.startswith("### "):
        text = text[4:].strip()
    out[0] = f"{out[0]} | {text}"
    return out


def _replace_invoice_status(detail_lines: List[str], inv_ref: str, new_state: str) -> List[str]:
    if not detail_lines:
        return detail_lines
    old = f"### Invoice {inv_ref} - draft"
    new = f"### Invoice {inv_ref} - {new_state}"
    detail_lines[0] = detail_lines[0].replace(old, new)
    return detail_lines


def _format_do_line_for_invoice_log(line: str) -> str:
    text = (line or "").strip()
    if " | " not in text:
        return text
    left, right = text.rsplit(" | ", 1)
    state = right.strip()
    if state in {"done", "waiting", "not delivered", "partially delivered"}:
        return f"{left} | DO - {state}"
    return text


def _format_do_lines_for_invoice_log(lines: List[str]) -> List[str]:
    return [_format_do_line_for_invoice_log(x) for x in lines]


def _existing_invoice_for_sage(client: OdooClient, so_ref: str, inv_ref: str) -> Optional[Dict[str, object]]:
    # We keep Sage invoice number in Odoo invoice "name".
    # Customer reference ("ref") is used for Sage PO number, so it cannot be
    # used to identify invoices.
    rows = client.search_read(
        "account.move",
        [
            ("move_type", "=", "out_invoice"),
            ("invoice_origin", "=", so_ref),
            ("state", "in", ["draft", "posted"]),
        ],
        ["id", "name", "state", "ref", "invoice_user_id", "team_id", "amount_total"],
        limit=50,
        offset=0,
    )
    if not rows:
        return None
    ref_norm = (inv_ref or "").strip().upper()
    for r in rows:
        name_norm = str(r.get("name") or "").strip().upper()
        old_ref_norm = str(r.get("ref") or "").strip().upper()
        if name_norm == ref_norm or old_ref_norm == ref_norm:
            return r
    return None


def _sage_invoice_total(inv_header: Dict[str, str]) -> float:
    return round(abs(parse_decimal(inv_header.get("MainAmount") or "")), 2)


def _sage_customer_po(inv_header: Dict[str, str]) -> str:
    return str(inv_header.get("PurchOrder") or "").strip()


def _normalize_freight_line_labels(client: OdooClient, move_id: int, freight_product_id: int) -> bool:
    if int(move_id or 0) <= 0 or int(freight_product_id or 0) <= 0:
        return False
    lines = client.search_read(
        "account.move.line",
        [("move_id", "=", int(move_id)), ("product_id", "=", int(freight_product_id))],
        ["id", "name", "display_type"],
        limit=200,
        offset=0,
    )
    changed = False
    for ln in lines:
        dtype = str(ln.get("display_type") or "")
        if dtype in {"line_note", "line_section", "payment_term"}:
            continue
        current = str(ln.get("name") or "").strip()
        if current != "Freight":
            client.models.execute_kw(
                client.db,
                client.uid,
                client.apikey,
                "account.move.line",
                "write",
                [[int(ln["id"])], {"name": "Freight"}],
            )
            changed = True
    return changed


def _load_so_lines(client: OdooClient, so_id: int) -> List[Dict[str, object]]:
    so_rows = client.search_read("sale.order", [("id", "=", so_id)], ["order_line"], limit=1, offset=0)
    if not so_rows:
        return []
    line_ids = so_rows[0].get("order_line") or []
    if not line_ids:
        return []
    return client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "sale.order.line",
        "read",
        [line_ids],
        {"fields": ["id", "product_id", "display_type", "qty_delivered", "qty_invoiced", "price_unit", "name", "tax_ids"]},
    )


def _build_invoice_line_commands(
    inv_header: Dict[str, str],
    invoice_lines_by_postorder: Dict[str, List[Dict[str, str]]],
    item_record_to_sku: Dict[str, str],
    sku_to_product_info: Dict[str, Dict[str, object]],
    so_lines: List[Dict[str, object]],
    freight_product_id: int,
    allow_already_invoiced: bool = False,
) -> Tuple[List[Tuple[int, int, Dict[str, object]]], List[str], List[str]]:
    errors: List[str] = []
    commands: List[Tuple[int, int, Dict[str, object]]] = []
    sage_descriptions: List[str] = []
    post = (inv_header.get("PostOrder") or "").strip()
    src_rows = invoice_lines_by_postorder.get(post, [])
    if not src_rows:
        return [], [f"No invoice lines found in Sage for PostOrder {post}"], []

    so_by_product: Dict[int, List[Dict[str, object]]] = defaultdict(list)
    for line in so_lines:
        if line.get("display_type"):
            continue
        prod = line.get("product_id") or []
        pid = int(prod[0]) if isinstance(prod, list) and prod else 0
        if pid > 0:
            so_by_product[pid].append(line)

    for pid in so_by_product:
        so_by_product[pid].sort(key=lambda r: int(r.get("id") or 0))

    for r in src_rows:
        jrx = (r.get("JournalRowEx") or "").strip()
        amt = parse_decimal(r.get("Amount") or "")
        if jrx == "0" and abs(amt) < 0.0001:
            continue
        # Ignore zero-amount shadow rows (common in Sage exports for row replicas).
        if abs(amt) < 0.0001:
            continue
        item_record = (r.get("ItemRecordNumber") or "").strip()
        qty = parse_decimal(r.get("Quantity") or "")
        if not item_record or item_record == "0" or qty <= 0:
            continue

        sku = item_record_to_sku.get(item_record, "")
        if not sku:
            errors.append(f"ItemRecordNumber {item_record}: missing SKU in items.csv")
            continue
        pinfo = sku_to_product_info.get(sku)
        if not pinfo:
            errors.append(f"SKU {sku}: missing in Odoo products")
            continue
        pid = int(pinfo.get("id") or 0)
        if pid <= 0:
            errors.append(f"SKU {sku}: invalid Odoo product id")
            continue
        ptype = str(pinfo.get("type") or "").strip().lower()
        pcateg = pinfo.get("categ_id") or []
        has_category = bool(isinstance(pcateg, list) and pcateg)
        if ptype == "service" and not has_category:
            pname = str(pinfo.get("name") or sku).strip()
            errors.append(f"SKU {sku}: service product '{pname}' has no category in Odoo")
            continue

        candidates = so_by_product.get(pid, [])
        if not candidates:
            errors.append(f"SKU {sku}: no matching sale.order.line in Odoo SO")
            continue

        # Services (e.g., discounts/freight-like commercial lines) should not
        # depend on stock delivery progression. Invoice them directly from the
        # SO line by requested Sage quantity.
        if ptype == "service":
            sol = candidates[0]
            tax_ids = sol.get("tax_ids") or []
            raw_desc = str(r.get("RowDescription") or sol.get("name") or "").strip()
            paren_parts = re.findall(r"\([^()]*\)", raw_desc)
            second_line = " | ".join([p.strip() for p in paren_parts if p.strip()])
            line1 = raw_desc
            for part in paren_parts:
                line1 = line1.replace(part, " ")
            line1 = re.sub(r"\s{2,}", " ", line1).strip()
            line_desc = line1 or raw_desc
            if second_line:
                line_desc = f"{line_desc}\n{second_line}"
            commands.append(
                (
                    0,
                    0,
                    {
                        "product_id": pid,
                        "name": line_desc,
                        "quantity": float(qty),
                        "price_unit": float(sol.get("price_unit") or 0.0),
                        "sale_line_ids": [(4, int(sol.get("id") or 0))],
                        "tax_ids": [(6, 0, list(tax_ids))] if tax_ids else [],
                    },
                )
            )
            sage_descriptions.append(line_desc)
            continue

        remaining = qty
        for sol in candidates:
            delivered = float(sol.get("qty_delivered") or 0.0)
            invoiced = float(sol.get("qty_invoiced") or 0.0)
            available = round(delivered - invoiced, 6)
            if allow_already_invoiced and available <= 0:
                # Resync mode for existing draft invoices: allow rebuilding
                # lines from Sage even if SO line is already considered invoiced.
                # This keeps product linkage but refreshes description/content.
                available = remaining
            if available <= 0:
                continue
            take = min(remaining, available)
            if take <= 0:
                continue
            tax_ids = sol.get("tax_ids") or []
            raw_desc = str(r.get("RowDescription") or sol.get("name") or "").strip()
            paren_parts = re.findall(r"\([^()]*\)", raw_desc)
            second_line = " | ".join([p.strip() for p in paren_parts if p.strip()])
            line1 = raw_desc
            for part in paren_parts:
                line1 = line1.replace(part, " ")
            line1 = re.sub(r"\s{2,}", " ", line1).strip()
            line_desc = line1 or raw_desc
            if second_line:
                line_desc = f"{line_desc}\n{second_line}"
            commands.append(
                (
                    0,
                    0,
                    {
                        "product_id": pid,
                        "name": line_desc,
                        "quantity": float(take),
                        "price_unit": float(sol.get("price_unit") or 0.0),
                        "sale_line_ids": [(4, int(sol.get("id") or 0))],
                        "tax_ids": [(6, 0, list(tax_ids))] if tax_ids else [],
                    },
                )
            )
            sage_descriptions.append(line_desc)
            remaining = round(remaining - take, 6)
            if remaining <= 0:
                break

        if remaining > 0:
            errors.append(
                f"SKU {sku}: not enough delivered-not-invoiced qty (needed={qty:g}, remaining={remaining:g})"
            )

    # Freight lines in Sage usually come with ItemRecordNumber=0 and RowDescription='Freight Amount'.
    freight_total = 0.0
    freight_desc = ""
    for r in src_rows:
        item_record = (r.get("ItemRecordNumber") or "").strip()
        if item_record not in {"", "0"}:
            continue
        desc = str(r.get("RowDescription") or "").strip().lower()
        if "freight" not in desc:
            continue
        amt = parse_decimal(r.get("Amount") or "")
        if abs(amt) <= 0.0001:
            continue
        if not freight_desc:
            freight_desc = str(r.get("RowDescription") or "").strip()
        freight_total += abs(amt)
    freight_total = round(freight_total, 2)
    if freight_total > 0:
        if freight_product_id <= 0:
            errors.append("Freight Amount found in Sage but FREIGHT product is missing in Odoo")
        else:
            freight_label = (freight_desc or "Freight").strip()
            # Sage often exports "Freight Amount"; in Odoo line description we
            # keep it cleaner as just "Freight".
            if freight_label.upper() == "FREIGHT AMOUNT":
                freight_label = "Freight"
            commands.append(
                (
                    0,
                    0,
                    {
                        "product_id": int(freight_product_id),
                        "name": freight_label,
                        "quantity": 1.0,
                        "price_unit": float(freight_total),
                    },
                )
            )
            sage_descriptions.append(freight_label)

    # Carry key SO contextual notes into invoice (no accounting impact).
    so_notes = []
    for line in so_lines:
        if str(line.get("display_type") or "") != "line_note":
            continue
        text = str(line.get("name") or "").strip()
        if not text:
            continue
        up = text.upper()
        if "SHIPPING METHOD" in up or "BOGO" in up:
            so_notes.append(text)
    # Keep order and uniqueness
    seen_notes = set()
    for text in so_notes:
        key = text.strip().lower()
        if key in seen_notes:
            continue
        seen_notes.add(key)
        commands.append(
            (
                0,
                0,
                {
                    "display_type": "line_note",
                    "name": text,
                },
            )
        )

    if not commands and not errors:
        errors.append("No invoiceable lines generated")
    return commands, errors, sage_descriptions


def _force_invoice_line_descriptions(
    client: OdooClient,
    move_id: int,
    desired_descriptions: List[str],
) -> bool:
    """
    Enforce Sage descriptions in invoice line Description (name).
    Odoo can override line names from sale/product defaults; this forces the
    final line label to match Sage export order.
    """
    if int(move_id or 0) <= 0 or not desired_descriptions:
        return False
    lines = client.search_read(
        "account.move.line",
        [("move_id", "=", int(move_id))],
        ["id", "name", "display_type", "product_id"],
        limit=500,
        offset=0,
    )
    product_lines = [
        ln for ln in sorted(lines, key=lambda x: int(x.get("id") or 0))
        if str(ln.get("display_type") or "") in {"", "product"}
        and isinstance(ln.get("product_id"), list)
        and bool(ln.get("product_id"))
    ]
    changed = False
    for i, desired in enumerate(desired_descriptions):
        if i >= len(product_lines):
            break
        ln = product_lines[i]
        current = str(ln.get("name") or "").strip()
        target = str(desired or "").strip()
        if target and current != target:
            client.models.execute_kw(
                client.db,
                client.uid,
                client.apikey,
                "account.move.line",
                "write",
                [[int(ln["id"])], {"name": target}],
            )
            changed = True
    return changed


def _as_invoice_datetime(inv_date: str) -> str:
    raw = (inv_date or "").strip()
    if not raw:
        return ""
    try:
        d = date.fromisoformat(raw)
        return f"{d.isoformat()} 12:00:00"
    except ValueError:
        return ""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Create draft Odoo invoices from Sage invoice deliveries",
        allow_abbrev=False,
    )
    p.add_argument("--root-dir", default=r"ENZO-Sage50")
    p.add_argument("--profile", default="STUDIOOPTYX")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--load", default="", help="DD/MM/YYYY, MM/YYYY, YYYY or range")
    p.add_argument("--reference", default="", help="One or many SO references separated by comma")
    p.add_argument("--limit", default="", help="N or start,count")
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--skip", action="store_true", help="Continue after errors")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--confirm", action="store_true", help="Post invoices after successful create/update (skip already posted)")
    p.add_argument("--gaps", action="store_true", help="Process only unresolved Sage invoices (missing in Odoo or still draft)")
    p.add_argument("--items-master", default=r"ENZO-Sage50\_master_sage\items.csv")
    return p


def _chunks(seq: List[str], size: int) -> List[List[str]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def run(args: argparse.Namespace) -> int:
    if not (args.load or "").strip():
        print("ERROR: --load is required")
        return 2

    max_orders, start_offset = _parse_limit_offset(args.limit, args.offset)
    continue_on_error = bool(args.skip)
    ref_filter = _parse_reference_filter(args.reference)

    env = load_env_file(args.env_file)
    url = profile_env(env, args.profile, "URL")
    db = profile_env(env, args.profile, "DB")
    user = profile_env(env, args.profile, "USER")
    apikey = profile_env(env, args.profile, "APIKEY")
    if not (url and db and user and apikey):
        print(f"ERROR: missing Odoo credentials for profile {args.profile}")
        return 2

    client = OdooClient(url=url, db=db, user=user, apikey=apikey)
    invoice_headers, invoice_lines_by_postorder = _load_by_load(args.root_dir, args.load)
    item_record_to_sku = _load_item_record_to_sku(args.items_master)
    sku_to_product_info = _load_sku_to_product_info(client)
    freight_product_id = _resolve_freight_product_id(client)

    invoices_by_so: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for h in invoice_headers:
        if not _tx_date_in_load((h.get("TransactionDate") or "").strip(), args.load):
            continue
        if (h.get("JournalEx") or "").strip() != "8":
            continue
        so_ref = (h.get("INV_POSOOrderNumber") or "").strip()
        if not so_ref:
            continue
        if ref_filter and so_ref not in ref_filter:
            continue
        invoices_by_so[so_ref].append(h)

    for so_ref in invoices_by_so:
        invoices_by_so[so_ref].sort(
            key=lambda r: (
                (r.get("TransactionDate") or "").strip(),
                _invoice_ref_sort_key((r.get("Reference") or "").strip()),
                (r.get("PostOrder") or "").strip(),
            )
        )

    if args.gaps:
        # Keep only unresolved Sage invoices:
        # - missing in Odoo, or
        # - existing but still in draft.
        flat: List[Tuple[str, str, Dict[str, str]]] = []
        for so_ref, invs in invoices_by_so.items():
            for inv in invs:
                inv_ref = (inv.get("Reference") or "").strip()
                if so_ref and inv_ref:
                    flat.append((so_ref, inv_ref, inv))

        existing_by_key: Dict[Tuple[str, str], Dict[str, object]] = {}
        all_inv_refs = sorted({inv_ref for _, inv_ref, _ in flat})
        for name_chunk in _chunks(all_inv_refs, 200):
            rows = client.search_read(
                "account.move",
                [("move_type", "=", "out_invoice"), ("name", "in", name_chunk)],
                ["id", "name", "state", "invoice_origin", "ref"],
                limit=2000,
                offset=0,
            )
            for r in rows:
                key = (str(r.get("invoice_origin") or "").strip(), str(r.get("name") or "").strip())
                if key[0] and key[1]:
                    existing_by_key[key] = r

        filtered: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        unresolved = 0
        for so_ref, inv_ref, inv in flat:
            r = existing_by_key.get((so_ref, inv_ref))
            if (r is None) or (str(r.get("state") or "") == "draft"):
                filtered[so_ref].append(inv)
                unresolved += 1
        invoices_by_so = filtered
        print(f"INFO: gaps mode (sage_invoices={len(flat)}, unresolved={unresolved})")

    so_sequence = sorted(
        invoices_by_so.keys(),
        key=lambda ref: ((invoices_by_so[ref][0].get("TransactionDate") or "").strip(), ref),
    )

    seen = 0
    processed = 0
    status_counts: Dict[str, int] = defaultdict(int)

    def emit(processed_index: int, tx_date: str, status: str, so_ref: str, inv_ref: str, detail: str) -> None:
        _print_result(start_offset + processed_index, tx_date, status, so_ref, inv_ref, detail)

    for so_ref in so_sequence:
        so_rows = client.search_read(
            "sale.order",
            [("name", "=", so_ref)],
            ["id", "name", "state", "partner_id", "partner_invoice_id", "payment_term_id", "currency_id", "user_id", "team_id"],
            limit=1,
            offset=0,
        )
        tx_date = (invoices_by_so[so_ref][0].get("TransactionDate") or "").strip()
        if not so_rows:
            processed += 1
            status_counts["SKIP"] += 1
            emit(processed, tx_date, "SKIP", so_ref, "", "Sales Order not found in Odoo")
            continue

        so = so_rows[0]
        so_id = int(so.get("id") or 0)
        so_state = str(so.get("state") or "")
        if so_state not in {"sale", "done"}:
            processed += 1
            status_counts["ERROR"] += 1
            emit(processed, tx_date, "ERROR", so_ref, "", f"Sales Order not confirmed (state={so_state})")
            if not continue_on_error:
                break
            continue

        so_lines = _load_so_lines(client, so_id)
        pickings = _pickings_for_sale_order(client, so_id)

        for inv in invoices_by_so[so_ref]:
            seen += 1
            if seen <= start_offset:
                continue
            if max_orders is not None and processed >= max_orders:
                break

            inv_ref = (inv.get("Reference") or "").strip()
            inv_date = (inv.get("TransactionDate") or "").strip()
            ship_via = (inv.get("ShipVia") or "").strip()

            target_picking = _find_existing_for_invoice_tag(pickings, inv_ref)
            if not target_picking:
                processed += 1
                status_counts["ERROR"] += 1
                emit(processed, inv_date, "ERROR", so_ref, inv_ref, "No linked delivery found in Odoo for this Sage invoice")
                if not continue_on_error:
                    break
                continue

            target_state = str(target_picking.get("state") or "")
            if target_state != "done":
                processed += 1
                status_counts["ERROR"] += 1
                emit(processed, inv_date, "ERROR", so_ref, inv_ref, f"Linked delivery is not done (state={target_state})")
                if not continue_on_error:
                    break
                continue

            existing_inv = _existing_invoice_for_sage(client, so_ref, inv_ref)
            if existing_inv:
                existing_state = str(existing_inv.get("state") or "")
                if existing_state != "draft":
                    processed += 1
                    status_counts["NO_CHANGES"] += 1
                    pid = int(target_picking.get("id") or 0)
                    other = [p for p in sorted(pickings, key=lambda x: int(x.get("id") or 0)) if int(p.get("id") or 0) != pid]
                    detail_lines = _format_do_lines_for_invoice_log(
                        [_picking_line(target_picking, ship_via, inv_ref, True)] + [_picking_line(p, "", inv_ref, False) for p in other]
                    )
                    detail_lines = _attach_invoice_status(detail_lines, f"### Invoice {inv_ref} - {existing_state}")
                    detail_lines.append("already confirmed/posted, skipped")
                    emit(processed, inv_date, "NO_CHANGES", so_ref, inv_ref, "\n".join(detail_lines))
                    continue
                changed = False
                changed_user_team = False
                changed_freight_label = False
                update_vals: Dict[str, object] = {}
                customer_po = _sage_customer_po(inv)
                so_user = so.get("user_id") or []
                so_team = so.get("team_id") or []
                inv_user = existing_inv.get("invoice_user_id") or []
                inv_team = existing_inv.get("team_id") or []
                so_user_id = int(so_user[0]) if isinstance(so_user, list) and so_user else 0
                so_team_id = int(so_team[0]) if isinstance(so_team, list) and so_team else 0
                inv_user_id = int(inv_user[0]) if isinstance(inv_user, list) and inv_user else 0
                inv_team_id = int(inv_team[0]) if isinstance(inv_team, list) and inv_team else 0
                if so_user_id and so_user_id != inv_user_id:
                    update_vals["invoice_user_id"] = so_user_id
                if so_team_id and so_team_id != inv_team_id:
                    update_vals["team_id"] = so_team_id
                current_ref = str(existing_inv.get("ref") or "").strip()
                if current_ref != customer_po:
                    update_vals["ref"] = customer_po or False
                if update_vals:
                    client.models.execute_kw(
                        client.db,
                        client.uid,
                        client.apikey,
                        "account.move",
                        "write",
                        [[int(existing_inv["id"])], update_vals],
                    )
                    changed = True
                    changed_user_team = True
                if str(existing_inv.get("state") or "") == "draft":
                    # Keep draft invoices fully aligned with Sage content:
                    # rebuild invoice lines so line descriptions (name) and
                    # structure are refreshed even when totals already match.
                    commands, map_errors, sage_line_descriptions = _build_invoice_line_commands(
                        inv,
                        invoice_lines_by_postorder,
                        item_record_to_sku,
                        sku_to_product_info,
                        so_lines,
                        freight_product_id,
                        allow_already_invoiced=True,
                    )
                    if map_errors:
                        processed += 1
                        status_counts["ERROR"] += 1
                        pid = int(target_picking.get("id") or 0)
                        other = [p for p in sorted(pickings, key=lambda x: int(x.get("id") or 0)) if int(p.get("id") or 0) != pid]
                        detail_lines = _format_do_lines_for_invoice_log(
                            [_picking_line(target_picking, ship_via, inv_ref, True)] + [_picking_line(p, "", inv_ref, False) for p in other]
                        )
                        detail_lines = _attach_invoice_status(detail_lines, f"### Invoice {inv_ref} - {existing_inv.get('state')}")
                        detail_lines.append("unable to sync draft invoice lines from Sage")
                        detail_lines.append(" | ".join(map_errors[:5]))
                        emit(processed, inv_date, "ERROR", so_ref, inv_ref, "\n".join(detail_lines))
                        if not continue_on_error:
                            break
                        continue

                    write_vals: Dict[str, object] = {
                        "invoice_line_ids": [(5, 0, 0)] + commands,
                        "invoice_date": inv_date or False,
                        "invoice_date_due": inv_date or False,
                        "ref": customer_po or False,
                        "name": inv_ref,
                    }
                    if "invoice_user_id" in update_vals:
                        write_vals["invoice_user_id"] = update_vals["invoice_user_id"]
                    if "team_id" in update_vals:
                        write_vals["team_id"] = update_vals["team_id"]
                    payment_term = so.get("payment_term_id") or []
                    if isinstance(payment_term, list) and payment_term:
                        write_vals["invoice_payment_term_id"] = int(payment_term[0])
                    client.models.execute_kw(
                        client.db,
                        client.uid,
                        client.apikey,
                        "account.move",
                        "write",
                        [[int(existing_inv["id"])], write_vals],
                    )
                    if _force_invoice_line_descriptions(client, int(existing_inv["id"]), sage_line_descriptions):
                        changed = True
                    changed = True

                    # Keep exact Sage row descriptions in invoice line name.
                    # Do not normalize freight labels.
                sage_total = _sage_invoice_total(inv)
                refreshed_inv = client.models.execute_kw(
                    client.db,
                    client.uid,
                    client.apikey,
                    "account.move",
                    "read",
                    [[int(existing_inv["id"])]],
                    {"fields": ["id", "amount_total", "state"]},
                )
                existing_total = round(float((refreshed_inv[0] if refreshed_inv else {}).get("amount_total") or 0.0), 2)
                pid = int(target_picking.get("id") or 0)
                other = [p for p in sorted(pickings, key=lambda x: int(x.get("id") or 0)) if int(p.get("id") or 0) != pid]
                detail_lines = _format_do_lines_for_invoice_log(
                    [_picking_line(target_picking, ship_via, inv_ref, True)] + [_picking_line(p, "", inv_ref, False) for p in other]
                )
                detail_lines = _attach_invoice_status(detail_lines, f"### Invoice {inv_ref} - {existing_inv.get('state')}")
                if abs(existing_total - sage_total) > 0.01:
                    if str(existing_inv.get("state") or "") == "draft":
                        # Auto-heal draft invoices in-place (no delete/recreate):
                        # rebuild invoice lines from current Sage data.
                        commands, map_errors, sage_line_descriptions = _build_invoice_line_commands(
                            inv,
                            invoice_lines_by_postorder,
                            item_record_to_sku,
                            sku_to_product_info,
                            so_lines,
                            freight_product_id,
                            allow_already_invoiced=True,
                        )
                        if map_errors:
                            processed += 1
                            status_counts["ERROR"] += 1
                            detail_lines.append("unable to auto-fix draft invoice lines")
                            detail_lines.append(" | ".join(map_errors[:5]))
                            emit(processed, inv_date, "ERROR", so_ref, inv_ref, "\n".join(detail_lines))
                            if not continue_on_error:
                                break
                            continue

                        write_vals: Dict[str, object] = {
                            "invoice_line_ids": [(5, 0, 0)] + commands,
                            "invoice_date": inv_date or False,
                            "invoice_date_due": inv_date or False,
                            "ref": customer_po or False,
                            "name": inv_ref,
                        }
                        if "invoice_user_id" in update_vals:
                            write_vals["invoice_user_id"] = update_vals["invoice_user_id"]
                        if "team_id" in update_vals:
                            write_vals["team_id"] = update_vals["team_id"]
                        payment_term = so.get("payment_term_id") or []
                        if isinstance(payment_term, list) and payment_term:
                            write_vals["invoice_payment_term_id"] = int(payment_term[0])

                        client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "account.move",
                            "write",
                            [[int(existing_inv["id"])], write_vals],
                        )
                        _force_invoice_line_descriptions(client, int(existing_inv["id"]), sage_line_descriptions)

                        repaired = client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "account.move",
                            "read",
                            [[int(existing_inv["id"])]],
                            {"fields": ["id", "amount_total", "state"]},
                        )
                        repaired_total = round(float(repaired[0].get("amount_total") or 0.0), 2) if repaired else 0.0
                        processed += 1
                        if abs(repaired_total - sage_total) > 0.01:
                            status_counts["ERROR"] += 1
                            detail_lines.append(f"invoice total mismatch after repair: Sage={sage_total:.2f} Odoo={repaired_total:.2f}")
                            emit(processed, inv_date, "ERROR", so_ref, inv_ref, "\n".join(detail_lines))
                            if not continue_on_error:
                                break
                        else:
                            status_counts["OK"] += 1
                            detail_lines.append("draft invoice repaired in place")
                            emit(processed, inv_date, "UPDATED", so_ref, inv_ref, "\n".join(detail_lines))
                        continue
                    else:
                        processed += 1
                        status_counts["ERROR"] += 1
                        detail_lines.append(f"invoice total mismatch: Sage={sage_total:.2f} Odoo={existing_total:.2f}")
                        emit(processed, inv_date, "ERROR", so_ref, inv_ref, "\n".join(detail_lines))
                        if not continue_on_error:
                            break
                else:
                    processed += 1
                    if changed:
                        status_counts["OK"] += 1
                        if changed_user_team:
                            detail_lines.append("invoice salesperson/team updated")
                        if changed_freight_label:
                            detail_lines.append("freight label normalized")
                    if args.confirm and str(existing_inv.get("state") or "") == "draft":
                        client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "account.move",
                            "action_post",
                            [[int(existing_inv["id"])]],
                        )
                        changed = True
                        detail_lines = _replace_invoice_status(detail_lines, inv_ref, "confirmed")
                    if not changed:
                        status_counts["NO_CHANGES"] += 1
                    emit(processed, inv_date, "UPDATED" if changed else "NO_CHANGES", so_ref, inv_ref, "\n".join(detail_lines))
                    continue

            commands, map_errors, sage_line_descriptions = _build_invoice_line_commands(
                inv,
                invoice_lines_by_postorder,
                item_record_to_sku,
                sku_to_product_info,
                so_lines,
                freight_product_id,
            )
            if map_errors:
                processed += 1
                status_counts["ERROR"] += 1
                emit(processed, inv_date, "ERROR", so_ref, inv_ref, " | ".join(map_errors[:5]))
                if not continue_on_error:
                    break
                continue

            pid = int(target_picking.get("id") or 0)
            other = [p for p in sorted(pickings, key=lambda x: int(x.get("id") or 0)) if int(p.get("id") or 0) != pid]
            base_lines = _format_do_lines_for_invoice_log(
                [_picking_line(target_picking, ship_via, inv_ref, True)] + [_picking_line(p, "", inv_ref, False) for p in other]
            )

            if args.dry_run:
                processed += 1
                status_counts["DRY_RUN"] += 1
                detail_lines = _attach_invoice_status(base_lines, f"### Invoice {inv_ref} - draft (dry-run)")
                emit(processed, inv_date, "DRY_RUN", so_ref, inv_ref, "\n".join(detail_lines))
                continue

            partner_invoice = so.get("partner_invoice_id") or so.get("partner_id") or []
            partner_id = int(partner_invoice[0]) if isinstance(partner_invoice, list) and partner_invoice else 0
            if not partner_id:
                processed += 1
                status_counts["ERROR"] += 1
                emit(processed, inv_date, "ERROR", so_ref, inv_ref, "Missing invoice partner on Sales Order")
                if not continue_on_error:
                    break
                continue

            move_vals = {
                "move_type": "out_invoice",
                "partner_id": partner_id,
                # Keep Sage invoice number as the visible Odoo invoice identifier whenever possible.
                "name": inv_ref,
                "invoice_origin": so_ref,
                "invoice_date": inv_date or False,
                "ref": _sage_customer_po(inv) or False,
                "invoice_line_ids": commands,
                "invoice_date_due": inv_date or False,
            }
            so_user = so.get("user_id") or []
            if isinstance(so_user, list) and so_user:
                move_vals["invoice_user_id"] = int(so_user[0])
            so_team = so.get("team_id") or []
            if isinstance(so_team, list) and so_team:
                move_vals["team_id"] = int(so_team[0])
            payment_term = so.get("payment_term_id") or []
            if isinstance(payment_term, list) and payment_term:
                move_vals["invoice_payment_term_id"] = int(payment_term[0])

            inv_id = client.models.execute_kw(
                client.db,
                client.uid,
                client.apikey,
                "account.move",
                "create",
                [move_vals],
            )
            created = client.models.execute_kw(
                client.db,
                client.uid,
                client.apikey,
                "account.move",
                "read",
                [[int(inv_id)]],
                {"fields": ["id", "amount_total", "state"]},
            )
            _force_invoice_line_descriptions(client, int(inv_id), sage_line_descriptions)
            created_total = round(float(created[0].get("amount_total") or 0.0), 2) if created else 0.0
            sage_total = _sage_invoice_total(inv)
            if abs(created_total - sage_total) > 0.01:
                processed += 1
                status_counts["ERROR"] += 1
                detail_lines = _attach_invoice_status(base_lines, f"### Invoice {inv_ref} - draft")
                detail_lines.append(f"invoice total mismatch: Sage={sage_total:.2f} Odoo={created_total:.2f}")
                detail_lines.append("draft kept for manual review")
                emit(processed, inv_date, "ERROR", so_ref, inv_ref, "\n".join(detail_lines))
                if not continue_on_error:
                    break
                continue
            processed += 1
            status_counts["OK"] += 1
            detail_lines = _attach_invoice_status(base_lines, f"### Invoice {inv_ref} - draft")
            if args.confirm:
                client.models.execute_kw(
                    client.db,
                    client.uid,
                    client.apikey,
                    "account.move",
                    "action_post",
                    [[int(inv_id)]],
                )
                detail_lines = _replace_invoice_status(detail_lines, inv_ref, "confirmed")
            emit(processed, inv_date, "UPDATED", so_ref, inv_ref, "\n".join(detail_lines))

        if max_orders is not None and processed >= max_orders:
            break

    print(f"Processed {processed}/{max_orders if max_orders is not None else 'all'}.")
    print(
        "Summary: "
        f"{status_counts.get('NO_CHANGES', 0)} No changes | "
        f"{status_counts.get('OK', 0)} Updated | "
        f"{status_counts.get('SKIP', 0)} Skip | "
        f"{status_counts.get('DRY_RUN', 0)} Dry run | "
        f"{status_counts.get('ERROR', 0)} Error"
    )
    return 0


if __name__ == "__main__":
    parser = build_parser()
    raise SystemExit(run(parser.parse_args()))
