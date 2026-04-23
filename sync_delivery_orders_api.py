import argparse
import os
import re
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from sync_customers import get_env_value, load_env_file, read_csv
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
        return date(year, 2, 1)
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
        return date(year + 1, 2, 1)
    raise ValueError(f"Unsupported range boundary: {raw}")


def _parse_limit_offset(limit_arg: str, offset_arg: int) -> Tuple[Optional[int], int]:
    raw = str(limit_arg or "").strip()
    if not raw:
        return None, max(0, int(offset_arg or 0))
    if "," in raw:
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


def _invoice_tag(invoice_ref: str) -> str:
    return f"Sage Invoice: {invoice_ref}"


def _append_note(existing_note: str, tag: str) -> str:
    note = (existing_note or "").strip()
    if tag in note:
        return note
    if not note:
        return tag
    return note + "\n" + tag


def _extract_sage_invoice_from_note(note: str) -> str:
    text = str(note or "")
    m = re.search(r"Sage Invoice:\s*([A-Za-z0-9/\-]+)", text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def _norm_shipvia(value: str) -> str:
    return " ".join((value or "").strip().upper().split())


def _build_carrier_lookup(client: OdooClient) -> Dict[str, int]:
    rows = client.search_read("delivery.carrier", [], ["id", "name", "active"], limit=500, offset=0)
    out: Dict[str, int] = {}
    for r in rows:
        name = _norm_shipvia(str(r.get("name") or ""))
        cid = int(r.get("id") or 0)
        if name and cid and name not in out:
            out[name] = cid
    return out


def _resolve_carrier_id(ship_via: str, carriers_by_name: Dict[str, int]) -> int:
    raw = (ship_via or "").strip()
    if not raw:
        return 0
    key = _norm_shipvia(raw)
    if key in carriers_by_name:
        return int(carriers_by_name[key])
    aliases = {
        "USPS": "US MAIL",
        "USPS DOMESTIC": "US MAIL",
    }
    alias = aliases.get(key, "")
    if alias and alias in carriers_by_name:
        return int(carriers_by_name[alias])
    return 0


def _invoice_ref_sort_key(invoice_ref: str) -> Tuple[str, int, str]:
    """
    Sort invoice refs like:
    362492-A, 362492-B, ..., 362492-Z
    keeping natural delivery progression for the same SO.
    """
    ref = (invoice_ref or "").strip().upper()
    m = re.match(r"^(.*?)-([A-Z])$", ref)
    if not m:
        return ref, 0, ""
    base = m.group(1)
    suffix = m.group(2)
    return base, ord(suffix) - ord("A") + 1, suffix


def _build_file_paths(root_dir: str, y: int, m: int) -> Tuple[str, str, str]:
    month_names = {
        2: "01_02_Feb",
        3: "02_03_Mar",
        4: "03_04_Apr",
        5: "04_05_May",
        6: "05_06_Jun",
        7: "06_07_Jul",
        8: "07_08_Aug",
        9: "08_09_Sep",
        10: "09_10_Oct",
        11: "10_11_Nov",
        12: "11_12_Dec",
        1: "12_01_Jan",
    }
    folder = month_names.get(m)
    if not folder:
        return "", "", ""
    yymm = f"{y:04d}_{m:02d}"
    base = os.path.join(root_dir, "13_2026", folder)
    return (
        os.path.join(base, f"{yymm}_sales_orders_headers.csv"),
        os.path.join(base, f"{yymm}_invoice.csv"),
        os.path.join(base, f"{yymm}_invoice_lines.csv"),
    )


def _months_for_load(spec: str) -> List[Tuple[int, int]]:
    kind, payload = _parse_load_spec(spec)
    out: List[Tuple[int, int]] = []
    if kind == "day":
        d = payload
        out.append((d.year, d.month))
        return out
    if kind == "month":
        y, m = payload
        out.append((y, m))
        return out
    if kind in {"fiscal_year", "range"}:
        start, end = payload
        cur = date(start.year, start.month, 1)
        end_month = date(end.year, end.month, 1)
        while cur < end_month:
            out.append((cur.year, cur.month))
            cur = _first_day_next_month(cur.year, cur.month)
        return out
    return out


def _load_by_load(root_dir: str, load_spec: str):
    months = _months_for_load(load_spec)
    invoices: List[Dict[str, str]] = []
    lines_by_postorder: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    matched = 0
    for y, m in months:
        so_path, inv_path, lines_path = _build_file_paths(root_dir, y, m)
        if not (so_path and os.path.exists(so_path) and os.path.exists(inv_path) and os.path.exists(lines_path)):
            continue
        # Keep sales_orders_headers existence check to ensure this month folder is complete,
        # but deliveries are processed invoice-driven.
        _, _ = read_csv(so_path)
        _, inv_rows = read_csv(inv_path)
        _, line_rows = read_csv(lines_path)
        invoices.extend(inv_rows)
        for r in line_rows:
            po = (r.get("PostOrder") or "").strip()
            if po:
                lines_by_postorder[po].append(r)
        matched += 1
    for po in lines_by_postorder:
        lines_by_postorder[po].sort(key=lambda r: int((r.get("RowNumber") or "0").strip() or 0))
    print(f"INFO: auto-load matched files={matched}, invoices={len(invoices)}")
    return invoices, lines_by_postorder


def _load_item_record_to_sku(items_master: str) -> Dict[str, str]:
    _, rows = read_csv(items_master)
    out: Dict[str, str] = {}
    for r in rows:
        rec = (r.get("ItemRecordNumber") or "").strip()
        sku = (r.get("ItemID") or "").strip().upper()
        if rec and sku:
            out[rec] = sku
    return out


def _load_sku_to_product_info(client: OdooClient) -> Dict[str, Tuple[int, str]]:
    out: Dict[str, Tuple[int, str]] = {}
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
                "fields": ["id", "default_code", "type"],
                "limit": 2000,
                "offset": offset,
                "context": {"active_test": False},
            },
        )
        if not rows:
            break
        for r in rows:
            sku = str(r.get("default_code") or "").strip().upper()
            ptype = str(r.get("type") or "").strip()
            if sku and sku not in out:
                out[sku] = (int(r.get("id") or 0), ptype)
        offset += len(rows)
    return out


def _invoice_product_qty(
    invoice_header: Dict[str, str],
    invoice_lines_by_postorder: Dict[str, List[Dict[str, str]]],
    item_record_to_sku: Dict[str, str],
    sku_to_product_info: Dict[str, Tuple[int, str]],
) -> Tuple[Dict[int, float], List[str]]:
    post = (invoice_header.get("PostOrder") or "").strip()
    rows = invoice_lines_by_postorder.get(post, [])
    qty_by_pid: Dict[int, float] = defaultdict(float)
    errors: List[str] = []
    for r in rows:
        # Sage invoice lines can include duplicate technical shadow rows
        # (typically JournalRowEx=0 with Amount=0). Ignore them.
        jrx = (r.get("JournalRowEx") or "").strip()
        amt = parse_decimal(r.get("Amount") or "")
        if jrx == "0" and abs(amt) < 0.0001:
            continue
        item_record = (r.get("ItemRecordNumber") or "").strip()
        if not item_record or item_record == "0":
            continue
        qty = parse_decimal(r.get("Quantity") or "")
        if qty <= 0:
            continue
        sku = item_record_to_sku.get(item_record, "")
        if not sku:
            errors.append(f"ItemRecordNumber {item_record}: missing SKU in items.csv")
            continue
        info = sku_to_product_info.get(sku)
        if not info:
            errors.append(f"SKU {sku}: missing product in Odoo")
            continue
        pid, ptype = info
        # Deliveries should include physical stock moves only.
        # Services (discounts, freight, notes-like service products, etc.) do not belong to pickings.
        if str(ptype or "").lower() == "service":
            continue
        qty_by_pid[int(pid)] += qty
    return qty_by_pid, errors


def _pickings_for_sale_order(client: OdooClient, so_id: int) -> List[Dict[str, object]]:
    return client.search_read(
        "stock.picking",
        [("sale_id", "=", int(so_id))],
        ["id", "name", "state", "note", "scheduled_date", "date_done", "carrier_id"],
        limit=200,
        offset=0,
    )


def _as_effective_datetime(inv_date: str) -> str:
    raw = (inv_date or "").strip()
    if not raw:
        return ""
    try:
        d = date.fromisoformat(raw)
        # Midday avoids timezone day-shifts in most displays.
        return f"{d.isoformat()} 12:00:00"
    except ValueError:
        return ""


def _find_existing_for_invoice_tag(pickings: List[Dict[str, object]], invoice_ref: str) -> Optional[Dict[str, object]]:
    # Notes can be inherited to backorders, causing duplicate tags.
    # Prefer exact extracted invoice ref matches and, when ambiguous,
    # prefer already delivered pickings.
    candidates: List[Dict[str, object]] = []
    for p in pickings:
        note = str(p.get("note") or "")
        extracted = _extract_sage_invoice_from_note(note)
        if extracted == invoice_ref:
            candidates.append(p)
            continue
        if _invoice_tag(invoice_ref) in note:
            candidates.append(p)

    if not candidates:
        return None

    done = [p for p in candidates if str(p.get("state") or "") == "done"]
    if done:
        done.sort(key=lambda p: int(p.get("id") or 0))
        return done[0]

    candidates.sort(key=lambda p: int(p.get("id") or 0))
    return candidates[0]


def _choose_open_picking(pickings: List[Dict[str, object]]) -> Optional[Dict[str, object]]:
    open_states = {"waiting", "confirmed", "assigned", "partially_available"}
    candidates = [p for p in pickings if str(p.get("state") or "") in open_states]
    if not candidates:
        return None
    candidates.sort(key=lambda p: int(p.get("id") or 0))
    return candidates[0]


def _so_deliveries_brief(pickings: List[Dict[str, object]]) -> str:
    if not pickings:
        return ""
    rows = sorted(pickings, key=lambda p: int(p.get("id") or 0))
    parts = [f"{str(p.get('name') or '')}({str(p.get('state') or '')})" for p in rows if str(p.get("name") or "").strip()]
    if not parts:
        return ""
    return " | SO deliveries: " + ", ".join(parts)


def _picking_state_label(raw_state: str) -> str:
    state = str(raw_state or "").strip()
    mapping = {
        "confirmed": "waiting",
        "assigned": "waiting",
        "partially_available": "waiting",
        "waiting": "waiting",
        "done": "done",
        "cancel": "cancelled",
    }
    return mapping.get(state, state or "unknown")


def _picking_line(p: Dict[str, object], ship_via: str = "", current_invoice_ref: str = "", is_primary: bool = False) -> str:
    name = str(p.get("name") or "").strip()
    note_ref = _extract_sage_invoice_from_note(str(p.get("note") or "")).strip()
    if not note_ref:
        note_ref = "???????"
    elif (not is_primary) and current_invoice_ref and note_ref == current_invoice_ref:
        # Backorders can inherit parent note; avoid showing a misleading invoice link.
        note_ref = "???????"
    state = _picking_state_label(str(p.get("state") or ""))
    ship = (ship_via or "").strip()
    if ship:
        return f"{name} (SAGE {note_ref}) {ship} | {state}"
    return f"{name} (SAGE {note_ref}) | {state}"


def _read_moves(client: OdooClient, picking_id: int) -> List[Dict[str, object]]:
    picks = client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "stock.picking",
        "read",
        [[int(picking_id)]],
        {"fields": ["id", "move_ids"]},
    )
    if not picks:
        return []
    move_ids = picks[0].get("move_ids") or []
    if not move_ids:
        return []
    return client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "stock.move",
        "read",
        [move_ids],
        {"fields": ["id", "product_id", "product_uom_qty", "quantity", "state"]},
    )


def _apply_invoice_quantities_to_picking(
    client: OdooClient,
    picking_id: int,
    desired_qty_by_pid: Dict[int, float],
) -> Tuple[bool, str]:
    moves = _read_moves(client, picking_id)
    if not moves:
        return False, "No stock moves found in candidate picking"

    moves_by_pid: Dict[int, List[Dict[str, object]]] = defaultdict(list)
    for m in moves:
        prod = m.get("product_id") or []
        pid = int(prod[0]) if isinstance(prod, list) and prod else 0
        if pid > 0 and str(m.get("state") or "") not in {"done", "cancel"}:
            moves_by_pid[pid].append(m)

    missing_products = [pid for pid in desired_qty_by_pid.keys() if pid not in moves_by_pid]
    if missing_products:
        return False, "Invoice has products not present in open picking moves: " + ", ".join(str(x) for x in missing_products)

    remaining = dict(desired_qty_by_pid)
    write_rows: List[Tuple[int, float]] = []
    for pid, move_rows in moves_by_pid.items():
        needed = float(remaining.get(pid, 0.0))
        for m in move_rows:
            demand = float(m.get("product_uom_qty") or 0.0)
            take = 0.0
            if needed > 0:
                take = min(needed, demand)
                needed = round(needed - take, 6)
            write_rows.append((int(m.get("id") or 0), float(take)))
        remaining[pid] = needed

    still_needed = {pid: qty for pid, qty in remaining.items() if qty > 0.0001}
    if still_needed:
        return False, "Invoice quantity exceeds remaining move demand for product ids: " + ", ".join(
            f"{pid}({qty:g})" for pid, qty in still_needed.items()
        )

    for mid, qty in write_rows:
        client.models.execute_kw(
            client.db,
            client.uid,
            client.apikey,
            "stock.move",
            "write",
            [[mid], {"quantity": qty}],
        )
    return True, ""


def _process_validate_wizard(client: OdooClient, action: object, picking_id: int) -> Tuple[bool, str]:
    if not isinstance(action, dict):
        return True, ""
    model = str(action.get("res_model") or "")
    res_id = int(action.get("res_id") or 0)
    if not model:
        return False, f"Unsupported validation action response: {action}"
    # Some actions return no explicit res_id but include default_pick_ids in context.
    if res_id <= 0 and model == "stock.backorder.confirmation":
        ctx = action.get("context") or {}
        pick_ids = ctx.get("default_pick_ids") or []
        if isinstance(pick_ids, list) and pick_ids and isinstance(pick_ids[0], list):
            generated = client.models.execute_kw(
                client.db,
                client.uid,
                client.apikey,
                model,
                "create",
                [{"pick_ids": pick_ids}],
            )
            res_id = int(generated or 0)
    if res_id <= 0:
        return False, f"Unsupported validation action response: {action}"
    if model == "stock.immediate.transfer":
        client.models.execute_kw(
            client.db,
            client.uid,
            client.apikey,
            model,
            "process",
            [[res_id]],
        )
        return True, ""
    if model == "stock.backorder.confirmation":
        client.models.execute_kw(
            client.db,
            client.uid,
            client.apikey,
            model,
            "process",
            [[res_id]],
            {"context": {"button_validate_picking_ids": [int(picking_id)]}},
        )
        return True, ""
    if model == "confirm.stock.sms":
        # Some Odoo versions wrap validation with an SMS confirmation wizard.
        for method in ["action_confirm", "process", "action_validate"]:
            try:
                client.models.execute_kw(
                    client.db,
                    client.uid,
                    client.apikey,
                    model,
                    method,
                    [[res_id]],
                )
                return True, ""
            except Exception:
                continue
        return False, "Unsupported SMS confirmation flow for delivery validation"
    return False, f"Unsupported validation wizard model: {model}"


def _print_result(index: int, tx_date: str, status: str, so_ref: str, inv_ref: str, detail: str) -> None:
    prefix = f"[{index:03d}]"
    if tx_date:
        prefix += f" - {tx_date}"
    line = f"{prefix} - [{status}] SO {so_ref}"
    if (inv_ref or "").strip():
        line += f" | Invoice {inv_ref}"
    print(line)
    if detail:
        # Keep compact statuses in one single line; split only for errors.
        if status == "ERROR":
            for part in [p.strip() for p in detail.split("; ") if p.strip()]:
                print(f"        - {part}")
        else:
            if "\n" in detail:
                for part in [p.strip() for p in detail.splitlines() if p.strip()]:
                    print(f"        - {part}")
            else:
                print(f"        - {detail.strip()}")
    print("")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Create/validate Odoo Delivery Orders from Sage invoices",
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
    p.add_argument(
        "--validate",
        action="store_true",
        help="Validate (mark as shipped) the delivery after applying invoice quantities. Default: do NOT validate.",
    )
    p.add_argument("--items-master", default=r"ENZO-Sage50\_master_sage\items.csv")
    return p


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
    carriers_by_name = _build_carrier_lookup(client)

    invoices_by_so: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for h in invoice_headers:
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

    # Process SO groups in chronological order of their first invoice in range.
    so_sequence = sorted(
        invoices_by_so.keys(),
        key=lambda ref: (
            (invoices_by_so[ref][0].get("TransactionDate") or "").strip(),
            ref,
        ),
    )

    seen = 0
    processed = 0
    status_counts: Dict[str, int] = defaultdict(int)

    def emit(processed_index: int, tx_date: str, status: str, so_ref: str, inv_ref: str, detail: str) -> None:
        _print_result(start_offset + processed_index, tx_date, status, so_ref, inv_ref, detail)

    for so_ref in so_sequence:
        seen += 1
        if seen <= start_offset:
            continue
        if max_orders is not None and processed >= max_orders:
            break

        tx_date = (invoices_by_so[so_ref][0].get("TransactionDate") or "").strip()
        so_rows = client.search_read("sale.order", [("name", "=", so_ref)], ["id", "name", "state"], limit=1, offset=0)
        if not so_rows:
            processed += 1
            status_counts["SKIP"] += 1
            emit(processed, tx_date, "SKIP", so_ref, "*", "Sales Order not found in Odoo")
            continue

        so_id = int(so_rows[0]["id"])
        so_state = str(so_rows[0].get("state") or "").strip()
        if so_state not in {"sale", "done"}:
            processed += 1
            status_counts["ERROR"] += 1
            emit(processed, tx_date, "ERROR", so_ref, "*", f"Sales Order not confirmed (state={so_state})")
            if not continue_on_error:
                break
            continue

        for inv in invoices_by_so[so_ref]:
            if max_orders is not None and processed >= max_orders:
                break
            inv_ref = (inv.get("Reference") or "").strip()
            inv_date = (inv.get("TransactionDate") or "").strip()
            ship_via = (inv.get("ShipVia") or "").strip()
            carrier_id = _resolve_carrier_id(ship_via, carriers_by_name)
            carrier_info = ""
            if ship_via and carrier_id:
                carrier_info = f"carrier={ship_via}"
            elif ship_via and not carrier_id:
                carrier_info = f"carrier not found for ShipVia={ship_via}"
            qty_by_pid, map_errors = _invoice_product_qty(inv, invoice_lines_by_postorder, item_record_to_sku, sku_to_product_info)
            if map_errors:
                processed += 1
                status_counts["ERROR"] += 1
                emit(processed, inv_date, "ERROR", so_ref, inv_ref, " | ".join(map_errors[:5]))
                if not continue_on_error:
                    break
                continue

            pickings = _pickings_for_sale_order(client, so_id)
            existing = _find_existing_for_invoice_tag(pickings, inv_ref)
            if existing:
                existing_id = int(existing.get("id") or 0)
                existing_state = str(existing.get("state") or "")
                existing_date_done = str(existing.get("date_done") or "")
                note_invoice_ref = _extract_sage_invoice_from_note(str(existing.get("note") or ""))
                picking_name = str(existing.get("name") or "")
                picking_delivery_flag = "delivered" if existing_state == "done" else "not delivered"
                sage_ref_text = note_invoice_ref or inv_ref
                carrier_text = ship_via or ""
                carrier_updated = False
                effective_updated = False
                effective_dt = _as_effective_datetime(inv_date)
                if carrier_id and existing_id > 0:
                    raw_existing_carrier = existing.get("carrier_id")
                    existing_carrier_id = 0
                    if isinstance(raw_existing_carrier, list) and raw_existing_carrier:
                        existing_carrier_id = int(raw_existing_carrier[0] or 0)
                    elif isinstance(raw_existing_carrier, int):
                        existing_carrier_id = int(raw_existing_carrier or 0)
                    if existing_carrier_id != int(carrier_id):
                        client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "stock.picking",
                            "write",
                            [[existing_id], {"carrier_id": int(carrier_id)}],
                        )
                        carrier_updated = True
                if existing_state == "done" and existing_id > 0 and effective_dt:
                    if not existing_date_done.startswith((inv_date or "").strip()):
                        client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "stock.picking",
                            "write",
                            [[existing_id], {"date_done": effective_dt}],
                        )
                        effective_updated = True
                if args.validate and existing_state not in {"done", "cancel"} and existing_id > 0:
                    try:
                        client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "stock.picking",
                            "action_assign",
                            [[existing_id]],
                        )
                        ok, err = _apply_invoice_quantities_to_picking(client, existing_id, qty_by_pid)
                        if not ok:
                            processed += 1
                            status_counts["ERROR"] += 1
                            emit(processed, inv_date, "ERROR", so_ref, inv_ref, err)
                            if not continue_on_error:
                                break
                            continue

                        validation = client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "stock.picking",
                            "button_validate",
                            [[existing_id]],
                            {"context": {"skip_sms": True, "button_validate_picking_ids": [existing_id]}},
                        )
                        ok2, err2 = _process_validate_wizard(client, validation, existing_id)
                        if not ok2:
                            processed += 1
                            status_counts["ERROR"] += 1
                            emit(processed, inv_date, "ERROR", so_ref, inv_ref, err2)
                            if not continue_on_error:
                                break
                            continue
                        if effective_dt:
                            client.models.execute_kw(
                                client.db,
                                client.uid,
                                client.apikey,
                                "stock.picking",
                                "write",
                                [[existing_id], {"date_done": effective_dt}],
                            )
                            effective_updated = True
                        all_pickings = sorted(pickings, key=lambda p: int(p.get("id") or 0))
                        others = [p for p in all_pickings if int(p.get("id") or 0) != existing_id]
                        current = dict(existing)
                        current["state"] = "done"
                        detail_lines = [
                            _picking_line(current, carrier_text, inv_ref, True)
                        ] + [
                            _picking_line(p, "", inv_ref, False) for p in others
                        ]
                        if carrier_updated:
                            detail_lines.append("carrier updated")
                        if effective_updated:
                            detail_lines.append("effective date updated")
                        processed += 1
                        status_counts["OK"] += 1
                        emit(
                            processed,
                            inv_date,
                            "OK",
                            so_ref,
                            "",
                            "\n".join(detail_lines),
                        )
                    except Exception as exc:
                        processed += 1
                        status_counts["ERROR"] += 1
                        emit(processed, inv_date, "ERROR", so_ref, inv_ref, f"{type(exc).__name__}: {exc}")
                        if not continue_on_error:
                            break
                    continue

                processed += 1
                changed_existing = bool(carrier_updated or effective_updated)
                if changed_existing:
                    status_counts["OK"] += 1
                else:
                    status_counts["NO_CHANGES"] += 1
                all_pickings = sorted(pickings, key=lambda p: int(p.get("id") or 0))
                others = [p for p in all_pickings if int(p.get("id") or 0) != existing_id]
                detail_lines = [
                    _picking_line(existing, carrier_text, inv_ref, True)
                ] + [
                    _picking_line(p, "", inv_ref, False) for p in others
                ]
                if carrier_updated:
                    detail_lines.append("carrier updated")
                if effective_updated:
                    detail_lines.append("effective date updated")
                emit(
                    processed,
                    inv_date,
                    "UPDATED" if changed_existing else "NO_CHANGES",
                    so_ref,
                    "",
                    "\n".join(detail_lines),
                )
                continue

            candidate = _choose_open_picking(pickings)
            if not candidate:
                processed += 1
                status_counts["ERROR"] += 1
                emit(
                    processed,
                    inv_date,
                    "ERROR",
                    so_ref,
                    inv_ref,
                    "No open picking available for this Sales Order (script does not create deliveries)",
                )
                if not continue_on_error:
                    break
                continue

            picking_id = int(candidate.get("id") or 0)
            detail = f"Picking {candidate.get('name')}"
            if args.dry_run:
                processed += 1
                status_counts["DRY_RUN"] += 1
                emit(processed, inv_date, "DRY_RUN", so_ref, inv_ref, detail)
                continue

            try:
                client.models.execute_kw(
                    client.db,
                    client.uid,
                    client.apikey,
                    "stock.picking",
                    "action_assign",
                    [[picking_id]],
                )
                ok, err = _apply_invoice_quantities_to_picking(client, picking_id, qty_by_pid)
                if not ok:
                    processed += 1
                    status_counts["ERROR"] += 1
                    emit(processed, inv_date, "ERROR", so_ref, inv_ref, err)
                    if not continue_on_error:
                        break
                    continue

                existing_note = str(candidate.get("note") or "")
                new_note = _append_note(existing_note, _invoice_tag(inv_ref))
                write_vals = {"note": new_note}
                if carrier_id:
                    write_vals["carrier_id"] = int(carrier_id)
                client.models.execute_kw(
                    client.db,
                    client.uid,
                    client.apikey,
                    "stock.picking",
                    "write",
                    [[picking_id], write_vals],
                )

                if args.validate:
                    validation = client.models.execute_kw(
                        client.db,
                        client.uid,
                        client.apikey,
                        "stock.picking",
                        "button_validate",
                        [[picking_id]],
                        {"context": {"skip_sms": True, "button_validate_picking_ids": [picking_id]}},
                    )
                    ok2, err2 = _process_validate_wizard(client, validation, picking_id)
                    if not ok2:
                        processed += 1
                        status_counts["ERROR"] += 1
                        emit(processed, inv_date, "ERROR", so_ref, inv_ref, err2)
                        if not continue_on_error:
                            break
                        continue
                    effective_dt = _as_effective_datetime(inv_date)
                    if effective_dt:
                        client.models.execute_kw(
                            client.db,
                            client.uid,
                            client.apikey,
                            "stock.picking",
                            "write",
                            [[picking_id], {"date_done": effective_dt}],
                        )

                processed += 1
                status_counts["OK"] += 1
                emit(
                    processed,
                    inv_date,
                    "OK",
                    so_ref,
                    inv_ref,
                    detail
                    + (f"; {carrier_info}" if carrier_info else "")
                    + ("; validated" if args.validate else "; prepared (not validated)"),
                )
            except Exception as exc:
                processed += 1
                status_counts["ERROR"] += 1
                emit(processed, inv_date, "ERROR", so_ref, inv_ref, f"{type(exc).__name__}: {exc}")
                if not continue_on_error:
                    break

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
