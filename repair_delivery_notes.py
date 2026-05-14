import argparse
import glob
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple

from sync_customers import get_env_value, load_env_file, read_csv
from sync_parity import OdooClient
from sync_delivery_orders_api import (
    _invoice_product_qty,
    _load_item_record_to_sku,
    _load_sku_to_product_info,
)


OPEN_STATES = {"waiting", "confirmed", "assigned", "partially_available"}


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


def _note_for_log(note: str) -> str:
    text = str(note or "").strip()
    if not text:
        return "<empty>"
    lines: List[str] = []
    for raw_ln in text.splitlines():
        plain = re.sub(r"(?is)<[^>]+>", "", raw_ln or "").strip()
        if plain:
            lines.append(plain)
    if not lines:
        return "<empty>"
    return " | ".join(lines)


def _notes_semantically_equal(left: str, right: str) -> bool:
    return _note_for_log(left) == _note_for_log(right)


def _replace_sage_invoice_note(existing_note: str, invoice_ref: str) -> str:
    text = str(existing_note or "")
    kept: List[str] = []
    seen = set()
    for raw_ln in text.splitlines():
        ln_raw = str(raw_ln or "").strip()
        if not ln_raw:
            continue
        plain = re.sub(r"(?is)<[^>]+>", "", ln_raw).strip()
        if not plain:
            continue
        if re.match(r"(?i)^sage\s*invoice\s*:\s*[A-Za-z0-9/\-]+\s*$", plain):
            continue
        key = plain.lower()
        if key in seen:
            continue
        seen.add(key)
        kept.append(plain)
    kept.append(f"Sage Invoice: {invoice_ref}")
    return "\n".join(kept).strip()


def _clear_sage_invoice_note(existing_note: str) -> str:
    text = str(existing_note or "")
    kept: List[str] = []
    seen = set()
    for raw_ln in text.splitlines():
        ln_raw = str(raw_ln or "").strip()
        if not ln_raw:
            continue
        plain = re.sub(r"(?is)<[^>]+>", "", ln_raw).strip()
        if not plain:
            continue
        if re.match(r"(?i)^sage\s*invoice\s*:\s*[A-Za-z0-9/\-]+\s*$", plain):
            continue
        key = plain.lower()
        if key in seen:
            continue
        seen.add(key)
        kept.append(plain)
    return "\n".join(kept).strip()


def _invoice_ref_sort_key(ref: str) -> Tuple[int, str]:
    r = (ref or "").strip()
    if "-" not in r:
        return (0, r)
    suffix = r.rsplit("-", 1)[-1].strip().upper()
    if len(suffix) == 1 and suffix.isalpha():
        return (1, suffix)
    return (2, suffix)


def _parse_dt(text: str) -> Tuple[int, str]:
    raw = (text or "").strip()
    if not raw:
        return (1, "")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return (0, datetime.strptime(raw[:19], fmt).isoformat())
        except Exception:
            pass
    return (1, raw)


def _fetch_all_pickings(client: OdooClient, batch_size: int = 500) -> List[Dict[str, object]]:
    fields = ["id", "name", "state", "sale_id", "note", "date_done", "create_date"]
    offset = 0
    all_rows: List[Dict[str, object]] = []
    while True:
        rows = client.search_read("stock.picking", [], fields, limit=batch_size, offset=offset)
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
    return all_rows


def _fetch_sale_names(client: OdooClient, sale_ids: List[int]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    if not sale_ids:
        return out
    step = 500
    for i in range(0, len(sale_ids), step):
        chunk = sale_ids[i : i + step]
        rows = client.search_read("sale.order", [("id", "in", chunk)], ["id", "name"], limit=len(chunk), offset=0)
        for r in rows:
            out[int(r["id"])] = str(r.get("name") or "")
    return out


def _load_sage_invoices(root_dir: str) -> Dict[str, List[Dict[str, str]]]:
    by_so: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    pattern = os.path.join(root_dir, "**", "*_invoice.csv")
    for path in glob.glob(pattern, recursive=True):
        if "_credit_note" in path.lower():
            continue
        try:
            _, rows = read_csv(path)
        except Exception:
            continue
        for h in rows:
            so_ref = (h.get("INV_POSOOrderNumber") or "").strip()
            inv_ref = (h.get("Reference") or "").strip()
            tdate = (h.get("TransactionDate") or "").strip()
            if so_ref and inv_ref:
                by_so[so_ref].append({"Reference": inv_ref, "TransactionDate": tdate})
    for so_ref, rows in by_so.items():
        uniq = {(r["Reference"], r["TransactionDate"]): r for r in rows}
        ordered = sorted(
            uniq.values(),
            key=lambda r: ((r.get("TransactionDate") or "").strip(), _invoice_ref_sort_key((r.get("Reference") or "").strip())),
        )
        by_so[so_ref] = ordered
    return by_so


def _load_sage_invoice_lines(root_dir: str) -> Dict[str, List[Dict[str, str]]]:
    by_postorder: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    pattern = os.path.join(root_dir, "**", "*_invoice_lines.csv")
    for path in glob.glob(pattern, recursive=True):
        if "_credit_note" in path.lower():
            continue
        try:
            _, rows = read_csv(path)
        except Exception:
            continue
        for r in rows:
            po = (r.get("PostOrder") or "").strip()
            if po:
                by_postorder[po].append(r)
    return by_postorder


def _read_done_qty_by_product(client: OdooClient, picking_id: int) -> Dict[int, float]:
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
        return {}
    move_ids = picks[0].get("move_ids") or []
    if not move_ids:
        return {}
    moves = client.models.execute_kw(
        client.db,
        client.uid,
        client.apikey,
        "stock.move",
        "read",
        [move_ids],
        {"fields": ["id", "product_id", "quantity", "state"]},
    )
    out: Dict[int, float] = defaultdict(float)
    for m in moves:
        if str(m.get("state") or "").strip() != "done":
            continue
        prod = m.get("product_id") or []
        pid = int(prod[0]) if isinstance(prod, list) and prod else 0
        if pid <= 0:
            continue
        qty = float(m.get("quantity") or 0.0)
        if abs(qty) <= 0.000001:
            continue
        out[pid] += qty
    return dict(out)


def _qty_maps_equal(a: Dict[int, float], b: Dict[int, float], tol: float = 0.0001) -> bool:
    keys = set(a.keys()) | set(b.keys())
    for k in keys:
        if abs(float(a.get(k, 0.0)) - float(b.get(k, 0.0))) > tol:
            return False
    return True


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
    sage_by_so = _load_sage_invoices(args.root_dir)
    sage_lines_by_postorder = _load_sage_invoice_lines(args.root_dir)
    item_record_to_sku = _load_item_record_to_sku(args.items_master)
    sku_to_product_info = _load_sku_to_product_info(client)
    target_so_refs = set()
    if (args.from_duplicates_csv or "").strip():
        dup_path = args.from_duplicates_csv.strip()
        if not os.path.exists(dup_path):
            print(f"ERROR: duplicates CSV not found: {dup_path}")
            return 2
        _, dup_rows = read_csv(dup_path)
        for r in dup_rows:
            so = (r.get("SaleOrderName") or "").strip()
            if so:
                target_so_refs.add(so)
        print(f"INFO: restricting scope to duplicates CSV SO refs: {len(target_so_refs)}")

    pickings = _fetch_all_pickings(client, args.batch_size)

    by_sale: Dict[int, List[Dict[str, object]]] = defaultdict(list)
    for p in pickings:
        sale = p.get("sale_id") or []
        if isinstance(sale, list) and sale:
            by_sale[int(sale[0])].append(p)

    sale_names = _fetch_sale_names(client, sorted(by_sale.keys()))
    touched = 0
    actions = 0
    for sale_id, picks in by_sale.items():
        so_ref = sale_names.get(sale_id, "").strip()
        if not so_ref:
            continue
        if target_so_refs and so_ref not in target_so_refs:
            continue
        invs = sage_by_so.get(so_ref, [])
        if not invs:
            continue

        done = [p for p in picks if str(p.get("state") or "").strip().lower() == "done"]
        if not done:
            continue
        done_sorted = sorted(
            done,
            key=lambda p: (
                _parse_dt(str(p.get("date_done") or "")),
                _parse_dt(str(p.get("create_date") or "")),
                int(p.get("id") or 0),
            ),
        )
        inv_refs = [x["Reference"] for x in invs]
        if args.verbose:
            print(f"SO {so_ref} ({sale_id}) -> done={len(done_sorted)} sage_invoices={len(inv_refs)}")

        # Build invoice content maps (physical product qty by product_id).
        inv_maps: List[Tuple[str, Dict[int, float]]] = []
        for inv in invs:
            inv_ref = (inv.get("Reference") or "").strip()
            qty_map, _errs = _invoice_product_qty(inv, sage_lines_by_postorder, item_record_to_sku, sku_to_product_info)
            inv_maps.append((inv_ref, qty_map))

        # Build done picking content maps.
        pick_maps: Dict[int, Dict[int, float]] = {}
        for p in done_sorted:
            pid = int(p.get("id") or 0)
            pick_maps[pid] = _read_done_qty_by_product(client, pid)

        # Match by content first; fallback to date order for unresolved cases.
        matched_ref_by_pid: Dict[int, str] = {}
        used_pids = set()
        for inv_ref, inv_map in inv_maps:
            candidates = []
            for p in done_sorted:
                pid = int(p.get("id") or 0)
                if pid in used_pids:
                    continue
                if _qty_maps_equal(inv_map, pick_maps.get(pid, {})):
                    candidates.append(p)
            if len(candidates) == 1:
                pid = int(candidates[0].get("id") or 0)
                matched_ref_by_pid[pid] = inv_ref
                used_pids.add(pid)
            elif len(candidates) > 1:
                # Prefer candidate already tagged with this exact ref.
                chosen = None
                for c in candidates:
                    if _extract_sage_invoice_ref(str(c.get("note") or "")).strip() == inv_ref:
                        chosen = c
                        break
                if not chosen:
                    chosen = sorted(
                        candidates,
                        key=lambda p: (
                            _parse_dt(str(p.get("date_done") or "")),
                            _parse_dt(str(p.get("create_date") or "")),
                            int(p.get("id") or 0),
                        ),
                    )[0]
                pid = int(chosen.get("id") or 0)
                matched_ref_by_pid[pid] = inv_ref
                used_pids.add(pid)

        # Fallback by order for unresolved done pickings/invoices.
        remaining_refs = [r for r, _m in inv_maps if r not in set(matched_ref_by_pid.values())]
        for p in done_sorted:
            pid = int(p.get("id") or 0)
            if pid in matched_ref_by_pid:
                continue
            desired_ref = remaining_refs.pop(0) if remaining_refs else ""
            if desired_ref:
                matched_ref_by_pid[pid] = desired_ref

        for p in done_sorted:
            pid = int(p.get("id") or 0)
            desired_ref = matched_ref_by_pid.get(pid, "")
            current_note = str(p.get("note") or "")
            current_ref = _extract_sage_invoice_ref(current_note)
            pname = str(p.get("name") or "")
            if desired_ref:
                new_note = _replace_sage_invoice_note(current_note, desired_ref)
                needs_set = (current_ref != desired_ref) or (new_note.strip() != current_note.strip())
                # Avoid noisy no-op lines when difference is only HTML formatting (<p>..</p>).
                if needs_set and current_ref == desired_ref and _notes_semantically_equal(current_note, new_note):
                    needs_set = False
                if needs_set:
                    actions += 1
                    print(
                        f"[SET] SO {so_ref} | {pname}#{pid} > {_note_for_log(current_note)} > {_note_for_log(new_note)}"
                    )
                    if args.apply:
                        client.models.execute_kw(
                            client.db, client.uid, client.apikey, "stock.picking", "write", [[pid], {"note": new_note}]
                        )
                        touched += 1
            else:
                # more done pickings than Sage invoices: clear stale tag
                new_note = _clear_sage_invoice_note(current_note)
                if new_note.strip() != current_note.strip():
                    actions += 1
                    print(
                        f"[CLEAR] SO {so_ref} | {pname}#{pid} -> clear stale Sage note (extra done picking) > {_note_for_log(current_note)} > {_note_for_log(new_note)}"
                    )
                    if args.apply:
                        client.models.execute_kw(
                            client.db, client.uid, client.apikey, "stock.picking", "write", [[pid], {"note": (new_note or False)}]
                        )
                        touched += 1

        # Open pickings should not keep Sage note.
        for p in picks:
            st = str(p.get("state") or "").strip().lower()
            if st not in OPEN_STATES:
                continue
            current_note = str(p.get("note") or "")
            new_note = _clear_sage_invoice_note(current_note)
            if new_note.strip() != current_note.strip():
                pid = int(p.get("id") or 0)
                pname = str(p.get("name") or "")
                actions += 1
                print(
                    f"[CLEAR] SO {so_ref} | {pname}#{pid} -> remove inherited Sage note (open picking) > {_note_for_log(current_note)} > {_note_for_log(new_note)}"
                )
                if args.apply:
                    client.models.execute_kw(
                        client.db, client.uid, client.apikey, "stock.picking", "write", [[pid], {"note": (new_note or False)}]
                    )
                    touched += 1

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"Done ({mode}): proposed_actions={actions}, updated={touched}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Repair duplicated/wrong Sage Invoice notes in Odoo Delivery Orders.")
    p.add_argument("--profile", default="STUDIOOPTYX")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--root-dir", default=r"ENZO-Sage50")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--items-master", default=r"ENZO-Sage50\_master_sage\items.csv")
    p.add_argument(
        "--from-duplicates-csv",
        default=r"ENZO-Sage50\_master_odoo\pickings_notes_duplicates.csv",
        help="Restrict processing to SO refs present in this duplicates CSV.",
    )
    p.add_argument("--apply", action="store_true", help="Apply changes in Odoo. Default is dry-run.")
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
