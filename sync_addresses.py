import argparse
import os
from datetime import datetime
from typing import Dict, List

from sync_customers import normalize_name, read_csv, sanitize_external_id, write_csv


def _norm_text(value: str) -> str:
    return normalize_name(value or "")


def _norm_addr(street: str, street2: str, city: str, state: str, zip_code: str) -> str:
    parts = [street, street2, city, state, zip_code]
    return " ".join([_norm_text(p) for p in parts if p]).strip()


def _load_customers_by_record(customers_master: str) -> Dict[str, Dict[str, str]]:
    if not os.path.exists(customers_master):
        return {}
    _, rows = read_csv(customers_master)
    by_record: Dict[str, Dict[str, str]] = {}
    for r in rows:
        crn = (r.get("CustomerRecordNumber") or "").strip()
        if crn:
            by_record[crn] = r
    return by_record


def _load_addresses_by_record(address_master: str) -> Dict[str, Dict[str, str]]:
    _, rows = read_csv(address_master)
    by_record: Dict[str, Dict[str, str]] = {}
    for r in rows:
        arn = (r.get("AddressRecordNumber") or "").strip()
        if arn:
            by_record[arn] = r
    return by_record


def _load_odoo_delivery_by_parent(odoo_delivery_csv: str) -> Dict[str, List[Dict[str, str]]]:
    existing: Dict[str, List[Dict[str, str]]] = {}
    if not os.path.exists(odoo_delivery_csv):
        return existing
    _, rows = read_csv(odoo_delivery_csv)
    for r in rows:
        parent_id = (r.get("ParentId") or "").strip()
        if not parent_id:
            continue
        existing.setdefault(parent_id, []).append(r)
    return existing


def _load_country_parity(parity_path: str) -> Dict[str, str]:
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


def _load_country_name_to_code(countries_odoo_path: str) -> Dict[str, str]:
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


def _load_state_parity(state_parity_path: str) -> Dict[str, Dict[str, str]]:
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


def _match_delivery(existing_rows: List[Dict[str, str]], name: str, addr_key: str) -> str:
    if not existing_rows:
        return ""
    n_name = _norm_text(name)
    for r in existing_rows:
        r_name = _norm_text(r.get("OdooName", ""))
        if n_name and r_name and n_name == r_name:
            return str(r.get("OdooId", ""))
    if addr_key:
        for r in existing_rows:
            r_key = _norm_addr(
                r.get("Street", ""),
                r.get("Street2", ""),
                r.get("City", ""),
                r.get("State", ""),
                r.get("Zip", ""),
            )
            if r_key and r_key == addr_key:
                return str(r.get("OdooId", ""))
    return ""


def build_addresses_sync(args: argparse.Namespace) -> int:
    contacts_master = args.contacts_master
    address_master = args.address_master
    customers_sync = args.customers_sync
    odoo_delivery = args.odoo_delivery
    country_parity_path = args.country_parity
    state_parity_path = args.state_parity
    countries_odoo_path = args.countries_odoo
    out_path = args.out_path

    if not os.path.exists(contacts_master):
        print(f"ERROR: contacts master not found: {contacts_master}")
        return 2
    if not os.path.exists(address_master):
        print(f"ERROR: address master not found: {address_master}")
        return 2

    customers_by_record = _load_customers_by_record(customers_sync)
    addresses_by_record = _load_addresses_by_record(address_master)
    existing_by_parent = _load_odoo_delivery_by_parent(odoo_delivery)
    country_parity = _load_country_parity(country_parity_path)
    country_name_to_code = _load_country_name_to_code(countries_odoo_path)
    state_parity = _load_state_parity(state_parity_path)

    _, contact_rows = read_csv(contacts_master)

    fieldnames = [
        "ContactRecordNumber",
        "CustomerRecordNumber",
        "CustomerID",
        "CustomerName",
        "AddressRecordNumber",
        "DeliveryName",
        "AddressTypeNumber",
        "AddressTypeDesc",
        "Email",
        "Phone",
        "Street",
        "Street2",
        "City",
        "State",
        "Zip",
        "Country",
        "OdooState",
        "OdooCountry",
        "Notes",
        "OdooAddressId",
        "OdooParentId",
        "LastLookupAt",
    ]

    rows_out: List[Dict[str, str]] = []
    now_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for c in contact_rows:
        customer_record = (c.get("CustomerRecord") or "").strip()
        address_record = (c.get("AddressRecordNumber") or "").strip()
        if not customer_record or not address_record:
            continue
        if (c.get("IsPrimaryContact") or "").strip() == "1":
            # Skip primary contact (company main row)
            continue
        delivery_name = (c.get("CompanyName") or "").strip()
        if not delivery_name:
            continue

        addr = addresses_by_record.get(address_record, {})
        cust = customers_by_record.get(customer_record, {})

        addr_key = _norm_addr(
            addr.get("AddressLine1", ""),
            addr.get("AddressLine2", ""),
            addr.get("City", ""),
            addr.get("State", ""),
            addr.get("Zip", ""),
        )

        raw_country = (addr.get("Country") or "").strip()
        mapped_country = country_parity.get(raw_country, "")
        if not mapped_country and raw_country:
            mapped_country = country_name_to_code.get(raw_country, "")
        if not mapped_country:
            mapped_country = raw_country

        raw_state = (addr.get("State") or "").strip()
        state_info = state_parity.get(raw_state, {})
        mapped_state = state_info.get("state_name") or raw_state
        if mapped_state and state_info.get("country_name"):
            inferred = country_name_to_code.get(state_info.get("country_name", ""), "")
            if inferred:
                mapped_state = f"{mapped_state} ({inferred})"
                if not mapped_country:
                    mapped_country = inferred

        odoo_parent_id = ""
        if cust:
            odoo_parent_id = (cust.get("OdooId") or "").strip()

        match_id = ""
        if odoo_parent_id:
            match_id = _match_delivery(existing_by_parent.get(odoo_parent_id, []), delivery_name, addr_key)

        rows_out.append({
            "ContactRecordNumber": (c.get("RecordNumber") or "").strip(),
            "CustomerRecordNumber": customer_record,
            "CustomerID": (cust.get("CustomerID") or "").strip(),
            "CustomerName": (cust.get("Customer_Bill_Name") or "").strip(),
            "AddressRecordNumber": address_record,
            "DeliveryName": delivery_name,
            "AddressTypeNumber": (addr.get("AddressTypeNumber") or "").strip(),
            "AddressTypeDesc": (addr.get("AddressTypeDesc") or "").strip(),
            "Email": (c.get("Email") or "").strip(),
            "Phone": (c.get("Telephone1") or "").strip(),
            "Street": (addr.get("AddressLine1") or "").strip(),
            "Street2": (addr.get("AddressLine2") or "").strip(),
            "City": (addr.get("City") or "").strip(),
            "State": raw_state,
            "Zip": (addr.get("Zip") or "").strip(),
            "Country": raw_country,
            "OdooState": mapped_state,
            "OdooCountry": mapped_country,
            "Notes": f"{addr.get('AddressTypeNumber','')} | {addr.get('AddressTypeDesc','')}".strip(),
            "OdooAddressId": match_id,
            "OdooParentId": odoo_parent_id,
            "LastLookupAt": now_stamp,
        })

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    write_csv(out_path, fieldnames, rows_out)
    print(f"OK: addresses sync rows: {len(rows_out)} -> {out_path}")
    return 0


def build_delivery_import(args: argparse.Namespace) -> int:
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
        print(f"ERROR: delivery sync file not found: {sync_path}")
        return 2
    if not os.path.exists(template_path):
        print(f"ERROR: template not found: {template_path}")
        return 2

    master_root = os.path.dirname(sync_path)
    odoo_imports = os.path.join(master_root, "odoo_imports")
    os.makedirs(odoo_imports, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d")
    out_xlsx = os.path.join(odoo_imports, f"{stamp}_customers_delivery_NEW.xlsx")
    out_csv = os.path.join(master_root, "customer_delivery_addresses_sync_NEW.csv")

    _, sync_rows = read_csv(sync_path)

    wb = load_workbook(template_path)
    ws = wb["Partners"] if "Partners" in wb.sheetnames else wb.active
    header_map = {}
    for col_idx in range(1, ws.max_column + 1):
        header = ws.cell(row=1, column=col_idx).value
        if header:
            header_map[str(header).strip()] = col_idx

    def set_cell(col_name: str, value: str) -> None:
        col_idx = header_map.get(col_name)
        if col_idx:
            ws.cell(row=row_idx, column=col_idx, value=value)

    ws.delete_rows(2, ws.max_row)
    row_idx = 2
    emitted = 0
    new_rows: List[Dict[str, str]] = []
    for r in sync_rows:
        if (r.get("OdooAddressId") or "").strip():
            continue
        parent_id = (r.get("OdooParentId") or "").strip()
        if not parent_id:
            continue
        name = (r.get("DeliveryName") or "").strip()
        if not name:
            continue
        new_rows.append(r)

        customer_id = (r.get("CustomerID") or "").strip()
        contact_record = (r.get("ContactRecordNumber") or "").strip()
        raw_ext = f"{customer_id}_{contact_record}" if customer_id and contact_record else ""
        ext_id = sanitize_external_id(raw_ext)

        state = (r.get("OdooState") or "").strip() or (r.get("State") or "").strip()
        country = (r.get("OdooCountry") or "").strip() or (r.get("Country") or "").strip()

        set_cell("External_ID", ext_id)
        set_cell("Parent/Database ID", parent_id)
        set_cell("Reference", (r.get("ContactRecordNumber") or "").strip())
        set_cell("is_company", 0)
        set_cell("type", "delivery")
        set_cell("Name", name)
        set_cell("Email", (r.get("Email") or "").strip())
        set_cell("Phone", (r.get("Phone") or "").strip())
        set_cell("Street", (r.get("Street") or "").strip())
        set_cell("Street2", (r.get("Street2") or "").strip())
        set_cell("City", (r.get("City") or "").strip())
        set_cell("State", state)
        set_cell("ZIP", (r.get("Zip") or "").strip())
        set_cell("Country", country)
        set_cell("Notes", (r.get("Notes") or "").strip())
        row_idx += 1
        emitted += 1

    wb.save(out_xlsx)
    fieldnames = []
    if new_rows:
        fieldnames = list(new_rows[0].keys())
    write_csv(out_csv, fieldnames, new_rows)
    print(f"OK: delivery new rows: {emitted} -> {out_xlsx}")
    print(f"OK: delivery NEW sync -> {out_csv}")
    return 0
