import argparse
import csv
import os
from datetime import datetime
from typing import Dict, List, Tuple
import html
import re
from difflib import SequenceMatcher
from collections import defaultdict

try:
    import xmlrpc.client as xmlrpc_client
except Exception:
    xmlrpc_client = None


DELIMITER = ";"


def truthy(value: str) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "t"}


def normalize_name(value: str) -> str:
    if not value:
        return ""
    v = html.unescape(value).lower()
    v = v.replace("&", " and ")
    v = re.sub(r"[^a-z0-9]+", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return v


def sanitize_external_id(value: str) -> str:
    if not value:
        return ""
    v = str(value).strip()
    if not v:
        return ""
    # Odoo XML IDs cannot contain spaces.
    v = re.sub(r"\s+", "_", v)
    return v


def parse_date(value: str):
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    # Strip common time suffixes if present.
    if " " in raw:
        raw = raw.split(" ", 1)[0].strip()
    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%m-%d-%Y",
        "%d-%m-%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None




def read_csv(path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=DELIMITER)
        rows = [dict(r) for r in reader]
        return reader.fieldnames or [], rows


def write_csv(path: str, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=DELIMITER)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def load_env_file(path: str) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def get_env_value(env: Dict[str, str], key: str, fallback: str = "") -> str:
    return os.environ.get(key) or env.get(key) or fallback


def refresh_sage(args: argparse.Namespace) -> int:
    env = load_env_file(".env")
    min_customer_since = get_env_value(env, "CUSTOMER_SINCE_MIN")
    min_last_invoice = get_env_value(env, "LAST_INVOICE_MIN")
    min_customer_since_date = parse_date(min_customer_since)
    min_last_invoice_date = parse_date(min_last_invoice)

    customers_master = args.customers_master
    items_master = args.items_master

    if not os.path.exists(customers_master):
        print(f"ERROR: customers master not found: {customers_master}")
        return 2
    if not os.path.exists(items_master):
        print(f"ERROR: items master not found: {items_master}")
        return 2

    customer_out = args.customers_out
    items_out = args.items_out

    existing_customers = {}
    if os.path.exists(customer_out):
        _, rows = read_csv(customer_out)
        for row in rows:
            key = row.get("CustomerRecordNumber", "").strip()
            if key:
                existing_customers[key] = row

    existing_items = {}
    if os.path.exists(items_out):
        _, rows = read_csv(items_out)
        for row in rows:
            key = row.get("ItemRecordNumber", "").strip()
            if key:
                existing_items[key] = row

    _, customer_rows = read_csv(customers_master)
    _, item_rows = read_csv(items_master)

    customer_fields = [
        "CustomerRecordNumber",
        "CustomerIsInactive",
        "CustomerSince",
        "LastInvoiceDate",
        "CustomerID",
        "Customer_Bill_Name",
        "OdooId",
        "OdooName",
        "Exclude",
        "LastLookupAt",
    ]
    item_fields = [
        "ItemRecordNumber",
        "ItemID",
        "ItemDescription",
        "ItemIsInactive",
        "OdooVariantId",
        "OdooName",
        "OdooColor",
        "Exclude",
        "LastLookupAt",
    ]

    out_customers: List[Dict[str, str]] = []
    for row in customer_rows:
        key = (row.get("CustomerRecordNumber") or "").strip()
        if not key:
            continue
        existing = existing_customers.get(key, {})
        out_customers.append({
            "CustomerRecordNumber": key,
            "CustomerIsInactive": (row.get("CustomerIsInactive") or "").strip(),
            "CustomerSince": (row.get("CustomerSince") or "").strip(),
            "LastInvoiceDate": (row.get("LastInvoiceDate") or "").strip(),
            "CustomerID": (row.get("CustomerID") or "").strip(),
            "Customer_Bill_Name": (row.get("Customer_Bill_Name") or "").strip(),
            "OdooId": (existing.get("OdooId") or "").strip(),
            "OdooName": (existing.get("OdooName") or "").strip(),
            "Exclude": (existing.get("Exclude") or "").strip(),
            "LastLookupAt": (existing.get("LastLookupAt") or "").strip(),
        })

    out_items: List[Dict[str, str]] = []
    for row in item_rows:
        key = (row.get("ItemRecordNumber") or "").strip()
        if not key:
            continue
        existing = existing_items.get(key, {})
        out_items.append({
            "ItemRecordNumber": key,
            "ItemID": (row.get("ItemID") or "").strip(),
            "ItemDescription": (row.get("ItemDescription") or "").strip(),
            "ItemIsInactive": (row.get("ItemIsInactive") or "").strip(),
            "OdooColor": (existing.get("OdooColor") or "").strip(),
            "OdooVariantId": (existing.get("OdooVariantId") or "").strip(),
            "OdooName": (existing.get("OdooName") or "").strip(),
            "Exclude": (existing.get("Exclude") or "").strip(),
            "LastLookupAt": (existing.get("LastLookupAt") or "").strip(),
        })

    out_customers.sort(key=lambda r: int(r["CustomerRecordNumber"]))
    out_items.sort(key=lambda r: int(r["ItemRecordNumber"]))

    write_csv(customer_out, customer_fields, out_customers)
    write_csv(items_out, item_fields, out_items)

    # Build customers_NEW files
    try:
        from openpyxl import load_workbook
    except Exception:
        load_workbook = None

    master_root = os.path.dirname(customer_out)
    odoo_imports = os.path.join(master_root, "odoo_imports")
    os.makedirs(odoo_imports, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d")
    customers_new_xlsx = os.path.join(odoo_imports, f"{stamp}_customers_NEW.xlsx")
    customers_new_min_xlsx = os.path.join(master_root, "customers_NEW.xlsx")
    template_path = os.path.join(master_root, "odoo_templates", "customers.xlsx")

    # Filter: active in Sage + no OdooId + optional date thresholds
    new_customers = [
        c for c in out_customers
        if (c.get("CustomerIsInactive") or "").strip() != "1"
        and not c.get("OdooId")
        and (
            not min_customer_since_date
            or (parse_date(c.get("CustomerSince")) or datetime.min.date()) >= min_customer_since_date
        )
        and (
            not min_last_invoice_date
            or (parse_date(c.get("LastInvoiceDate")) or datetime.min.date()) >= min_last_invoice_date
        )
    ]

    if load_workbook and os.path.exists(template_path):
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
        # Load Sage master for address details
        sage_master_path = customers_master
        sage_by_id = {}
        if os.path.exists(sage_master_path):
            _, sage_rows = read_csv(sage_master_path)
            for r in sage_rows:
                cid = (r.get("CustomerID") or "").strip()
                if cid:
                    sage_by_id[cid] = r
        # Load Address master (preferred over Cardholder_* when available)
        address_master_path = os.path.join(os.path.dirname(customers_master), "address.csv")
        address_by_customer_record = {}
        if os.path.exists(address_master_path):
            _, address_rows = read_csv(address_master_path)
            for r in address_rows:
                crn = (r.get("CustomerRecordNumber") or "").strip()
                if not crn:
                    continue
                addr_type = (r.get("AddressTypeNumber") or "").strip()
                if addr_type != "0":
                    continue
                existing = address_by_customer_record.get(crn)
                if not existing:
                    address_by_customer_record[crn] = r
                    continue
                # If multiple type-0 addresses exist, keep the lowest AddressRecordNumber
                try:
                    new_num = int((r.get("AddressRecordNumber") or "0").strip() or 0)
                except ValueError:
                    new_num = 0
                try:
                    old_num = int((existing.get("AddressRecordNumber") or "0").strip() or 0)
                except ValueError:
                    old_num = 0
                if new_num and (old_num == 0 or new_num < old_num):
                    address_by_customer_record[crn] = r
        # Load Contacts master to attach primary contact (child) rows
        contacts_master_path = os.path.join(os.path.dirname(customers_master), "contacts.csv")
        primary_contact_by_customer_record = {}
        if os.path.exists(contacts_master_path):
            _, contact_rows = read_csv(contacts_master_path)
            for r in contact_rows:
                crn = (r.get("CustomerRecord") or "").strip()
                if not crn:
                    continue
                if (r.get("IsPrimaryContact") or "").strip() != "1":
                    continue
                existing = primary_contact_by_customer_record.get(crn)
                if not existing:
                    primary_contact_by_customer_record[crn] = r
                    continue
                try:
                    new_num = int((r.get("RecordNumber") or "0").strip() or 0)
                except ValueError:
                    new_num = 0
                try:
                    old_num = int((existing.get("RecordNumber") or "0").strip() or 0)
                except ValueError:
                    old_num = 0
                if new_num and (old_num == 0 or new_num < old_num):
                    primary_contact_by_customer_record[crn] = r

        # Optional country parity for Odoo import (applied only to customers_NEW)
        parity_path = os.path.join(master_root, "country_parity.csv")
        country_parity = {}
        country_name_to_code = {}
        if os.path.exists(parity_path):
            with open(parity_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=DELIMITER)
                for row in reader:
                    raw = (row.get("sage_country_raw") or "").strip()
                    if not raw:
                        continue
                    odoo_code = (row.get("odoo_country_code") or "").strip()
                    if odoo_code:
                        country_parity[raw] = odoo_code
                    odoo_name = (row.get("odoo_country_name") or "").strip()
                    if odoo_name and odoo_code:
                        country_name_to_code[odoo_name] = odoo_code

        # Fallback: load Odoo country list for name->code mapping
        master_base = os.path.dirname(master_root)
        countries_export = os.path.join(master_base, "_master_odoo", "countries_odoo.csv")
        if os.path.exists(countries_export):
            with open(countries_export, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=DELIMITER)
                for row in reader:
                    name = (row.get("OdooName") or "").strip()
                    code = (row.get("OdooCode") or "").strip()
                    if name and code:
                        country_name_to_code.setdefault(name, code)

        # Optional state parity (state code -> full name + implied country)
        state_parity_path = os.path.join(master_root, "state_parity.csv")
        state_parity = {}
        if os.path.exists(state_parity_path):
            with open(state_parity_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=DELIMITER)
                for row in reader:
                    raw = (row.get("sage_state_raw") or "").strip()
                    if not raw:
                        continue
                    state_name = (row.get("odoo_state_name") or "").strip()
                    country_name = (row.get("odoo_country_name") or "").strip()
                    state_parity[raw] = {
                        "state_name": state_name,
                        "country_name": country_name,
                    }

        for c in new_customers:
            cid = c.get("CustomerID", "")
            name = c.get("Customer_Bill_Name", "")
            sage = sage_by_id.get(cid, {})
            crn = (c.get("CustomerRecordNumber") or "").strip()
            addr = address_by_customer_record.get(crn, {})
            def pick_addr(addr_key: str) -> str:
                return (addr.get(addr_key) or "").strip()
            ext_id = sanitize_external_id(cid)
            if not ext_id:
                ext_id = sanitize_external_id(crn)
            set_cell("External_ID", ext_id)
            set_cell("name", name)
            set_cell("is_company", 1)
            set_cell("company_name", name)
            raw_country = pick_addr("Country")
            mapped_country = country_parity.get(raw_country, "")
            if not mapped_country and raw_country:
                mapped_country = country_name_to_code.get(raw_country, "")
            if not mapped_country:
                mapped_country = raw_country
            raw_state = pick_addr("State")
            state_info = state_parity.get(raw_state, {})
            mapped_state = state_info.get("state_name") or raw_state
            if mapped_state and state_info.get("country_name"):
                country_code = country_name_to_code.get(state_info.get("country_name", ""), "")
                if country_code:
                    mapped_state = f"{mapped_state} ({country_code})"
            set_cell("country_id", mapped_country)
            # If country missing, infer from state (US/Canada/known from Odoo)
            if not mapped_country:
                country_name = (state_info.get("country_name") or "").strip()
                if country_name:
                    # Map country name to Odoo ISO code; do not fall back to Sage names
                    mapped_country = country_name_to_code.get(country_name, "")
                    if mapped_country:
                        set_cell("country_id", mapped_country)
            set_cell("state_id", mapped_state)
            set_cell("zip", pick_addr("Zip"))
            set_cell("city", pick_addr("City"))
            set_cell("street", pick_addr("AddressLine1"))
            set_cell("street2", pick_addr("AddressLine2"))
            set_cell("phone", (sage.get("Phone_Number") or "").strip())
            set_cell("email", (sage.get("eMail_Address") or "").strip())
            credit_msg = (sage.get("CreditStatusMsg") or "").strip()
            if credit_msg and credit_msg != "You have requested to be notified when a transaction is created for this customer.":
                set_cell("Notes", credit_msg)
            set_cell("Reference", cid)
            set_cell("Language", "English (US)")
            row_idx += 1
        wb.save(customers_new_xlsx)

    # Minimal XLSX in _master
    minimal_fields = [
        "CustomerRecordNumber",
        "CustomerSince",
        "LastInvoiceDate",
        "CustomerID",
        "Customer_Bill_Name",
    ]
    if load_workbook:
        from openpyxl import Workbook
        wb_min = Workbook()
        ws_min = wb_min.active
        ws_min.title = "customers_NEW"
        for col_idx, name in enumerate(minimal_fields, start=1):
            ws_min.cell(row=1, column=col_idx, value=name)
        row_idx = 2
        for c in new_customers:
            for col_idx, name in enumerate(minimal_fields, start=1):
                ws_min.cell(row=row_idx, column=col_idx, value=c.get(name, ""))
            row_idx += 1
        wb_min.save(customers_new_min_xlsx)

    print(f"OK: customers sync rows: {len(out_customers)} -> {customer_out}")
    print(f"OK: items sync rows: {len(out_items)} -> {items_out}")
    return 0


def build_contacts_import(args: argparse.Namespace) -> int:
    try:
        from openpyxl import load_workbook
    except Exception:
        load_workbook = None

    if load_workbook is None:
        print("ERROR: openpyxl not available for XLSX export")
        return 2

    env = load_env_file(args.env_file)
    url = get_env_value(env, "ODOO_STUDIOOPTYX_URL")
    db = get_env_value(env, "ODOO_STUDIOOPTYX_DB")
    user = get_env_value(env, "ODOO_STUDIOOPTYX_USER")
    apikey = get_env_value(env, "ODOO_STUDIOOPTYX_APIKEY")

    customers_sync = args.customers_sync
    customers_master = args.customers_master
    template_path = args.template_path

    if not os.path.exists(customers_sync):
        print(f"ERROR: customers sync file not found: {customers_sync}")
        return 2
    if not os.path.exists(customers_master):
        print(f"ERROR: customers master not found: {customers_master}")
        return 2
    if not os.path.exists(template_path):
        print(f"ERROR: template not found: {template_path}")
        return 2

    master_root = os.path.dirname(customers_sync)
    odoo_imports = os.path.join(master_root, "odoo_imports")
    os.makedirs(odoo_imports, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d")
    contacts_out_xlsx = os.path.join(odoo_imports, f"{stamp}_contacts_NEW.xlsx")

    # Load customers sync (needs OdooId to reference parent_id)
    _, sync_rows = read_csv(customers_sync)
    customers_by_record = {}
    for r in sync_rows:
        crn = (r.get("CustomerRecordNumber") or "").strip()
        if not crn:
            continue
        customers_by_record[crn] = r

    # Load Contacts master to attach primary contact (child) rows
    contacts_master_path = os.path.join(os.path.dirname(customers_master), "contacts.csv")
    primary_contact_by_customer_record = {}
    if os.path.exists(contacts_master_path):
        _, contact_rows = read_csv(contacts_master_path)
        for r in contact_rows:
            crn = (r.get("CustomerRecord") or "").strip()
            if not crn:
                continue
            if (r.get("IsPrimaryContact") or "").strip() != "1":
                continue
            existing = primary_contact_by_customer_record.get(crn)
            if not existing:
                primary_contact_by_customer_record[crn] = r
                continue
            try:
                new_num = int((r.get("RecordNumber") or "0").strip() or 0)
            except ValueError:
                new_num = 0
            try:
                old_num = int((existing.get("RecordNumber") or "0").strip() or 0)
            except ValueError:
                old_num = 0
            if new_num and (old_num == 0 or new_num < old_num):
                primary_contact_by_customer_record[crn] = r

    # Optionally load existing contacts from Odoo to avoid duplicates
    existing_by_parent = {}
    if args.skip_existing and (url and db and user and apikey):
        try:
            client = OdooClient(url, db, user, apikey)
            parent_ids = []
            for row in customers_by_record.values():
                oid = (row.get("OdooId") or "").strip()
                if oid:
                    try:
                        parent_ids.append(int(oid))
                    except ValueError:
                        continue
            chunk_size = 1000
            for i in range(0, len(parent_ids), chunk_size):
                chunk = parent_ids[i:i + chunk_size]
                offset = 0
                while True:
                    rows = client.search_read(
                        "res.partner",
                        [["parent_id", "in", chunk]],
                        ["id", "parent_id", "name", "email", "phone"],
                        limit=args.batch_size,
                        offset=offset,
                    )
                    if not rows:
                        break
                    for r in rows:
                        parent = r.get("parent_id") or []
                        pid = parent[0] if isinstance(parent, list) and parent else None
                        if not pid:
                            continue
                        existing_by_parent.setdefault(pid, []).append({
                            "name": (r.get("name") or "").strip(),
                            "email": (r.get("email") or "").strip(),
                            "phone": (r.get("phone") or "").strip(),
                        })
                    offset += len(rows)
        except Exception as exc:
            print(f"WARNING: failed to load Odoo contacts ({exc}); proceeding without de-dup")
    elif args.skip_existing:
        print("WARNING: missing Odoo credentials; proceeding without de-dup")

    def norm_email(value: str) -> str:
        return (value or "").strip().lower()

    def norm_phone(value: str) -> str:
        return re.sub(r"\\D+", "", (value or ""))

    def contact_exists(parent_id: int, name: str, email: str, phone: str) -> bool:
        existing = existing_by_parent.get(parent_id, [])
        if not existing:
            return False
        n_name = normalize_name(name)
        n_email = norm_email(email)
        n_phone = norm_phone(phone)
        for r in existing:
            r_email = norm_email(r.get("email", ""))
            r_phone = norm_phone(r.get("phone", ""))
            r_name = normalize_name(r.get("name", ""))
            if n_email and r_email and n_email == r_email:
                return True
            if n_name and r_name and n_name == r_name:
                if n_phone and r_phone and n_phone == r_phone:
                    return True
                if not n_email and not r_email:
                    return True
        return False

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
    for crn, customer in customers_by_record.items():
        if (customer.get("CustomerIsInactive") or "").strip() == "1":
            continue
        odoo_id = (customer.get("OdooId") or "").strip()
        if not odoo_id:
            continue
        primary = primary_contact_by_customer_record.get(crn)
        if not primary:
            continue

        try:
            parent_id_int = int(odoo_id)
        except ValueError:
            continue

        cid = (customer.get("CustomerID") or "").strip()
        name = (customer.get("Customer_Bill_Name") or "").strip()
        first = (primary.get("FirstName") or "").strip()
        last = (primary.get("LastName") or "").strip()
        contact_name = " ".join([p for p in [first, last] if p]).strip()
        if not contact_name:
            contact_name = (primary.get("CompanyName") or "").strip()
        if not contact_name:
            contact_name = f"{name} Contact" if name else "Contact"
        contact_rec = (primary.get("RecordNumber") or "").strip()

        contact_phone = (primary.get("Telephone1") or "").strip()
        contact_email = (primary.get("Email") or "").strip()
        if args.skip_existing and contact_exists(parent_id_int, contact_name, contact_email, contact_phone):
            continue

        raw_ext = f"{cid}_{contact_rec}" if contact_rec and cid else (f"{cid}_contact" if cid else "")
        set_cell("External_ID", sanitize_external_id(raw_ext))
        set_cell("ParentId", odoo_id)
        set_cell("ContactName", contact_name)
        set_cell("ContactEmail", contact_email)
        set_cell("ContactPhone", contact_phone)
        set_cell("ContactJobTitle", (primary.get("Title") or "").strip())
        set_cell("ContactNotes", (primary.get("Notes") or "").strip())
        set_cell("Language", "English (US)")
        row_idx += 1
        emitted += 1

    wb.save(contacts_out_xlsx)
    print(f"OK: contacts new rows: {emitted} -> {contacts_out_xlsx}")
    return 0


class OdooClient:
    def __init__(self, url: str, db: str, user: str, apikey: str):
        if xmlrpc_client is None:
            raise RuntimeError("xmlrpc.client unavailable")
        self.url = url.rstrip("/")
        self.db = db
        self.user = user
        self.apikey = apikey
        self.common = xmlrpc_client.ServerProxy(f"{self.url}/xmlrpc/2/common")
        self.models = xmlrpc_client.ServerProxy(f"{self.url}/xmlrpc/2/object")
        self.uid = self.common.authenticate(self.db, self.user, self.apikey, {})
        if not self.uid:
            raise RuntimeError("Odoo authentication failed")

    def search_read(
        self,
        model: str,
        domain: List,
        fields: List[str],
        limit: int = 2,
        offset: int = 0,
    ):
        return self.models.execute_kw(
            self.db,
            self.uid,
            self.apikey,
            model,
            "search_read",
            [domain],
            {"fields": fields, "limit": limit, "offset": offset},
        )


def refresh_odoo(args: argparse.Namespace) -> int:
    env = load_env_file(args.env_file)
    url = get_env_value(env, "ODOO_STUDIOOPTYX_URL")
    db = get_env_value(env, "ODOO_STUDIOOPTYX_DB")
    user = get_env_value(env, "ODOO_STUDIOOPTYX_USER")
    apikey = get_env_value(env, "ODOO_STUDIOOPTYX_APIKEY")

    if not (url and db and user and apikey):
        print("ERROR: missing Odoo credentials (URL/DB/USER/APIKEY)")
        return 2

    client = OdooClient(url, db, user, apikey)

    customers_out = args.customers_out
    items_out = args.items_out
    os.makedirs(os.path.dirname(customers_out), exist_ok=True)
    os.makedirs(os.path.dirname(items_out), exist_ok=True)

    batch = args.batch_size

    customer_fields = ["OdooId", "OdooName", "OdooRef", "Active"]
    with open(customers_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=customer_fields, delimiter=DELIMITER)
        writer.writeheader()
        offset = 0
        while True:
            rows = client.search_read(
                "res.partner",
                [],
                ["id", "name", "ref", "active"],
                limit=batch,
                offset=offset,
            )
            if not rows:
                break
            for r in rows:
                writer.writerow({
                    "OdooId": r.get("id", ""),
                    "OdooName": r.get("name", "") or "",
                    "OdooRef": r.get("ref", "") or "",
                    "Active": r.get("active", ""),
                })
            offset += len(rows)

    item_fields = ["OdooVariantId", "OdooName", "OdooVariantName", "OdooItemCode", "OdooColor", "Active"]
    with open(items_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=item_fields, delimiter=DELIMITER)
        writer.writeheader()
        offset = 0
        while True:
            rows = client.search_read(
                "product.product",
                [],
                ["id", "name", "display_name", "default_code", "active", "product_template_attribute_value_ids"],
                limit=batch,
                offset=offset,
            )
            if not rows:
                break
            attr_ids = set()
            for r in rows:
                for aid in r.get("product_template_attribute_value_ids") or []:
                    attr_ids.add(aid)
            attr_map = {}
            if attr_ids:
                ids = list(attr_ids)
                chunk_size = 1000
                for i in range(0, len(ids), chunk_size):
                    chunk = ids[i:i + chunk_size]
                    vals = client.models.execute_kw(
                        client.db,
                        client.uid,
                        client.apikey,
                        "product.template.attribute.value",
                        "read",
                        [chunk],
                        {"fields": ["name", "attribute_id"]},
                    )
                    for v in vals:
                        attr = v.get("attribute_id")
                        attr_name = attr[1] if isinstance(attr, list) and len(attr) > 1 else ""
                        attr_map[v.get("id")] = {
                            "name": v.get("name", ""),
                            "attribute": attr_name,
                        }
            for r in rows:
                color_values = []
                for aid in r.get("product_template_attribute_value_ids") or []:
                    info = attr_map.get(aid)
                    if info and (info.get("attribute") or "").lower() == "color":
                        color_values.append(info.get("name", ""))
                writer.writerow({
                    "OdooVariantId": r.get("id", ""),
                    "OdooName": r.get("name", "") or "",
                    "OdooVariantName": r.get("display_name", "") or "",
                    "OdooItemCode": r.get("default_code", "") or "",
                    "OdooColor": " / ".join([c for c in color_values if c]),
                    "Active": r.get("active", ""),
                })
            offset += len(rows)

    print(f"OK: odoo customers exported -> {customers_out}")
    print(f"OK: odoo items exported -> {items_out}")
    return 0


def export_countries(args: argparse.Namespace) -> int:
    env = load_env_file(args.env_file)
    url = get_env_value(env, "ODOO_STUDIOOPTYX_URL")
    db = get_env_value(env, "ODOO_STUDIOOPTYX_DB")
    user = get_env_value(env, "ODOO_STUDIOOPTYX_USER")
    apikey = get_env_value(env, "ODOO_STUDIOOPTYX_APIKEY")

    if not (url and db and user and apikey):
        print("ERROR: missing Odoo credentials (URL/DB/USER/APIKEY)")
        return 2

    client = OdooClient(url, db, user, apikey)

    customers_master = args.customers_master
    if not os.path.exists(customers_master):
        print(f"ERROR: customers master not found: {customers_master}")
        return 2

    master_root = os.path.dirname(args.customers_sync)
    master_odoo_root = os.path.dirname(args.odoo_customers)
    os.makedirs(master_odoo_root, exist_ok=True)

    countries_out = os.path.join(master_odoo_root, "countries_odoo.csv")
    states_out = os.path.join(master_odoo_root, "states_odoo.csv")
    parity_out = os.path.join(master_root, "country_parity.csv")
    state_parity_out = os.path.join(master_root, "state_parity.csv")

    # Export Odoo countries (id, name, code)
    fields = ["id", "name", "code"]
    offset = 0
    batch = args.batch_size
    countries = []
    while True:
        rows = client.search_read("res.country", [], fields, limit=batch, offset=offset)
        if not rows:
            break
        countries.extend(rows)
        offset += len(rows)

    countries.sort(key=lambda r: (r.get("name") or ""))
    with open(countries_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["OdooId", "OdooName", "OdooCode"], delimiter=DELIMITER)
        writer.writeheader()
        for r in countries:
            writer.writerow({
                "OdooId": r.get("id", ""),
                "OdooName": r.get("name", "") or "",
                "OdooCode": r.get("code", "") or "",
            })

    # Build parity from Sage Address.Country (AddressTypeNumber = 0 only)
    address_master_path = os.path.join(os.path.dirname(customers_master), "address.csv")
    if not os.path.exists(address_master_path):
        print(f"ERROR: address master not found: {address_master_path}")
        return 2

    _, address_rows = read_csv(address_master_path)
    sage_counts: Dict[str, int] = {}
    for r in address_rows:
        addr_type = (r.get("AddressTypeNumber") or "").strip()
        if addr_type != "0":
            continue
        raw = (r.get("Country") or "").strip()
        if not raw:
            continue
        sage_counts[raw] = sage_counts.get(raw, 0) + 1

    # Build quick match suggestions (exact match on ISO2 code or country name)
    by_code = {}
    by_name = {}
    for r in countries:
        code = (r.get("code") or "").strip().upper()
        name = (r.get("name") or "").strip().upper()
        if code:
            by_code[code] = r
        if name:
            by_name[name] = r

    def suggest(raw: str):
        key = raw.strip().upper()
        if key in by_code:
            return by_code[key], "code"
        if key in by_name:
            return by_name[key], "name"
        return None, ""

    with open(parity_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "sage_country_raw",
                "count_address",
                "odoo_country_name",
                "odoo_country_code",
                "match_type",
            ],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        for raw in sorted(sage_counts.keys(), key=lambda k: sage_counts[k], reverse=True):
            match, match_type = suggest(raw)
            writer.writerow({
                "sage_country_raw": raw,
                "count_address": sage_counts[raw],
                "odoo_country_name": (match.get("name") if match else "") or "",
                "odoo_country_code": (match.get("code") if match else "") or "",
                "match_type": match_type,
            })

    print(f"OK: odoo countries exported -> {countries_out}")
    print(f"OK: country parity (address only) -> {parity_out}")

    # Export Odoo states (id, name, code, country)
    state_fields = ["id", "name", "code", "country_id"]
    offset = 0
    states = []
    while True:
        rows = client.search_read("res.country.state", [], state_fields, limit=batch, offset=offset)
        if not rows:
            break
        states.extend(rows)
        offset += len(rows)

    with open(states_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["OdooId", "OdooName", "OdooCode", "OdooCountryId", "OdooCountryName"],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        for r in states:
            country = r.get("country_id") or []
            writer.writerow({
                "OdooId": r.get("id", ""),
                "OdooName": r.get("name", "") or "",
                "OdooCode": r.get("code", "") or "",
                "OdooCountryId": country[0] if isinstance(country, list) and country else "",
                "OdooCountryName": country[1] if isinstance(country, list) and len(country) > 1 else "",
            })

    # Build state parity from Sage Address.State (AddressTypeNumber = 0 only)
    sage_state_counts: Dict[str, int] = {}
    for r in address_rows:
        addr_type = (r.get("AddressTypeNumber") or "").strip()
        if addr_type != "0":
            continue
        raw = (r.get("State") or "").strip()
        if not raw:
            continue
        sage_state_counts[raw] = sage_state_counts.get(raw, 0) + 1

    by_state_code = {}
    for r in states:
        code = (r.get("code") or "").strip().upper()
        if not code:
            continue
        by_state_code.setdefault(code, []).append(r)

    def suggest_state(raw: str):
        key = raw.strip().upper()
        options = by_state_code.get(key, [])
        if not options:
            return None, ""
        # Prefer US/CA if multiple matches
        preferred = None
        for r in options:
            country = r.get("country_id") or []
            cname = country[1] if isinstance(country, list) and len(country) > 1 else ""
            ccode = ""
            if cname:
                if cname.lower() in {"united states", "united states of america"}:
                    preferred = (r, "code:US")
                    break
                if cname.lower() == "canada":
                    preferred = (r, "code:CA")
        if preferred:
            return preferred
        return options[0], "code"

    with open(state_parity_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "sage_state_raw",
                "count_address",
                "odoo_state_name",
                "odoo_state_code",
                "odoo_country_name",
                "match_type",
            ],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        for raw in sorted(sage_state_counts.keys(), key=lambda k: sage_state_counts[k], reverse=True):
            match, match_type = suggest_state(raw)
            country = match.get("country_id") if match else []
            writer.writerow({
                "sage_state_raw": raw,
                "count_address": sage_state_counts[raw],
                "odoo_state_name": (match.get("name") if match else "") or "",
                "odoo_state_code": (match.get("code") if match else "") or "",
                "odoo_country_name": (country[1] if isinstance(country, list) and len(country) > 1 else "") or "",
                "match_type": match_type,
            })

    print(f"OK: odoo states exported -> {states_out}")
    print(f"OK: state parity (address only) -> {state_parity_out}")
    return 0


def sync_local(args: argparse.Namespace) -> int:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    customer_sync = args.customers_sync
    item_sync = args.items_sync
    odoo_customers = args.odoo_customers
    odoo_items = args.odoo_items

    if not os.path.exists(customer_sync):
        print(f"ERROR: customers sync file not found: {customer_sync}")
        return 2
    if not os.path.exists(item_sync):
        print(f"ERROR: items sync file not found: {item_sync}")
        return 2
    if not os.path.exists(odoo_customers):
        print(f"ERROR: odoo customers file not found: {odoo_customers}")
        return 2
    if not os.path.exists(odoo_items):
        print(f"ERROR: odoo items file not found: {odoo_items}")
        return 2

    _, odoo_cust_rows = read_csv(odoo_customers)
    _, odoo_item_rows = read_csv(odoo_items)

    odoo_cust_by_ref: Dict[str, List[Dict[str, str]]] = {}
    odoo_cust_by_name: Dict[str, List[Dict[str, str]]] = {}
    for r in odoo_cust_rows:
        ref = (r.get("OdooRef") or "").strip()
        name = (r.get("OdooName") or "").strip()
        if ref:
            odoo_cust_by_ref.setdefault(ref, []).append(r)
        if name:
            odoo_cust_by_name.setdefault(name, []).append(r)

    odoo_item_by_code: Dict[str, List[Dict[str, str]]] = {}
    odoo_item_by_id: Dict[str, Dict[str, str]] = {}
    for r in odoo_item_rows:
        code = (r.get("OdooItemCode") or "").strip()
        if code:
            odoo_item_by_code.setdefault(code, []).append(r)
        oid = str(r.get("OdooVariantId") or "").strip()
        if oid:
            odoo_item_by_id[oid] = r

    customer_fields, customer_rows = read_csv(customer_sync)
    item_fields, item_rows = read_csv(item_sync)

    if "LastLookupAt" not in customer_fields:
        customer_fields.append("LastLookupAt")
    if "LastLookupAt" not in item_fields:
        item_fields.append("LastLookupAt")

    updated_customers = 0
    for row in customer_rows:
        if row.get("OdooId"):
            continue
        if truthy(row.get("Exclude")):
            continue
        customer_id = (row.get("CustomerID") or "").strip()
        customer_name = (row.get("Customer_Bill_Name") or "").strip()
        record = None
        if customer_id:
            matches = odoo_cust_by_ref.get(customer_id, [])
            if len(matches) == 1:
                record = matches[0]
        if record is None and args.customer_match_name and customer_name:
            matches = odoo_cust_by_name.get(customer_name, [])
            if len(matches) == 1:
                record = matches[0]
        row["LastLookupAt"] = now
        if record:
            row["OdooId"] = str(record.get("OdooId", ""))
            row["OdooName"] = record.get("OdooName", "") or ""
            updated_customers += 1

    updated_items = 0
    for row in item_rows:
        if row.get("OdooVariantId"):
            existing = odoo_item_by_id.get(str(row.get("OdooVariantId")).strip())
            if existing:
                if not row.get("OdooColor"):
                    row["OdooColor"] = existing.get("OdooColor", "") or ""
                if not row.get("OdooName"):
                    row["OdooName"] = existing.get("OdooName", "") or ""
            continue
        if truthy(row.get("Exclude")):
            continue
        item_id = (row.get("ItemID") or "").strip()
        if not item_id:
            continue
        matches = odoo_item_by_code.get(item_id, [])
        row["LastLookupAt"] = now
        if len(matches) == 1:
            record = matches[0]
            row["OdooVariantId"] = str(record.get("OdooVariantId", ""))
            row["OdooName"] = record.get("OdooName", "") or ""
            row["OdooColor"] = record.get("OdooColor", "") or ""
            updated_items += 1

    customer_rows.sort(key=lambda r: int(r["CustomerRecordNumber"]))
    item_rows.sort(key=lambda r: int(r["ItemRecordNumber"]))

    write_csv(customer_sync, customer_fields, customer_rows)
    write_csv(item_sync, item_fields, item_rows)

    print(f"OK: customers updated with Odoo IDs: {updated_customers}")
    print(f"OK: items updated with Odoo IDs: {updated_items}")

    # Build FAILS for Odoo customers/items not found in Sage
    customer_fails_path = os.path.join(os.path.dirname(customer_sync), "_customer_FAILS.csv")
    item_fails_path = os.path.join(os.path.dirname(item_sync), "_item_FAILS.csv")

    sage_customer_ids = set()
    for r in customer_rows:
        cid = (r.get("CustomerID") or "").strip()
        if cid:
            sage_customer_ids.add(cid)

    odoo_customers_fail = []
    for r in odoo_cust_rows:
        ref = (r.get("OdooRef") or "").strip()
        if not ref or ref not in sage_customer_ids:
            row = dict(r)
            row["Reason"] = "missing_ref" if not ref else "ref_not_in_sage"
            odoo_customers_fail.append(row)

    # Build Sage index for customer name suggestions
    cust_index = defaultdict(list)
    for r in customer_rows:
        name = r.get("Customer_Bill_Name", "")
        norm = normalize_name(name)
        if norm:
            cust_index[norm[0]].append({
                "CustomerRecordNumber": r.get("CustomerRecordNumber", ""),
                "CustomerID": r.get("CustomerID", ""),
                "Customer_Bill_Name": name,
                "NormName": norm,
            })

    def best_match_customer(norm_name_value: str):
        candidates = cust_index.get(norm_name_value[0], [])
        if not candidates:
            return None, 0.0
        best = None
        best_score = 0.0
        for entry in candidates:
            ratio = SequenceMatcher(a=norm_name_value, b=entry["NormName"]).ratio()
            if ratio > best_score:
                best_score = ratio
                best = entry
        return best, best_score

    customer_fail_fields = ["OdooId", "OdooName", "OdooRef", "Active", "Reason"]
    extra_cust_cols = [
        "SuggestedSageCustomerRecordNumber",
        "SuggestedSageCustomerID",
        "SuggestedSageCustomerName",
        "SuggestedSageScore",
    ]
    for c in extra_cust_cols:
        if c not in customer_fail_fields:
            customer_fail_fields.append(c)

    with open(customer_fails_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=customer_fail_fields, delimiter=DELIMITER)
        writer.writeheader()
        for r in odoo_customers_fail:
            name = r.get("OdooName", "")
            norm = normalize_name(name)
            if norm:
                match, score = best_match_customer(norm)
                if match and score >= 0.75:
                    r["SuggestedSageCustomerRecordNumber"] = match.get("CustomerRecordNumber", "")
                    r["SuggestedSageCustomerID"] = match.get("CustomerID", "")
                    r["SuggestedSageCustomerName"] = match.get("Customer_Bill_Name", "")
                    r["SuggestedSageScore"] = f"{score:.3f}"
            writer.writerow({k: r.get(k, "") for k in customer_fail_fields})

    # Items FAILS (Odoo products not found in Sage)
    sage_item_ids = set()
    for r in item_rows:
        iid = (r.get("ItemID") or "").strip()
        if iid:
            sage_item_ids.add(iid)

    odoo_items_fail = []
    for r in odoo_item_rows:
        code = (r.get("OdooItemCode") or "").strip()
        if not code or code not in sage_item_ids:
            row = dict(r)
            row["Reason"] = "missing_code" if not code else "code_not_in_sage"
            odoo_items_fail.append(row)

    item_index = defaultdict(list)
    for r in item_rows:
        name = r.get("ItemDescription", "")
        norm = normalize_name(name)
        if norm:
            item_index[norm[0]].append({
                "ItemRecordNumber": r.get("ItemRecordNumber", ""),
                "ItemID": r.get("ItemID", ""),
                "ItemDescription": name,
                "NormName": norm,
            })

    def best_match_item(norm_name_value: str):
        candidates = item_index.get(norm_name_value[0], [])
        if not candidates:
            return None, 0.0
        best = None
        best_score = 0.0
        for entry in candidates:
            ratio = SequenceMatcher(a=norm_name_value, b=entry["NormName"]).ratio()
            if ratio > best_score:
                best_score = ratio
                best = entry
        return best, best_score

    item_fail_fields = ["OdooVariantId", "OdooName", "OdooColor", "OdooItemCode", "Active", "Reason"]
    extra_item_cols = [
        "SuggestedSageItemRecordNumber",
        "SuggestedSageItemID",
        "SuggestedSageItemDescription",
        "SuggestedSageScore",
    ]
    for c in extra_item_cols:
        if c not in item_fail_fields:
            item_fail_fields.append(c)

    with open(item_fails_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=item_fail_fields, delimiter=DELIMITER)
        writer.writeheader()
        for r in odoo_items_fail:
            name = r.get("OdooVariantName", "") or r.get("OdooName", "")
            norm = normalize_name(name)
            if norm:
                match, score = best_match_item(norm)
                if match and score >= 0.75:
                    r["SuggestedSageItemRecordNumber"] = match.get("ItemRecordNumber", "")
                    r["SuggestedSageItemID"] = match.get("ItemID", "")
                    r["SuggestedSageItemDescription"] = match.get("ItemDescription", "")
                    r["SuggestedSageScore"] = f"{score:.3f}"
            writer.writerow({k: r.get(k, "") for k in item_fail_fields})

    print(f"OK: customer FAILS -> {customer_fails_path}")
    print(f"OK: item FAILS -> {item_fails_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sage ↔ Odoo parity tables")
    sub = parser.add_subparsers(dest="command", required=True)

    p1 = sub.add_parser("refresh_sage", help="Build sync tables from Sage master exports")
    p1.add_argument(
        "--customers-master",
        default=r"ENZO-Sage50\_master_sage\customers.csv",
    )
    p1.add_argument(
        "--items-master",
        default=r"ENZO-Sage50\_master_sage\items.csv",
    )
    p1.add_argument(
        "--customers-out",
        default=r"ENZO-Sage50\_master\customers_sync.csv",
    )
    p1.add_argument(
        "--items-out",
        default=r"ENZO-Sage50\_master\items_sync.csv",
    )
    p1.set_defaults(func=refresh_sage)

    p2 = sub.add_parser("refresh_odoo", help="Export Odoo masters to local CSVs")
    p2.add_argument(
        "--customers-out",
        default=r"ENZO-Sage50\_master_odoo\customers_odoo.csv",
    )
    p2.add_argument(
        "--items-out",
        default=r"ENZO-Sage50\_master_odoo\items_odoo.csv",
    )
    p2.add_argument(
        "--env-file",
        default=".env",
    )
    p2.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for Odoo export",
    )
    p2.set_defaults(func=refresh_odoo)

    p3 = sub.add_parser("sync", help="Match Sage sync tables with Odoo masters")
    p3.add_argument(
        "--customers-sync",
        default=r"ENZO-Sage50\_master\customers_sync.csv",
    )
    p3.add_argument(
        "--items-sync",
        default=r"ENZO-Sage50\_master\items_sync.csv",
    )
    p3.add_argument(
        "--odoo-customers",
        default=r"ENZO-Sage50\_master_odoo\customers_odoo.csv",
    )
    p3.add_argument(
        "--odoo-items",
        default=r"ENZO-Sage50\_master_odoo\items_odoo.csv",
    )
    p3.add_argument(
        "--customer-match-name",
        action="store_true",
        help="Allow exact name match if ref match fails",
    )
    p3.set_defaults(func=sync_local)

    p4 = sub.add_parser("build_contacts", help="Build contacts import file using Odoo parent IDs")
    p4.add_argument(
        "--customers-sync",
        default=r"ENZO-Sage50\_master\customers_sync.csv",
    )
    p4.add_argument(
        "--customers-master",
        default=r"ENZO-Sage50\_master_sage\customers.csv",
    )
    p4.add_argument(
        "--env-file",
        default=".env",
    )
    p4.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for Odoo contact lookup",
    )
    p4.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip contacts that already exist in Odoo under the same parent",
    )
    p4.add_argument(
        "--template-path",
        default=r"ENZO-Sage50\_master\odoo_templates\contacts.xlsx",
    )
    p4.set_defaults(func=build_contacts_import)

    p5 = sub.add_parser("export_countries", help="Export Odoo countries + build Sage parity table (address only)")
    p5.add_argument(
        "--customers-sync",
        default=r"ENZO-Sage50\_master\customers_sync.csv",
    )
    p5.add_argument(
        "--customers-master",
        default=r"ENZO-Sage50\_master_sage\customers.csv",
    )
    p5.add_argument(
        "--odoo-customers",
        default=r"ENZO-Sage50\_master_odoo\customers_odoo.csv",
    )
    p5.add_argument(
        "--env-file",
        default=".env",
    )
    p5.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for Odoo export",
    )
    p5.set_defaults(func=export_countries)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
