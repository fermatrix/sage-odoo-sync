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

    customers_new_xlsx = os.path.join(odoo_imports, "customers_NEW.xlsx")
    customers_new_min_xlsx = os.path.join(master_root, "customers_NEW.xlsx")
    template_path = os.path.join(master_root, "odoo_templates", "customers.xlsx")

    # Filter: active in Sage + no OdooId
    new_customers = [
        c for c in out_customers
        if (c.get("CustomerIsInactive") or "").strip() != "1" and not c.get("OdooId")
    ]

    if load_workbook and os.path.exists(template_path):
        wb = load_workbook(template_path)
        ws = wb["Partners"] if "Partners" in wb.sheetnames else wb.active
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

        for c in new_customers:
            cid = c.get("CustomerID", "")
            name = c.get("Customer_Bill_Name", "")
            sage = sage_by_id.get(cid, {})
            ws.cell(row=row_idx, column=1, value=cid)
            ws.cell(row=row_idx, column=2, value=name)
            ws.cell(row=row_idx, column=3, value=1)
            ws.cell(row=row_idx, column=4, value=name)
            ws.cell(row=row_idx, column=5, value=(sage.get("Cardholder_Country") or "").strip())
            ws.cell(row=row_idx, column=6, value=(sage.get("Cardholder_State") or "").strip())
            ws.cell(row=row_idx, column=7, value=(sage.get("Cardholder_ZIP") or "").strip())
            ws.cell(row=row_idx, column=8, value=(sage.get("Cardholder_City") or "").strip())
            ws.cell(row=row_idx, column=9, value=(sage.get("Cardholder_Address1") or "").strip())
            ws.cell(row=row_idx, column=10, value=(sage.get("Cardholder_Address2") or "").strip())
            ws.cell(row=row_idx, column=11, value=(sage.get("Phone_Number") or "").strip())
            ws.cell(row=row_idx, column=12, value=(sage.get("eMail_Address") or "").strip())
            ws.cell(row=row_idx, column=13, value=cid)
            ws.cell(row=row_idx, column=14, value="English (US)")
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

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
