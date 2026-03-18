# Sage 50 → Odoo Sync (Project Notes)

## Current Workflow (March 13, 2026)

Primary commands (run from repo root):
```
python sage_odoo_parity.py refresh_sage
python sage_odoo_parity.py refresh_odoo
python sage_odoo_parity.py sync
python sage_odoo_parity.py build_contacts_sync
python sage_odoo_parity.py build_contacts
python sage_odoo_parity.py build_product_sync --year-month 2026_02
python sage_odoo_parity.py build_items_sync_new
python sage_odoo_parity.py export_countries
```

Folder layout:
- `ENZO-Sage50/_master_sage/` — Sage **general/master** exports (non-temporal tables like Customers, Items, Address, Contacts)
- `ENZO-Sage50/_master_odoo/` — Odoo master exports (`customers_odoo.csv`, `items_odoo.csv`)
- `ENZO-Sage50/_master/` — Sync outputs and match files (Sage ↔ Odoo)

Key outputs:
- `ENZO-Sage50/_master/customers_sync.csv`
  - Includes `CustomerIsInactive`, `CustomerSince`, `LastInvoiceDate`
- `ENZO-Sage50/_master/items_sync.csv`
  - Includes `ItemIsInactive`, `OdooColor`, `Barcode` (from `UPC_SKU`), `ItemDescriptionForSale`
- `ENZO-Sage50/_master/items_sync_NEW.csv`
  - Items without Odoo match, barcode present (default >=12 digits)
- `ENZO-Sage50/_master/YYYY_MM_product_sync.csv`
  - Items present in that month's invoice + credit note lines that are missing in Odoo
- `ENZO-Sage50/_master/_customer_FAILS.csv`
- `ENZO-Sage50/_master/_item_FAILS.csv`

Odoo import files:
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_customers_NEW.xlsx`
  - Active Sage customers without Odoo match, formatted for import
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_contacts_CHILDREN.xlsx`
  - Child contact rows for customers that already have `OdooId` (after import + refresh/sync)
- `ENZO-Sage50/_master/odoo_templates/customer_contacts.xlsx`
  - Template used for contact imports
- `ENZO-Sage50/_master/customers_NEW.xlsx`
  - Same as above, without timestamp (working copy)

Odoo reference exports:
- `ENZO-Sage50/_master_odoo/countries_odoo.csv`
- `ENZO-Sage50/_master_odoo/states_odoo.csv`
- `ENZO-Sage50/_master_odoo/customers_contacts.csv` (Odoo contacts with `ParentId`)

Parity tables (generated from Sage Address + Odoo reference lists):
- `ENZO-Sage50/_master/country_parity.csv`
  - `sage_country_raw` → `odoo_country_code` suggestions (ISO2)
- `ENZO-Sage50/_master/state_parity.csv`
  - `sage_state_raw` → `odoo_state_name` (full state) + implied country
- `ENZO-Sage50/_master/customers_NEW.xlsx`
  - Minimal tracking list (same customers, fewer columns)

Data hygiene:
- All customer and invoice data files are excluded by `.gitignore` (CSV/XLS/XLSX/PDF and Sage output folders).

## Sage ODBC Tables (Known Names)

Confirmed in this DSN/catalog (`STUDIOOPTYXINC`):
- `Customers` (master customers)
- `Address` (customer address master)
- `Contacts` (customer contacts master)
- `LineItem` (product/master item table used for items export)
- `JrnlHdr` (invoice/transaction headers)
- `JrnlRow` (invoice/transaction lines)
- `StoredTransHeaders`
- `StoredTransRows`
- `Tax_Code`
- `Tax_Authority`
- `PaymentMethod`

Other item-related tables present (not used for master export yet):
- `BOMItems`
- `InventoryChains`
- `InventoryCosts`

## Discovery Summary (March 12, 2026)

### Invoice references
- The Excel file `ENZO-Sage50/13_2026/01_02_Feb/_ENZO_balance_sheet_2023_2024_2025.xlsx` contains a sheet **"Sales Invoice List"**.
- In that sheet, **"Invoice No."** values match the **`Reference`** field in the Sage ODBC exports.
- **`CustomerInvoiceNo` is empty** in the Sage ODBC exports for these invoices.

Quick validation (February 2026):
- Excel invoice refs: **2093**
- Exported header refs matched: **2091** (missing only `""` and `"DELETE THIS"`).

### Journal mapping (current working assumption)
- **Invoices:** `Module=R`, `JournalEx=8`
- **Credit notes:** `Module=R`, `JournalEx=3`

These mappings may be refined if the business rules change, but they currently align with the February 2026 invoice list.

### Output structure (fiscal year 13 / 2026)
Monthly folders under:
`ENZO-Sage50/13_2026/`

Naming convention:
- `YYYY_MM_invoices.csv` (invoice headers)
- `YYYY_MM_invoice_lines.csv` (invoice line items)
- `YYYY_MM_credit_notes.csv` (credit note headers)
- `YYYY_MM_credit_note_lines.csv` (credit note line items)

Example:
```
13_2026/01_02_Feb/2026_02_invoices.csv
13_2026/01_02_Feb/2026_02_invoice_lines.csv
13_2026/01_02_Feb/2026_02_credit_notes.csv
13_2026/01_02_Feb/2026_02_credit_note_lines.csv
```

### Remote autoexec watcher
- Jobs are dropped into: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec`
- Executables live in: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\exe`
- Outputs go to final destination folders (e.g. `13_2026/...`)
- Logs: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\log\autoexec.log`
- Watcher script: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\watcher\autoexec_watcher.ps1`

The watcher ignores jobs prefixed with `executed_`, `failed_`, or `processing_`, and moves completed jobs into `autoexec/done`.

#### Autoexec job notes (important)
- Job files are `*.job.txt` with:
  - Line 1: exe name (must exist in `autoexec\exe`)
  - Line 2+: one argument per line
- `odbc_master_export.exe` **must** be given `--password` or it exits with code `2`.
- The watcher auto-injects `--out-dir` if not provided, but:
  - For **Sage general/master** exports, explicitly use:
    - `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master_sage`
  - For **Odoo masters**, use:
    - `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master_odoo`
  - For **match/sync outputs**, use:
    - `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master`

Example job (export Address to `_master_sage`):
```
odbc_master_export.exe
--password
S@g31879
--table
Address
--out-name
address
--out-dir
C:\Users\soadmin\Dropbox\ENZO-Sage50\_master_sage
```

## Customers_NEW generation notes (March 16, 2026)

### Source tables
- `customers.csv` (Sage master)
- `address.csv` (Sage master)
- `contacts.csv` (Sage master)

### Address logic
- Join key: `Customers.CustomerRecordNumber` ↔ `Address.CustomerRecordNumber`.
- Only **AddressTypeNumber = 0** is used (single address per customer).
- If no Address row exists, **leave address fields empty** (no fallback to `Cardholder_*`).
- Country/state mapping is applied **only when generating `customers_NEW.xlsx`**:
  - Country uses `country_parity.csv` (ISO2). If no match, keep the Sage value.
  - State uses `state_parity.csv` to map code → full name.
  - If country is missing but the state matches, infer country from state (US/Canada, etc.).

### Contact logic (primary contact)
- Child contacts are now exported in a **separate import file** after parent companies exist in Odoo.
- Each child row is included **only if** there is an `IsPrimaryContact=1` and the parent has an `OdooId`.
- Child contact has **no address** and **no `company_name`**.
- Child contact does carry `email` and `phone` (if present in `contacts.csv`).
- `External_ID` for child contact: `CustomerID_ContactRecordNumber` (fallback to `CustomerID_contact` if missing).
- `CustomerRef` is filled **only on the company row**, **left empty on child**.
- `ParentId` is set to the **OdooId** of the parent record in the child row.
- Contacts are only emitted into the NEW file if:
  - They do **not** already exist in Odoo (matched by name/email/phone under the same parent), and
  - The parent company exists in Odoo (valid `ParentId`).

### Contacts sync (new)
- `python sage_odoo_parity.py build_contacts_sync` builds:
  - `ENZO-Sage50/_master/customer_contacts_sync.csv`
  - This matches Sage primary contacts to existing Odoo contacts from:
    - `ENZO-Sage50/_master_odoo/customers_contacts.csv` (exported by `refresh_odoo`)

### Product sync (new)
- `python sage_odoo_parity.py build_product_sync --year-month YYYY_MM`
  - Scans:
    - `ENZO-Sage50/**/YYYY_MM_invoice_lines.csv`
    - `ENZO-Sage50/**/YYYY_MM_credit_note_lines.csv`
  - Joins with:
    - `ENZO-Sage50/_master_sage/items.csv` (barcode in `UPC_SKU`)
    - `ENZO-Sage50/_master/items_sync.csv`
  - Outputs:
    - `ENZO-Sage50/_master/YYYY_MM_product_sync.csv`

### Items sync NEW (new)
- `python sage_odoo_parity.py build_items_sync_new`
  - Filters `ENZO-Sage50/_master/items_sync.csv`:
    - No `OdooVariantId`
    - `Barcode` has >= 12 digits (default)
    - Includes inactive items (no `ItemIsInactive` filter)
  - Output:
    - `ENZO-Sage50/_master/items_sync_NEW.csv`
  - Options:
    - `--barcode-digits 0` disables barcode-length filtering
  - Adds `Invoiced2026` column (X) if item appears in 2026_02 or 2026_03 invoice lines


### Countries & states export
- `python sage_odoo_parity.py export_countries` fetches Odoo reference data and builds parity:
  - Exports: `countries_odoo.csv`, `states_odoo.csv`
  - Parity tables: `country_parity.csv`, `state_parity.csv`

### Template headers (customers.xlsx)
- Added fields now used: `CustomerRef`, `ContactName`, `ContactEmail`, `ContactPhone`, `ContactJobTitle`, `ContactNotes`.
- `Contact*` fields are filled **only on child rows**.
