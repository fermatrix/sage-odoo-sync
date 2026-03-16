# Sage 50 → Odoo Sync (Project Notes)

## Current Workflow (March 13, 2026)

Primary commands (run from repo root):
```
python sage_odoo_parity.py refresh_sage
python sage_odoo_parity.py refresh_odoo
python sage_odoo_parity.py sync
```

Folder layout:
- `ENZO-Sage50/_master_sage/` — Sage **general/master** exports (non-temporal tables like Customers, Items, Address, Contacts)
- `ENZO-Sage50/_master_odoo/` — Odoo master exports (`customers_odoo.csv`, `items_odoo.csv`)
- `ENZO-Sage50/_master/` — Sync outputs and match files (Sage ↔ Odoo)

Key outputs:
- `ENZO-Sage50/_master/customers_sync.csv`
  - Includes `CustomerIsInactive`, `CustomerSince`, `LastInvoiceDate`
- `ENZO-Sage50/_master/items_sync.csv`
  - Includes `ItemIsInactive`, `OdooColor`
- `ENZO-Sage50/_master/_customer_FAILS.csv`
- `ENZO-Sage50/_master/_item_FAILS.csv`

Odoo import files:
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_customers_NEW.xlsx`
  - Active Sage customers without Odoo match, formatted for import
- `ENZO-Sage50/_master/customers_NEW.xlsx`
  - Minimal tracking list (same customers, fewer columns)

Data hygiene:
- All customer and invoice data files are excluded by `.gitignore` (CSV/XLS/XLSX/PDF and Sage output folders).

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
- If no Address row exists, fallback to `Cardholder_*` fields in `customers.csv`.

### Contact logic (primary contact)
- For each customer, add a child contact row **only if** there is an `IsPrimaryContact=1`.
- Child contact has **no address** and **no `company_name`**.
- Child contact does carry `email` and `phone` (if present in `contacts.csv`).
- `External_ID` for child contact: `CustomerID_ContactRecordNumber` (fallback to `CustomerID_contact` if missing).
- `CustomerRef` is filled **only on the company row**, **left empty on child**.

### Template headers (customers.xlsx)
- Added fields now used: `CustomerRef`, `ContactName`, `ContactEmail`, `ContactPhone`, `ContactJobTitle`, `ContactNotes`.
- `Contact*` fields are filled **only on child rows**.
