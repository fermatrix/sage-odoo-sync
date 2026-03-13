# Sage 50 → Odoo Sync (Project Notes)

## Current Workflow (March 13, 2026)

Primary commands (run from repo root):
```
python sage_odoo_parity.py refresh_sage
python sage_odoo_parity.py refresh_odoo
python sage_odoo_parity.py sync
```

Folder layout:
- `ENZO-Sage50/_master_sage/` — Sage masters (`customers.csv`, `items.csv`)
- `ENZO-Sage50/_master_odoo/` — Odoo masters (`customers_odoo.csv`, `items_odoo.csv`)
- `ENZO-Sage50/_master/` — Sync outputs and review files

Key outputs:
- `ENZO-Sage50/_master/customers_sync.csv`
  - Includes `CustomerIsInactive`, `CustomerSince`, `LastInvoiceDate`
- `ENZO-Sage50/_master/items_sync.csv`
  - Includes `ItemIsInactive`, `OdooColor`
- `ENZO-Sage50/_master/_customer_FAILS.csv`
- `ENZO-Sage50/_master/_item_FAILS.csv`

Odoo import files:
- `ENZO-Sage50/_master/odoo_imports/customers_NEW.xlsx`
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
