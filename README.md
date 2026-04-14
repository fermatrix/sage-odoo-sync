# Sage 50 -> Odoo Sync (Project Notes)

## WORK IN PROGRESS
- El proceso de **Products** se movió a `optyx-sync/README.md`.

## PROCESOS REMOTOS en el ORDENADOR donde esta SAGE
- Watcher:
  - Script: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\watcher\autoexec_watcher.ps1`
  - Jobs folder: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec`
  - EXE folder: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\exe`
  - Logs: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\log\autoexec.log`
  - Done: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\done`
  - Output por defecto: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\output`
- Job files (`*.job.txt`):
  - Line 1 = exe name (must exist in `autoexec\exe`)
  - Line 2+ = one argument per line
- Important:
  - `odbc_master_export.exe` requires `--password` or exits with code `2`.
  - Watcher injects `--out-dir` if missing. Prefer explicit paths:
    - Sage master exports -> `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master_sage`
  - Odoo master exports -> `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master_odoo`
    - Match/sync outputs -> `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master`
  - El watcher **solo procesa ficheros nuevos**. Si el watcher estaba parado:
    - Hay que volver a **crear** los `.job.txt` (borrar y re-copiar) para que cuenten como nuevos.
  - Al detectar un job, lo renombra a `processing_...` en la misma carpeta.
    - Luego lo mueve a `done/` con prefijo `executed_...` o `failed_...`.

### Watcher: cómo arrancarlo
En la máquina remota:
```
C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\watcher\start_watcher.cmd
```

### ODBC / Sage (remoto)
- **DSN** (confirmado): `SAGE`
- **User ID** típico: `Peachtree`
- **Password**: (consultar al cliente si cambia)
- Importante: el DSN debe existir en el **ODBC de 32 bits**.
  - Abrir: `C:\Windows\SysWOW64\odbcad32.exe`

### Export de facturas / credit notes (remoto)
Ejemplos de jobs (una línea por argumento en el `.job.txt`):

**Facturas (invoice)**
```
odbc_invoice_export.exe
--dsn
SAGE
--user
Peachtree
--password
<PASSWORD>
--start-date
2026-04-01
--end-date
2026-05-01
--module
R
--invoice-journalex
8
--invoice
--out-dir
C:\Users\soadmin\Dropbox\ENZO-Sage50\13_2026
```

**Credit notes**
```
odbc_invoice_export.exe
--dsn
SAGE
--user
Peachtree
--password
<PASSWORD>
--start-date
2026-03-01
--end-date
2026-04-01
--module
R
--credit-journalex
3
--credit-note
--out-dir
C:\Users\soadmin\Dropbox\ENZO-Sage50\13_2026
```

Notas:
- Para credit notes el flag correcto es `--credit-journalex` (no `--credit-note-journalex`).
- Si falla con `Invalid user authorization specification (-1903)`, el DSN/credenciales no son válidos.

## TABLAS DE SAGE
Tablas ODBC confirmadas (DSN `STUDIOOPTYXINC`):
- `Customers`
- `Address`
- `Contacts`
- `LineItem` (items master)
- `JrnlHdr` (invoice headers)
- `JrnlRow` (invoice lines)
- `StoredTransHeaders`
- `StoredTransRows`
- `Tax_Code`
- `Tax_Authority`
- `PaymentMethod`

Otras tablas de items (no usadas por ahora):
- `BOMItems`
- `InventoryChains`
- `InventoryCosts`

### Sage 50 Data (.DAT) Files (ODBC/Crystal Reports)
Fuente: lista de archivos .DAT disponibles en Sage 50 (para Crystal/ODBC).  
Hemos marcado si ya los hemos descargado por ODBC (**Sí**) o no (**No**).

| Tabla (.DAT) | Descripción (Sage) | Estado |
|---|---|---|
| `ADDRESS.DAT` | Address Fields | **Sí** (`Address`) |
| `AUDITTR.DAT` | Audit Trail Fields | No |
| `BOMHIST.DAT` | BOM History | No |
| `BOMITEMS.DAT` | Bill of Materials | No |
| `BDETAIL.DAT` | Budget Detail | No |
| `BKACTMAP.DAT` | Bank Account ID | No |
| `BUDGET.DAT` | Budget Fields | No |
| `CASHFLOWMANAGER.DAT` | Cash Flow Manager | No |
| `CHART.DAT` | Chart of Accounts | No |
| `CHGORDER.DAT` | Change Order | No |
| `CODETAIL.DAT` | Change Order Detail | No |
| `COMPANY.DAT` | Company Fields | No |
| `CONTACTS.DAT` | Contacts | **Sí** (`Contacts`) |
| `COST.DAT` | Cost Code | No |
| `CUSTOMER.DAT` | Customer Fields | **Sí** (`Customers`) |
| `DEFPRFLD.DAT` | Default Payroll Fields | No |
| `EARNSUMM.DAT` | Employee Earnings Summary | No |
| `EMPAYINF.DAT` | Employee Pay Information | No |
| `EMPLOYEE.DAT` | Employee Fields | No |
| `EMPPRFLD.DAT` | Employee Custom Payroll Fields | No |
| `ESPRFLD.DAT` | Earnings Summary Payroll | No |
| `ESWAGE.DAT` | Earnings Summary Wage | No |
| `GENERAL.DAT_AP` | Vendor/AP Defaults | No |
| `GENERAL.DAT_AR` | Customer/AR Defaults | No |
| `GENERAL.DAT_GL` | General Ledger Defaults | No |
| `GENERAL.DAT_INV` | Inventory Defaults | No |
| `GENERAL.DAT_JOBS` | Job Defaults | No |
| `JRNLHDR.DAT` | Journal Header | **Sí** (`JrnlHdr`) |
| `JRNLROW.DAT` | Journal Row | **Sí** (`JrnlRow`) |
| `JRNLSNO.DAT` | Serial Number | No |
| `JOBEST.DAT` | Job Estimates | No |
| `LINEITEM.DAT` | Inventory Items | **Sí** (`LineItem` -> `items.csv`) |
| `NOTADMSG.DAT` | Notification Additional Message | No |
| `NOTCDVAL.DAT` | Notification Condition | No |
| `NOTIFICA.DAT` | Notification | No |
| `NOTRECIP.DAT` | Notification Recipient | No |
| `NOTMSG.DAT` | Notification Message | No |
| `NOTRULES.DAT` | Notification Rules | No |
| `OLTRANS.DAT` | Online Banking Transactions | No |
| `PAYTYPES.DAT` | Pay Types | No |
| `PHASE.DAT` | Phase Fields | No |
| `PROJECT.DAT` | Job Fields | No |
| `QTYDISC.DAT` | Quantity Discount | No |
| `RAISEHST.DAT` | Raise History | No |
| `REVIEW.DAT` | Performance Review | No |
| `S1ActMap.DAT` | Account Mapping | No |
| `S1TaxMap.DAT` | Sales Tax Mapping | No |
| `STATCODE.DAT` | Status Code | No |
| `STATHIST.DAT` | Status History | No |
| `STATNOTE.DAT` | Status Note | No |
| `TAXAUTH.DAT` | Sales Tax Authority | **Sí** (`Tax_Authority`) |
| `TAXCODE.DAT` | Sales Tax Code | **Sí** (`Tax_Code`) |
| `TICKET.DAT` | Time/Expense Ticket | No |
| `UNITMEAS.DAT` | Unit/Measure | No |
| `VENDINS.DAT` | Vendor Insurance | No |
| `VENDOR.DAT` | Vendor Fields | No |
| `WORKTKT.DAT` | Work Ticket | No |

Notas:
- En nuestro ODBC **no aparece** una tabla explícita “SalesOrders”.  
- Las **Sales Orders / Quotes / Proposals** viven dentro de `JrnlHdr/JrnlRow` y se filtran por Journal.

### Sales Orders (Orders / Quotes / Proposals)
Identificación confirmada en `JrnlHdr`:
- **Sales Orders** → `JrnlKey_Journal = 11` y `JournalEx = 19`
- El número de orden (SO No.) aparece en `Reference`.

En `JrnlHdr` también aparecen:
- `INV_POSOOrderNumber` (en invoices)
- `CustomerSONo`, `PurchOrder` (otros escenarios)

En `JrnlRow`:
- Las líneas de Sales Orders se filtran con `Journal = 11`.

Ficheros generados (Feb/Mar/Abr 2026):
- `ENZO-Sage50/13_2026/01_02_Feb/2026_02_sales_orders_headers.csv`
- `ENZO-Sage50/13_2026/01_02_Feb/2026_02_sales_orders_lines.csv`
- `ENZO-Sage50/13_2026/02_03_Mar/2026_03_sales_orders_headers.csv`
- `ENZO-Sage50/13_2026/02_03_Mar/2026_03_sales_orders_lines.csv`
- `ENZO-Sage50/13_2026/03_04_Apr/2026_04_sales_orders_headers.csv`
- `ENZO-Sage50/13_2026/03_04_Apr/2026_04_sales_orders_lines.csv`

Campos clave en `invoice_lines`:
- `INV_POSOOrderNumber` no aparece en `invoice_lines` (líneas), solo en headers.
- Para relacionar Orders ↔ Invoices se usa `INV_POSOOrderNumber` en `JrnlHdr`.

## PROCESOS DE SINCRONIZADO

### (1) CUSTOMERS
Comandos:
```
python sage_odoo_parity.py refresh_sage
python sage_odoo_parity.py refresh_odoo
python sage_odoo_parity.py sync
```
Entradas principales:
- Sage: `ENZO-Sage50/_master_sage/customers.csv`
- Sage: `ENZO-Sage50/_master_sage/address.csv` (solo `AddressTypeNumber = 0` para el address principal)
- Odoo: `ENZO-Sage50/_master_odoo/customers_odoo.csv`
  - Incluye `OdooSalespersonId`, `OdooSalesperson`, `OdooPricelistId`, `OdooPricelist`.

Salidas:
- `ENZO-Sage50/_master/customers_sync.csv`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_customers_NEW.xlsx`

Lógica clave:
- Dirección principal:
  - Se toma **solo** `AddressTypeNumber = 0` como address principal.
  - Si hay varias filas tipo 0, se usa la de menor `AddressRecordNumber`.
  - Campos pegados al Customer:
    - `street` = `AddressLine1`
    - `street2` = `AddressLine2`
    - `city` = `City`
    - `zip` = `Zip`
    - `state_id` = `State` (con parity)
    - `country_id` = `Country` (con parity)
- Parity:
  - País: `_parity_country.csv`
  - Estado: `_parity_state.csv`
- `refresh_odoo` exporta también contactos y child partners para matches posteriores.

### (2) BILL TO
Comandos:
```
python sage_odoo_parity.py build_billto_sync
python sage_odoo_parity.py build_billto
```
Entradas:
- Sage: `contacts.csv`, `address.csv`
- Odoo: `customers_contacts.csv` (exportado en `refresh_odoo`)

Salidas:
- `ENZO-Sage50/_master/customers_billto_sync.csv`
- `ENZO-Sage50/_master/customers_billto_sync_NEW.csv`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_customers_billto_NEW.xlsx`

Lógica clave:
- Bill To = contactos primarios (`IsPrimaryContact = 1`).
- Join con direcciones por `AddressRecordNumber`.
- En Odoo: `type = invoice`.
- Match: `ParentId` + `Reference`.

### (3) ADDRESS
Comandos:
```
python sage_odoo_parity.py build_addresses_sync
python sage_odoo_parity.py build_delivery_addresses
```
Entradas:
- Sage: `contacts.csv`, `address.csv`
- Odoo: `customers_delivery_addresses.csv` (exportado en `refresh_odoo`)

Salidas:
- `ENZO-Sage50/_master/customer_delivery_addresses_sync.csv`
- `ENZO-Sage50/_master/customer_delivery_addresses_sync_NEW.csv`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_customers_delivery_NEW.xlsx`

Lógica clave:
- Delivery addresses salen de **contactos no primarios** con `AddressRecordNumber`.
- Join: `contacts.AddressRecordNumber` -> `address.AddressRecordNumber`.
- `External_ID` = `CustomerID_ContactRecordNumber`.
- Notas: `AddressTypeNumber | AddressTypeDesc`.

### (4) PRODUCTS
**Nota:** el flujo de variantes **Sun vs Optics** vive ahora en `optyx-sync/README.md`.

Orden correcto (importante):
1. `python sage_odoo_parity.py refresh_sage`
2. `python sage_odoo_parity.py refresh_odoo`
3. `python sage_odoo_parity.py sync`
4. `python sage_odoo_parity.py build_product_sync --year-month YYYY_MM`

Comandos (ficheros/plantillas en el proyecto principal):
```
python sage_odoo_parity.py build_product_sync --year-month YYYY_MM
python sage_odoo_parity.py build_items_sync_new
python sage_odoo_parity.py build_products_sync_nobarcode_new
python sage_odoo_parity.py build_products_import
python sage_odoo_parity.py build_products_nobarcode_import
```
Entradas:
- Sage: `ENZO-Sage50/_master_sage/items.csv`
- Odoo: `ENZO-Sage50/_master_odoo/items_odoo.csv`

Salidas:
- `ENZO-Sage50/_master/products_sync.csv`
- `ENZO-Sage50/_master/products_sync_NEW.csv` (barcode >= 12)
- `ENZO-Sage50/_master/products_sync_nobarcode_NEW.csv` (barcode vacío o corto)
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_products_NEW.xlsx`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_products_nobarcode_NEW.xlsx`

Filtros/Notas:
- Excluir descripciones que empiecen por `DERAPAGE`, `ECLIPSE`, `90 PIECE`.
- `NW77PLAQUE` se excluye de NEW y se incluye en nobarcode.
- Import no-barcode (criterio Ally): `Invoiced2026 = X` o `ItemDescription` empieza por `ERKERS `, `BA&SH `, `NW 77TH `, `MONOQOOL `.

### (5) EMPLOYEES / SALES REP (Sage → Odoo users)
Comando:
```
python sage_odoo_parity.py build_employees_sync --months 2026_02,2026_03,2026_04
```
Entradas:
- Sage: `ENZO-Sage50/_master_sage/employees.csv`
- Sage: `ENZO-Sage50/13_2026/<MM>/2026_MM_sales_orders_headers.csv` (para filtrar solo reps usados en Orders)
- Odoo: `ENZO-Sage50/_master_odoo/users_odoo.csv`

Salidas:
- `ENZO-Sage50/_master/employees_sync.csv`
- `ENZO-Sage50/_master/employees_NEW.csv`

Notas:
- Se incluyen solo empleados con `EmpRecordNumber` presente en Sales Orders de los meses seleccionados.
- Match Odoo opcional:
  - Por nombre exacto (`EmployeeName` ↔ `OdooName`).
  - Por `EmployeeID` ↔ `OdooLogin` o `OdooName` (fallback).

### (6) USERS (import a Odoo)
Plantilla:
- `ENZO-Sage50/_master/odoo_templates/users.xlsx`

Import:
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_users.xlsx`

Campos clave:
- `id` → `__import__.SAGE_<EmpRecordNumber>` (External ID estable).
- `ref` → `EmployeeID` (código bonito tipo `JJ 367`).
- `comment` → `EmpRecordNumber=...`.
- `active` → `False` (usuarios importados **inactivos**).
- `lang` → `en_US`.
- Contacto:
  - `phone` (PhoneWork o PhoneNumber)
  - `mobile`
  - `function` (JobTitle)
  - `street`, `street2`, `city`, `zip`
  - `state_id/name` y `country_id/code` (aplicando parity).

Filtro:
- En el XLSX se incluyen solo empleados con `Invoiced2026 = X`.

### Parity común (estado / país)
Para evitar duplicar lógica, usamos `parity_utils.py`:
- `load_country_parity`, `load_country_name_to_code`, `load_state_parity`
- `normalize_country`, `normalize_state`

Esto se usa en:
- `sync_addresses.py`
- `sync_billto.py`
- Import de `users` (state/country).

### Odoo masters nuevos (Sales Rep / Pricing Level)
Exportados en `refresh_odoo`:
- `ENZO-Sage50/_master_odoo/users_odoo.csv` (res.users)
- `ENZO-Sage50/_master_odoo/pricelists_odoo.csv` (product.pricelist)
- `ENZO-Sage50/_master_odoo/pricelist_items_odoo.csv` (product.pricelist.item)
- `ENZO-Sage50/_master_odoo/pricelists/` (un CSV por cada pricelist)
- `ENZO-Sage50/_master_odoo/customers_odoo.csv` incluye:
  - `OdooSalespersonId` + `OdooSalesperson` (sales rep en Odoo)
  - `OdooPricelistId` + `OdooPricelist` (pricing level en Odoo)

### Pricing Level / Sales Rep (Sage vs Odoo)
Sage (customers.csv):
- `PriceLevel` es numérico (0..9). Distintos niveles detectados: 0,1,2,3,4,5,6,7,8,9.
- `EmpRecordNumber` está presente en customers, pero en orders el enlace directo confirmado es:
  - `JrnlHdr.EmpRecordNumber` → `employees.EmpRecordNumber` (sales rep real).

Odoo (masters actuales):
- Pricelists existentes en Odoo: `USA`, `EU`, `CAD`, `UK`, `AUD`.
- En `customers_odoo.csv` **todos** los customers tienen un pricelist asignado (normalmente `USA (USD)`).
- En `customers_odoo.csv` **no hay salesperson** (`OdooSalespersonId` vacío en todos).

Conclusión provisional:
- El `PriceLevel` numérico de Sage **no tiene mapeo directo** a las pricelists actuales de Odoo.
- Los sales reps todavía no existen como `res.users` en Odoo (o al menos no están asignados a customers).

