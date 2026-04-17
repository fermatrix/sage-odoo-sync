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

### Sales Orders API Sync (draft)
Script nuevo:
- `sync_sales_orders_api.py`

Objetivo:
- Crear `sale.order` en Odoo por API (XML-RPC), en borrador, leyendo Sage `sales_orders_headers/lines`.
- Mantener numeración Sage en `sale.order.name` (campo `Reference` de Sage).
- Crear también las líneas de pedido (`sale.order.line`).

Comandos base:
```
python sync_sales_orders_api.py --load 02/02/2026
python sync_sales_orders_api.py --load 02/2026
python sync_sales_orders_api.py --load 2026
python sync_sales_orders_api.py --load 02/02/2026 --limit 100
python sync_sales_orders_api.py --load 02/02/2026 --limit 12,1
python sync_sales_orders_api.py --load 02/02/2026-03/02/2026
python sync_sales_orders_api.py --load 02/2026-03/02/2026
```

Flags CLI (`sync_sales_orders_api.py`):
- `--root-dir`: raíz del proyecto de datos Sage/Odoo. Default: `ENZO-Sage50`.
- `--profile`: perfil Odoo para leer variables del `.env`. Default: `STUDIOOPTYX`.
- `--env-file`: ruta al `.env`. Default: `.env`.
- `--headers-path`: CSV de headers (modo manual, sin `--load`).
- `--lines-path`: CSV de líneas (modo manual, sin `--load`).
- `--customers-sync`: ruta a `customers_sync.csv`.
- `--products-sync`: ruta a `products_sync.csv`.
- `--employees-sync`: ruta a `employees_sync.csv`.
- `--load`: auto-descubre ficheros de sales orders por periodo/fecha. Formatos:
  - `DD/MM/YYYY`
  - `MM/YYYY`
  - `YYYY` (año fiscal Sage: Febrero -> Enero siguiente)
  - rango `inicio-fin` (ej: `02/02/2026-03/02/2026`, `02/2026-03/02/2026`)
- `--reference`: procesa solo una SO de Sage (ej: `357702`).
- `--limit`: límite de procesado:
  - `N` = primeras `N` candidatas
  - `start,count` = empieza en ordinal `start` y procesa `count` (ej: `12,1`)
  - vacío = sin límite
- `--offset`: salta N candidatas antes de procesar (acumulable con `--limit`).
- `--log-path`: salida del log CSV.
- `--dry-run`: valida/matchea todo sin crear/actualizar en Odoo.
- `--allow-partial`: no parar en el primer error crítico; sigue con el resto.
- `--skip`: alias de `--allow-partial` (continúa tras errores).
- `--gaps`: procesa solo SO de Sage que faltan en Odoo (`sale.order.name` no existe).
  - Además, se detiene automáticamente al llegar al primer bloque final de órdenes nunca importadas (trailing block).
  - Útil para reintentar “huecos” sin meterse en un intervalo completo pendiente.

Notas operativas de flags:
- Cualquier flag desconocido hace que el script falle inmediatamente (comportamiento estándar de `argparse`).
- Si se usa `--load`, el script ignora `--headers-path` y `--lines-path` para ese run.
- `--gaps --skip` = reintenta huecos y continúa aunque haya errores, para revisar todos en una pasada.

Modo estricto (por defecto):
- El proceso se detiene al primer error crítico de datos.
- Error crítico = falta de `customer`, `payment term`, `salesperson` (employee/user) o shipping sin match exacto.
- Para permitir continuar con parciales: `--allow-partial`.

Log de ejecución:
- `ENZO-Sage50/_master/orders_api_log.csv`
- Estados principales: `OK`, `OK_WARN`, `SKIP`, `ERROR`, `DRY_RUN`, `DRY_RUN_WARN`.

Validaciones implementadas:
- Match de customer (`CustVendId` -> `customers_sync.csv` -> `OdooId`).
- Match de producto por línea (`ItemRecordNumber` -> `products_sync.csv` -> `OdooVariantId`).
- Intento de match de términos de pago (`TermsDescription` de Sage con `account.payment.term` de Odoo).
- Control de totales (`MainAmount` vs total de líneas preparadas).
- Si hay inconsistencias, la order se puede crear igual en draft y se marca como `OK_WARN`.
- Además, en modo estricto no se crea/actualiza la order cuando falta dato crítico.
- Caso especial `EmpRecordNumber = 0`:
  - No se bloquea la SO por falta de rep.
  - Se limpia explícitamente el salesperson en Odoo (`user_id = False`).

Fechas en Odoo (estado actual):
- `date_order` = `TransactionDate` (Sage).
- `commitment_date` (Delivery Date) = `ShipByDate` (Sage). Si faltara, fallback a `TransactionDate`.
- `validity_date` (Expiration) = `TransactionDate` (pendiente decisión funcional futura).

ShipVia (Sage) -> Odoo (estado actual y futuro):
- **Estado actual (replicación histórica fiel a Sage):**
  - Se conserva literalmente el `ShipVia` de Sage (incluyendo legacy: `Airborne`, `Courier`, `MQPE60`).
  - Si la SO trae línea de shipping, se añade `Shipping Method: <ShipVia>` en la descripción de esa línea.
  - Si la SO **no** trae línea de shipping, se añade al final una `line_note` con `Shipping Method: <ShipVia>`.
- **Estado futuro (normalización solicitada por negocio):**
  - Catálogo objetivo: `UPS Ground`, `US Mail (USPS)`, `Hand Deliver`, `2nd Day Air`, `Overnight Delivery`.
  - `Airborne` se sustituirá por `2nd Day Air` / `Overnight Delivery`.
  - `MQPE60` se ignora (valor erróneo/legacy).
  - `Courier` pendiente de decisión funcional.

Resolución de Customer / Invoice / Delivery (regla actual):
- Dato 100% fiable en Order de Sage: `CustVendId` (customer).
- En el CSV de Sales Orders no viene un ID explícito de address para shipping/billing.
- `partner_id` (Odoo) se fija por `CustVendId`.
- `partner_invoice_id` sigue la lógica de Odoo: contacto `invoice` asociado al customer (`partner_id`). Si no existe, fallback al partner padre.
- `partner_shipping_id` se resuelve por `ShipToName/Address/City/State/ZIP` contra child partners de Odoo:
  - primero delivery,
  - y como fallback contactos child con misma dirección.
- Si no hay match fiable para shipping, fallback al partner padre.

Limitación conocida:
- Sin un `AddressRecordNumber` explícito en la Order de Sage, shipping/billing se resuelven por la mejor coincidencia disponible (no perfecto, pero actualmente es el mejor criterio operativo).

Orden de procesado:
- Cronológico real: `TransactionDate` ascendente (y desempate por `Reference`, `PostOrder`).

Fecha de creación vs fecha de pedido:
- `create_date` **no se puede** forzar por import estándar ni por API ORM estándar.
- Pruebas realizadas:
  - API `write/create` con `create_date`: Odoo ignora el valor y mantiene la fecha real de creación.
  - Import CSV/XLSX con columna `create_date`: Odoo no reconoce el campo.
- Campo operativo para fecha histórica del pedido: `date_order` (Quotation/Order Date en Odoo).

Campos clave en `invoice_lines`:
- `INV_POSOOrderNumber` no aparece en `invoice_lines` (líneas), solo en headers.
- Para relacionar Orders ↔ Invoices se usa `INV_POSOOrderNumber` en `JrnlHdr`.

Campos de Sales Order de Sage todavía no mapeados 1:1 en Odoo:
- `PurchOrder` (PO del cliente) aún no se está escribiendo en un campo destino.
- `ShipVia` no se guarda en un campo estructurado de cabecera; de momento queda en descripción/nota de líneas.
- `ShipByDate` y `GoodThruDate` aún no se trasladan explícitamente.
- Flags de cierre (`POSOisClosed`, `CompletedDate`) no se aplican al estado de la SO en Odoo.
- Metadatos técnicos de Sage (por ejemplo `JournalEx`, `DistNumber`) no se trasladan.
- Impuesto de Sage por línea (`TaxAuthorityCode` / `SalesTaxType`) no se replica 1:1 (Odoo usa su lógica fiscal).

Observación de datos (2026):
- Se detectaron 75 Sales Orders con línea `SALES TAX` (todas con `TaxAuthorityCode = SO`).
- Esto explica parte de los `Order total mismatch` cuando no se traslada esa línea aún.

## PROCESOS DE SINCRONIZADO

### (1) CUSTOMERS
Comandos:
```
python sage_odoo_parity.py refresh_sage
python sage_odoo_parity.py refresh_odoo
python sage_odoo_parity.py sync
```
Filtro NEW (actual):
- Se usa `LAST_SALESORDER_MIN` (env).  
- Compatibilidad hacia atrás: si no existe `LAST_SALESORDER_MIN`, usa `LAST_INVOICE_MIN`.
- `LastSalesOrderDate` se calcula desde `*_sales_orders_headers.csv` y se guarda en `customers_sync.csv`.
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
python sage_odoo_parity.py build_billto_update
```
Entradas:
- Sage: `contacts.csv`, `address.csv`
- Odoo: `customers_child_partners_all.csv` (exportado en `refresh_odoo`)

Salidas:
- `ENZO-Sage50/_master/customers_billto_sync.csv`
- `ENZO-Sage50/_master/customers_billto_sync_NEW.csv`
- `ENZO-Sage50/_master/customers_billto_sync_UPDATE.csv`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_customers_billto_NEW.xlsx`
- `ENZO-Sage50/_master/odoo_UPDATE/YYYYMMDD_customers_billto_UPDATE.xlsx`

Lógica clave:
- Bill To = contactos primarios (`IsPrimaryContact = 1`).
- Join con direcciones por `AddressRecordNumber`.
- En Odoo: `type = invoice`.
- Match estricto: `ParentId` + `Reference` (`OdooRef` = `ContactRecordNumber`).
- Sin fallback para UPDATE (ni por nombre ni por dirección).
- NEW: solo se generan filas con `BilltoSyncStatus = NEW` y `OdooParentId` informado.
- UPDATE: usa `OdooContactExternalId` real de Odoo (no generado).
- Country se puede mantener fuera de UPDATE según plantilla (`UPDATE_customers_billto.xlsx`).
- Plantilla NEW actual: `ENZO-Sage50/_master/odoo_templates/NEW_customer_billto.xlsx`.

### (3) ADDRESS
Comandos:
```
python sage_odoo_parity.py build_addresses_sync
python sage_odoo_parity.py build_delivery_addresses
python sage_odoo_parity.py build_delivery_addresses_update
```
Entradas:
- Sage: `contacts.csv`, `address.csv`
- Odoo: `customers_delivery_addresses.csv` (exportado en `refresh_odoo`)

Salidas:
- `ENZO-Sage50/_master/customer_delivery_addresses_sync.csv`
- `ENZO-Sage50/_master/customer_delivery_addresses_sync_NEW.csv`
- `ENZO-Sage50/_master/customer_delivery_addresses_sync_UPDATE.csv`
- `ENZO-Sage50/_master/customer_delivery_addresses_sync_UPDATE_CONFLICTS.csv`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_customers_delivery_NEW.xlsx`
- `ENZO-Sage50/_master/odoo_UPDATE/YYYYMMDD_customers_delivery_UPDATE.xlsx`

Lógica clave:
- Delivery addresses salen de **contactos no primarios** con `AddressRecordNumber`.
- Join: `contacts.AddressRecordNumber` -> `address.AddressRecordNumber`.
- `External_ID` = `CustomerID_ContactRecordNumber`.
- Notas: `AddressTypeNumber | AddressTypeDesc`.
- `Reference` en Odoo = `ContactRecordNumber` de SAGE.
- Match estricto para UPDATE: `ParentId` + `Reference` (`OdooRef` exacto).
- Sin fallback para UPDATE; si no hay match estricto queda en NEW.
- NEW de delivery sin `OdooParentId` no se importa (cliente no existe en Odoo).
- UPDATE de delivery usa `OdooAddressExternalId` real de Odoo.
- `Country` no entra en mismatch para UPDATE cuando la plantilla de update no lo incluye.
- Estado/country se normalizan con parity (`_parity_state.csv`, `_parity_country.csv`).

Operativa de recuperación (si faltan deliveries importadas previamente):
- Comparar `_master/odoo_imports/_IMPORTED/*customers_delivery*.xlsx` contra `_master_odoo/customers_delivery_addresses.csv`.
- Generar restore solo de `External_ID` faltantes (manteniendo `Parent/Database ID` actual).
- Importar restore y repetir:
  - `python sage_odoo_parity.py refresh_odoo`
  - `python sage_odoo_parity.py build_addresses_sync`
  - `python sage_odoo_parity.py build_delivery_addresses_update`

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

### (7) VENDORS
Objetivo:
- Homogeneizar proveedores de Sage con vendors de Odoo antes de cargar vendor pricelist.

Flujo:
```
python sage_odoo_parity.py refresh_sage
python sage_odoo_parity.py refresh_odoo
python sage_odoo_parity.py sync
python sage_odoo_parity.py build_vendors
python sage_odoo_parity.py build_vendors_update
```

Entradas:
- Sage: `ENZO-Sage50/_master_sage/vendors.csv`
- Sage: `ENZO-Sage50/_master_sage/address.csv` (AddressTypeNumber `0`, por `VendorRecordNumber`)
- Odoo: `ENZO-Sage50/_master_odoo/vendors_odoo.csv`

Salidas:
- `ENZO-Sage50/_master/vendors_sync.csv`
- `ENZO-Sage50/_master/vendors_sync_NEW.csv`
- `ENZO-Sage50/_master/vendors_sync_UPDATE.csv`
- `ENZO-Sage50/_master/vendors_NEW.xlsx`
- `ENZO-Sage50/_master/vendors_UPDATE.xlsx`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_vendors_NEW.xlsx`
- `ENZO-Sage50/_master/odoo_UPDATE/YYYYMMDD_vendors_UPDATE.xlsx`

Match:
- Estricto por `VendorID` (Sage) ↔ `OdooRef` (Odoo vendor).
- Si ya hay `OdooId`, se reutiliza para consolidar.
- Fallback por nombre solo para descubrimiento (`--vendor-match-name`), nunca para UPDATE seguro.

Estados:
- `MATCH`: existe y coincide.
- `UPDATE`: existe pero hay diferencias (name/ref/phone/email/street/street2/city/zip/country).
- `NEW`: no encontrado en Odoo.

Notas operativas:
- `Reference` en Odoo se alinea con `VendorID` de Sage (misma filosofía que Customers).
- En `NEW_vendors.xlsx` se rellena `Reference` con `Vendor_ID`.
- Si Sage trae address vacío y Odoo tiene dirección cargada, puede aparecer `UPDATE` por `street/country`.

### (8) VENDOR PRICELIST
Objetivo:
- Construir precios de compra por proveedor desde histórico de Sage y compararlos contra `product.supplierinfo` en Odoo.

Flujo:
```
python sage_odoo_parity.py refresh_odoo
python sage_odoo_parity.py build_vendor_pricelist_sync
python sage_odoo_parity.py build_vendor_pricelist_import
python sage_odoo_parity.py build_vendor_pricelist_update
```

Entradas:
- Sage: `ENZO-Sage50/_master_sage/JrnlHdr.csv`, `ENZO-Sage50/_master_sage/JrnlRow.csv`
- Sync: `ENZO-Sage50/_master/vendors_sync.csv`, `ENZO-Sage50/_master/products_sync.csv`
- Odoo: `ENZO-Sage50/_master_odoo/vendor_pricelist_odoo.csv` (exportado en `refresh_odoo`)

Salidas:
- `ENZO-Sage50/_master/vendor_pricelist_sync.csv`
- `ENZO-Sage50/_master/vendor_pricelist_sync_NEW.csv`
- `ENZO-Sage50/_master/vendor_pricelist_sync_UPDATE.csv`
- `ENZO-Sage50/_master/vendor_pricelist_sync_CONFLICTS.csv`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_vendor_pricelist_NEW.xlsx`
- `ENZO-Sage50/_master/odoo_UPDATE/YYYYMMDD_vendor_pricelist_UPDATE.xlsx`

Estados:
- `MATCH`: vendor+template ya existe y precio coincide.
- `UPDATE`: vendor+template ya existe pero precio difiere.
- `NEW`: no existe línea en Odoo.
- `CONFLICT`: falta match previo de vendor o template en Odoo (se revisa antes de importar precios).

Reglas de plantilla/import (estado actual):
- Se usa `Product Variant/Database ID` (no External ID).
- `product_uom_id` se exporta como `Units`.
- Para `NEW`, se fija por defecto:
  - `Currency = USD`
  - `min_qty = 1`
  - `delay = 120`
- `Vendor` se envía por nombre visible de partner (evita errores de lookup con `__import__...`).

Limitación actual de UPDATE:
- En muchos `product.supplierinfo` existentes no hay External ID (`OdooSupplierinfoExternalId` vacío).
- Sin ese identificador, el XLSX de UPDATE puede salir vacío aunque existan filas `UPDATE` en sync.

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

### Sage Multiple Price Levels (confirmado en UI)
En la ventana de Sage **Multiple Price Levels** (item level), el orden mostrado corresponde a `PriceLevel1..10` de `items.csv`:
- `PriceLevel1Amount` -> `REGULAR PRICE` (USA)
- `PriceLevel2Amount` -> `Price Level 2`
- `PriceLevel3Amount` -> `Price Level 3`
- `PriceLevel4Amount` -> `Price Level 4`
- `PriceLevel5Amount` -> `Price Level 5`
- `PriceLevel6Amount` -> `EURO PRICING`
- `PriceLevel7Amount` -> `GBP PRICING`
- `PriceLevel8Amount` -> `CAD PRICING`
- `PriceLevel9Amount` -> `DISTRIBUTOR US`
- `PriceLevel10Amount` -> `SPECS PRICING`

Nota importante:
- En Odoo, las pricelists actuales son geográficas (`USA`, `EU`, `UK`, `CAD`, `AUD`) y se aplican por `product.template` (no por variante).
- En Sage, los precios viven por item/variante, por eso hay que agrupar por template al comparar con Odoo.

Mapeo operativo (actual, pendiente de validación final con negocio):
- `USA (USD)` -> `PriceLevel1Amount`
- `EU (EUR)` -> `PriceLevel6Amount`
- `UK (GBP)` -> `PriceLevel7Amount`
- `CAD (CAD)` -> `PriceLevel8Amount`
- `AUD (AUD)` -> pendiente (no aparece explícito en la pantalla de Multiple Price Levels mostrada)

### Pricelist lines (IMPORT vs UPDATE)
Flujo:
```
python sage_odoo_parity.py refresh_odoo
python sage_odoo_parity.py build_pricelist_lines
python sage_odoo_parity.py build_pricelist_import
python sage_odoo_parity.py build_pricelist_update
```

Ficheros base:
- Sage esperado: `ENZO-Sage50/_master/pricelist_lines.csv`
- Nuevas líneas: `ENZO-Sage50/_master/pricelist_lines_NEW.csv`
- Conflictos de precio por variantes del mismo producto: `ENZO-Sage50/_master/pricelist_lines_CONFLICTS.csv`
- IMPORT: `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_pricelist.csv`
- UPDATE: `ENZO-Sage50/_master/odoo_UPDATE/YYYYMMDD_pricelist_UPDATE.csv`

Match de producto:
- Sage trabaja por variante/item.
- Odoo pricelists trabajan por `product.template`.
- `products_sync.csv` conserva `OdooVariantId`, `OdooTemplateId` y `OdooTemplateExternalId`.
- Para importar en Odoo se usa el External ID corto del template en `item_ids/product_tmpl_id/id`.
- Ejemplo: si Odoo exporta `__import__.NW_110`, en el CSV se usa `NW_110`.

IMPORT (líneas nuevas):
- Se usa cuando no existe una línea equivalente en `product.pricelist.item`.
- Match de existencia: `PricelistId + AppliedOn + ProductTemplateId + MinQuantity`.
- El fichero usa la plantilla `ENZO-Sage50/_master/odoo_templates/NEW_pricelist.csv`.
- No incluye ID de línea existente porque Odoo debe crearla.
- Si `pricelist_lines_NEW.csv` queda a `0`, no hay nada nuevo que importar.
- Los `id` de pricelist deben ir sin espacios (ej.: `PRICE_LEVEL_2_USD`, `DISTRIBUTOR_US`) para evitar el error de Odoo `ir_model_data_name_nospaces`.
- Los precios en `item_ids/fixed_price` se exportan con punto decimal `.` (formato US/Odoo), nunca con coma.

UPDATE (precios existentes que cambiaron):
- Se usa cuando la línea ya existe en Odoo pero `FixedPrice` difiere del precio esperado desde Sage.
- El fichero tiene el mismo formato que `pricelist.csv`, pero añade `item_ids/.id`.
- Importante: para actualizar una línea existente hay que usar `item_ids/.id` con el ID interno de `product.pricelist.item`.
- No usar `item_ids/id` para este caso: Odoo lo interpreta como External ID y puede duplicar líneas.

