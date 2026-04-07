# Sage 50 -> Odoo Sync (Project Notes)

## WORK IN PROGRESS
- Waiting on Ally’s feedback about **no-barcode products** that look like frames.
- Next steps once confirmed:
  - Move those items from `products_sync_nobarcode_NEW.csv` into `products_sync_NEW.csv`.
  - Regenerate `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_products_NEW.xlsx`.
  - Remove those items from the no-barcode list.
- `NW77PLAQUE` confirmed: excluded from NEW, kept in no-barcode.

## PROCESOS REMOTOS en el ORDENADOR donde esta SAGE
- Watcher:
  - Script: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\watcher\autoexec_watcher.ps1`
  - Jobs folder: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec`
  - EXE folder: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\exe`
  - Logs: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\log\autoexec.log`
  - Done: `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\done`
- Job files (`*.job.txt`):
  - Line 1 = exe name (must exist in `autoexec\exe`)
  - Line 2+ = one argument per line
- Important:
  - `odbc_master_export.exe` requires `--password` or exits with code `2`.
  - Watcher injects `--out-dir` if missing. Prefer explicit paths:
    - Sage master exports -> `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master_sage`
    - Odoo master exports -> `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master_odoo`
    - Match/sync outputs -> `C:\Users\soadmin\Dropbox\ENZO-Sage50\_master`

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
Comandos:
```
python sage_odoo_parity.py build_product_sync --year-month YYYY_MM
python sage_odoo_parity.py build_items_sync_new
python sage_odoo_parity.py build_products_sync_nobarcode_new
python sage_odoo_parity.py build_products_import
python sage_odoo_parity.py build_products_nobarcode_import
python sync_products_sun_vs_optics.py --start 1 --limit 100
```
Entradas:
- Sage: `ENZO-Sage50/_master_sage/items.csv` (UPC en `UPC_SKU`)
- Odoo: `ENZO-Sage50/_master_odoo/items_odoo.csv`

Salidas:
- `ENZO-Sage50/_master/products_sync.csv`
- `ENZO-Sage50/_master/products_sync_NEW.csv` (barcode >= 12)
- `ENZO-Sage50/_master/products_sync_nobarcode_NEW.csv` (barcode vacío o corto)
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_products_NEW.xlsx`
- `ENZO-Sage50/_master/odoo_imports/YYYYMMDD_products_nobarcode_NEW.xlsx`
- `ENZO-Sage50/_master/odoo_imports/20260507_sun_vs_optics_USA_LOG.xlsx`

Filtros/Notas:
- Excluir descripciones que empiecen por `DERAPAGE`, `ECLIPSE`, `90 PIECE`.
- `NW77PLAQUE` se excluye de NEW y se incluye en nobarcode.
- El import usa la plantilla simplificada `ENZO-Sage50/_master/odoo_templates/products.xlsx`.
  - Columnas fijas: `x` = `E`, `id`, `barcode`, `if_favorite`, `is_storable`, `Description for Sales`, `Item Description`.
- Nota: el fichero de verificacion de productos **ya no se genera**.

Import no-barcode (criterio Ally):
- `python sage_odoo_parity.py build_products_nobarcode_import`
  - Fuente: `products_sync_nobarcode_NEW.csv`
  - Incluye filas:
    - `Invoiced2026 = X`, o
    - `ItemDescription` empieza por `ERKERS `, `BA&SH `, `NW 77TH `, `MONOQOOL `
  - Salida: `odoo_imports/YYYYMMDD_products_nobarcode_NEW.xlsx` usando `products.xlsx`.

Sun vs Optics (variantes) — script dedicado:
- Script: `sync_products_sun_vs_optics.py`
- Fuente: `ENZO-Sage50/_master/odoo_imports/20260507_sun_vs_optics_USA.xlsx`
- Solo procesa filas con `x = E/F`.
- Orden de proceso por fila:
  1) Busca producto por External Id `__import__.{product_code_odoo}`.
  2) Si no existe, **crea** `product.template` con:
     - `name` = `brand_model`
     - `categ_id` (si la categoría existe por nombre)
     - External Id `__import__.{product_code_odoo}`
  3) Asegura valor de atributo **Color** (crea si no existe) y lo añade al producto.
  4) Localiza variante por color, actualiza:
     - `default_code` (SKU)
     - `barcode` (en paso separado)
     - `is_storable = True`
  5) Si la variante ya coincide (SKU + barcode + is_storable), marca **OK**.
- Log:
  - Archivo: `20260507_sun_vs_optics_USA_LOG.xlsx`
  - Columnas clave: `row`, `Item Description`, `color_color_code`, `product_code`, `sku`, `barcode`, `status`, `detail`, `barcode_error`
  - El `barcode` se escribe con prefijo `'` para conservar ceros.
  - `barcode_error` guarda **solo** el primer producto en conflicto (parte izquierda antes de “and”).
  - Filas con `status = OK` se rellenan en verde claro.
  - En el **paso 0**, si no se actualiza barcode, `detail` indica:  
    - `Barcode mismatch (update disabled)`  
    - `Barcode already set (update disabled)`  
    - `Barcode missing (update disabled)`  
    - `No barcode provided (update disabled)`

Flags del script:
- `--update-barcode`  
  - Solo aplica en el **paso 0** (cuando la variante ya existe por SKU).  
  - Por defecto **no** actualiza barcode en ese paso.
- `--verify-color`  
  - Solo aplica en el **paso 0**.  
  - Si se activa, valida que el color de la variante coincide con `color_color_code`.  
  - Si no coincide, marca `ERROR` y no continúa.

Extras (Odoo):
- `refresh_odoo` tambien exporta colores:
  - `ENZO-Sage50/_master_odoo/atributos_color.csv`
  - Campos: `OdooId`, `OdooName`, `AttributeId`, `AttributeName`
