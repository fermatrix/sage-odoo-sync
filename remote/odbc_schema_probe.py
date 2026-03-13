import argparse
import csv
import os
import re
import sys
from datetime import datetime

try:
    import pyodbc
except Exception:
    print("ERROR: pyodbc is required. Install it in the build environment.")
    raise


def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def list_columns(conn, table, catalog=None, schema=None):
    cols = []
    for row in conn.cursor().columns(table=table, catalog=catalog, schema=schema):
        # Expected order (ODBC): table_cat, table_schem, table_name, column_name, data_type,
        # type_name, column_size, buffer_length, decimal_digits, num_prec_radix, nullable,
        # remarks, column_def, sql_data_type, sql_datetime_sub, char_octet_length, ordinal_position, is_nullable
        cols.append(
            {
                "catalog": row[0] if len(row) > 0 else None,
                "schema": row[1] if len(row) > 1 else None,
                "table": row[2] if len(row) > 2 else None,
                "column": row[3] if len(row) > 3 else None,
                "data_type": row[4] if len(row) > 4 else None,
                "type_name": row[5] if len(row) > 5 else None,
                "column_size": row[6] if len(row) > 6 else None,
                "decimal_digits": row[8] if len(row) > 8 else None,
                "nullable": row[10] if len(row) > 10 else None,
                "remarks": row[11] if len(row) > 11 else None,
                "column_def": row[12] if len(row) > 12 else None,
                "ordinal_position": row[16] if len(row) > 16 else None,
                "is_nullable": row[17] if len(row) > 17 else None,
            }
        )
    return cols


def try_sample_rows(conn, table, limit=5):
    cursor = conn.cursor()
    # Try bracket quoting first, then double quotes
    queries = [
        f"SELECT TOP {limit} * FROM [{table}]",
        f"SELECT TOP {limit} * FROM \"{table}\"",
        f"SELECT * FROM [{table}]",
        f"SELECT * FROM \"{table}\"",
    ]
    last_exc = None
    for q in queries:
        try:
            cursor.execute(q)
            rows = cursor.fetchmany(limit)
            columns = [desc[0] for desc in cursor.description]
            return columns, rows
        except Exception as exc:
            last_exc = exc
            continue
    raise last_exc


def main():
    parser = argparse.ArgumentParser(description="ODBC schema + sample rows probe for Sage 50")
    parser.add_argument("--dsn", default=os.environ.get("SAGE_ODBC_DSN", "SAGE"))
    parser.add_argument("--user", default=os.environ.get("SAGE_ODBC_USER", "Peachtree"))
    parser.add_argument("--password", default=os.environ.get("SAGE_ODBC_PASSWORD", ""))
    parser.add_argument("--catalog", default=os.environ.get("SAGE_ODBC_CATALOG", "STUDIOOPTYXINC"))
    parser.add_argument("--schema", default=os.environ.get("SAGE_ODBC_SCHEMA", ""))
    parser.add_argument(
        "--tables",
        default=os.environ.get(
            "SAGE_ODBC_TABLES",
            "Customers,JrnlHdr,JrnlRow,StoredTransHeaders,StoredTransRows,LineItem,Tax_Code,Tax_Authority,PaymentMethod",
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=os.environ.get("SAGE_OUTPUT_DIR", r"C:\Users\soadmin\Dropbox\ENZO-Sage50\_tests"),
    )
    args = parser.parse_args()

    if not args.password:
        print("ERROR: missing password. Set --password or SAGE_ODBC_PASSWORD.")
        return 2

    conn_str = f"DSN={args.dsn};UID={args.user};PWD={args.password}"

    try:
        conn = pyodbc.connect(conn_str, timeout=10)
    except Exception as exc:
        print("ERROR: ODBC connection failed")
        print(str(exc))
        return 3

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    errors = []
    try:
        for table in tables:
            safe = safe_filename(table)
            # Columns
            cols = list_columns(conn, table, catalog=args.catalog or None, schema=args.schema or None)
            cols_path = os.path.join(args.out_dir, f"schema_{safe}_{stamp}.csv")
            with open(cols_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "catalog",
                        "schema",
                        "table",
                        "column",
                        "data_type",
                        "type_name",
                        "column_size",
                        "decimal_digits",
                        "nullable",
                        "remarks",
                        "column_def",
                        "ordinal_position",
                        "is_nullable",
                    ],
                )
                writer.writeheader()
                writer.writerows(cols)

            # Sample rows
            try:
                col_names, rows = try_sample_rows(conn, table, limit=5)
                sample_path = os.path.join(args.out_dir, f"sample_{safe}_{stamp}.csv")
                with open(sample_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(col_names)
                    writer.writerows(rows)
            except Exception as exc:
                errors.append(f"{table}: sample rows failed: {exc}")

    finally:
        conn.close()

    if errors:
        print("WARN: completed with some sample errors")
        for e in errors:
            print(" - " + e)

    print(f"OK: schema + samples exported to {args.out_dir} ({stamp})")
    return 0


if __name__ == "__main__":
    sys.exit(main())