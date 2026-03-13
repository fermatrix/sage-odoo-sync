import argparse
import csv
import os
import sys
from datetime import datetime

try:
    import pyodbc
except Exception:
    print("ERROR: pyodbc is required. Install it in the build environment.")
    raise


def list_tables(conn):
    tables = []
    for row in conn.cursor().tables():
        # Some drivers return rows without named attributes; fall back to indexes.
        # Expected order: table_cat, table_schem, table_name, table_type, remarks
        catalog = row[0] if len(row) > 0 else None
        schema = row[1] if len(row) > 1 else None
        name = row[2] if len(row) > 2 else None
        table_type = row[3] if len(row) > 3 else None
        remarks = row[4] if len(row) > 4 else None
        tables.append(
            {
                "catalog": catalog,
                "schema": schema,
                "name": name,
                "type": table_type,
                "remarks": remarks,
            }
        )
    return tables


def main():
    parser = argparse.ArgumentParser(description="ODBC connectivity probe for Sage 50")
    parser.add_argument("--dsn", default=os.environ.get("SAGE_ODBC_DSN", "SAGE"))
    parser.add_argument("--user", default=os.environ.get("SAGE_ODBC_USER", "Peachtree"))
    parser.add_argument("--password", default=os.environ.get("SAGE_ODBC_PASSWORD", ""))
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

    try:
        tables = list_tables(conn)
    finally:
        conn.close()

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(args.out_dir, f"odbc_tables_{stamp}.csv")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["catalog", "schema", "name", "type", "remarks"])
        writer.writeheader()
        writer.writerows(tables)

    print(f"OK: {len(tables)} tables exported to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
