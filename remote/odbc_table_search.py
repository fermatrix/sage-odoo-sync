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


def main():
    parser = argparse.ArgumentParser(description="ODBC search tables by name pattern")
    parser.add_argument("--dsn", default=os.environ.get("SAGE_ODBC_DSN", "SAGE"))
    parser.add_argument("--user", default=os.environ.get("SAGE_ODBC_USER", "Peachtree"))
    parser.add_argument("--password", default=os.environ.get("SAGE_ODBC_PASSWORD", ""))
    parser.add_argument("--pattern", default="order|sales|so|storedtrans|quote", help="regex pattern")
    parser.add_argument("--out-dir", default=os.environ.get("SAGE_OUTPUT_DIR", r"C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\output"))
    parser.add_argument("--out-name", default=None)
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

    import re
    rx = re.compile(args.pattern, re.IGNORECASE)

    rows = []
    try:
        for row in conn.cursor().tables():
            name = row[2] if len(row) > 2 else None
            if name and rx.search(str(name)):
                rows.append(
                    {
                        "catalog": row[0] if len(row) > 0 else None,
                        "schema": row[1] if len(row) > 1 else None,
                        "name": name,
                        "type": row[3] if len(row) > 3 else None,
                    }
                )
    finally:
        conn.close()

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = args.out_name or f"table_search_{stamp}.csv"
    out_path = os.path.join(args.out_dir, out_name)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["catalog", "schema", "name", "type"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"OK: {len(rows)} matches written to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())