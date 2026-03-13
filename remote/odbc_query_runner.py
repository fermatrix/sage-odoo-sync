import argparse
import csv
import os
import sys
from datetime import datetime

from common_format import format_value

try:
    import pyodbc
except Exception:
    print("ERROR: pyodbc is required. Install it in the build environment.")
    raise


def main():
    parser = argparse.ArgumentParser(description="Run an arbitrary SQL query via ODBC and export to CSV")
    parser.add_argument("--dsn", default=os.environ.get("SAGE_ODBC_DSN", "SAGE"))
    parser.add_argument("--user", default=os.environ.get("SAGE_ODBC_USER", "Peachtree"))
    parser.add_argument("--password", default=os.environ.get("SAGE_ODBC_PASSWORD", ""))
    parser.add_argument("--query", default=None)
    parser.add_argument("--sql-file", default=None)
    parser.add_argument("--out-dir", default=os.environ.get("SAGE_OUTPUT_DIR", r"C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec_output"))
    parser.add_argument("--out-name", default=None)
    parser.add_argument("--max-rows", type=int, default=0, help="0 = no limit")
    args = parser.parse_args()

    if not args.password:
        print("ERROR: missing password. Set --password or SAGE_ODBC_PASSWORD.")
        return 2

    query = args.query
    if args.sql_file:
        with open(args.sql_file, "r", encoding="utf-8") as f:
            query = f.read()

    if not query:
        print("ERROR: missing --query or --sql-file")
        return 2

    conn_str = f"DSN={args.dsn};UID={args.user};PWD={args.password}"

    try:
        conn = pyodbc.connect(conn_str, timeout=10)
    except Exception as exc:
        print("ERROR: ODBC connection failed")
        print(str(exc))
        return 3

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = args.out_name or f"query_result_{stamp}.csv"
    out_path = os.path.join(args.out_dir, out_name)

    rows_written = 0
    try:
        cur = conn.cursor()
        cur.execute(query)
        columns = [d[0] for d in cur.description] if cur.description else []

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if columns:
                writer.writerow(columns)
            while True:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    writer.writerow([format_value(v) for v in row])
                    rows_written += 1
                    if args.max_rows and rows_written >= args.max_rows:
                        break
                if args.max_rows and rows_written >= args.max_rows:
                    break
    finally:
        conn.close()

    print(f"OK: wrote {rows_written} rows to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
