import argparse
import csv
import os
import sys

from common_format import format_value

try:
    import pyodbc
except Exception:
    print("ERROR: pyodbc is required. Install it in the build environment.")
    raise


def is_binary_column(name: str) -> bool:
    lowered = name.lower()
    return (
        "binary" in lowered
        or "guid" in lowered
        or "acctref" in lowered
    )


def main():
    parser = argparse.ArgumentParser(description="Export Sage 50 master tables (full columns)")
    parser.add_argument("--dsn", default=os.environ.get("SAGE_ODBC_DSN", "SAGE"))
    parser.add_argument("--user", default=os.environ.get("SAGE_ODBC_USER", "Peachtree"))
    parser.add_argument("--password", default=os.environ.get("SAGE_ODBC_PASSWORD", ""))
    parser.add_argument("--table", required=True, help="Table name, e.g. Customers or LineItem")
    parser.add_argument("--out-dir", default=os.environ.get("SAGE_OUTPUT_DIR", r"C:\Users\soadmin\Dropbox\ENZO-Sage50\_master"))
    parser.add_argument("--out-name", required=True, help="Base output name without extension")
    parser.add_argument("--encoding", default="utf-8")
    args = parser.parse_args()

    if not args.password:
        print("ERROR: missing password. Set --password or SAGE_ODBC_PASSWORD.")
        return 2

    if not args.table.replace("_", "").isalnum():
        print("ERROR: invalid table name. Use only letters, numbers, and underscore.")
        return 2

    conn_str = f"DSN={args.dsn};UID={args.user};PWD={args.password}"

    try:
        conn = pyodbc.connect(conn_str, timeout=10)
    except Exception as exc:
        print("ERROR: ODBC connection failed")
        print(str(exc))
        return 3

    os.makedirs(args.out_dir, exist_ok=True)
    main_path = os.path.join(args.out_dir, f"{args.out_name}.csv")
    bin_path = os.path.join(args.out_dir, f"{args.out_name}_binaries.csv")

    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {args.table}")
        columns = [d[0] for d in cur.description]

        bin_cols = [c for c in columns if is_binary_column(c)]
        non_bin_cols = columns

        # Build binary file key columns based on known identifiers
        key_cols = []
        if "CustomerRecordNumber" in columns:
            key_cols.append("CustomerRecordNumber")
        if "CustomerID" in columns:
            key_cols.append("CustomerID")
        if "ItemRecordNumber" in columns:
            key_cols.append("ItemRecordNumber")
        if "ItemID" in columns:
            key_cols.append("ItemID")

        with open(main_path, "w", newline="", encoding=args.encoding) as f_main, \
             open(bin_path, "w", newline="", encoding=args.encoding) as f_bin:
            w_main = csv.writer(f_main, delimiter=";")
            w_bin = csv.writer(f_bin, delimiter=";")

            w_main.writerow(non_bin_cols)
            w_bin.writerow(key_cols + bin_cols)

            idx = {c: columns.index(c) for c in columns}

            for row in cur:
                out_row = []
                for c in non_bin_cols:
                    if is_binary_column(c):
                        out_row.append("<binary>")
                    else:
                        out_row.append(format_value(row[idx[c]]))
                w_main.writerow(out_row)

                bin_row = []
                for c in key_cols + bin_cols:
                    if c in idx:
                        bin_row.append(format_value(row[idx[c]]))
                    else:
                        bin_row.append("")
                w_bin.writerow(bin_row)

    finally:
        conn.close()

    print(f"OK: exported {args.table} to {main_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
