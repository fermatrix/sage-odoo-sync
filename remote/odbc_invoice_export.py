import argparse
import csv
import os
import sys
from datetime import datetime, date

from common_format import format_value

try:
    import pyodbc
except Exception:
    print("ERROR: pyodbc is required. Install it in the build environment.")
    raise


def fiscal_month_folder(d: date) -> str:
    # Fiscal year starts Feb 1. Feb => 01_02_Feb, Mar => 02_03_Mar, ..., Jan => 12_01_Jan
    fiscal_index = ((d.month - 2) % 12) + 1
    month_num = d.month
    month_name = d.strftime("%b")
    return f"{fiscal_index:02d}_{month_num:02d}_{month_name}"


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser(description="Export Sage 50 invoices/credit notes by fiscal month")
    parser.add_argument("--dsn", default=os.environ.get("SAGE_ODBC_DSN", "SAGE"))
    parser.add_argument("--user", default=os.environ.get("SAGE_ODBC_USER", "Peachtree"))
    parser.add_argument("--password", default=os.environ.get("SAGE_ODBC_PASSWORD", ""))
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD (exclusive)")
    parser.add_argument("--module", default="R", help="Filter Module (default R)")
    parser.add_argument("--invoice-journalex", default="8", help="Comma-separated JournalEx for invoices")
    parser.add_argument("--credit-journalex", default="3", help="Comma-separated JournalEx for credit notes")
    parser.add_argument("--invoice", action="store_true", help="Export invoices only (skip credit notes)")
    parser.add_argument("--credit-note", action="store_true", help="Export credit notes only (skip invoices)")
    parser.add_argument("--out-dir", default=os.environ.get("SAGE_OUTPUT_DIR", r"C:\Users\soadmin\Dropbox\ENZO-Sage50\13_2026"))
    parser.add_argument("--encoding", default="utf-8")
    args = parser.parse_args()

    if not args.password:
        print("ERROR: missing password. Set --password or SAGE_ODBC_PASSWORD.")
        return 2

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date) if args.end_date else None

    invoice_jex = {int(x.strip()) for x in args.invoice_journalex.split(",") if x.strip()}
    credit_jex = {int(x.strip()) for x in args.credit_journalex.split(",") if x.strip()}

    if args.invoice and args.credit_note:
        print("ERROR: --invoice and --credit-note are mutually exclusive.")
        return 2
    if args.invoice:
        credit_jex = set()
    if args.credit_note:
        invoice_jex = set()

    conn_str = f"DSN={args.dsn};UID={args.user};PWD={args.password}"

    try:
        conn = pyodbc.connect(conn_str, timeout=10)
    except Exception as exc:
        print("ERROR: ODBC connection failed")
        print(str(exc))
        return 3

    os.makedirs(args.out_dir, exist_ok=True)

    # Writers per month/type for headers and lines
    writers = {}
    files = {}

    def is_binary_column(name: str) -> bool:
        lowered = name.lower()
        return (
            "binary" in lowered
            or "guid" in lowered
            or "acctref" in lowered
        )

    def get_writer(d: date, kind: str, section: str, columns):
        folder = fiscal_month_folder(d)
        month_dir = os.path.join(args.out_dir, folder)
        os.makedirs(month_dir, exist_ok=True)
        if section == "header":
            if kind == "invoice":
                filename = f"{d.year}_{d.month:02d}_invoice.csv"
            else:
                filename = f"{d.year}_{d.month:02d}_credit_note.csv"
        else:
            if kind == "invoice":
                filename = f"{d.year}_{d.month:02d}_invoice_lines.csv"
            else:
                filename = f"{d.year}_{d.month:02d}_credit_note_lines.csv"
        path = os.path.join(month_dir, filename)

        key = (month_dir, kind, section)
        if key not in writers:
            f = open(path, "w", newline="", encoding=args.encoding)
            files[key] = f
            writer = csv.writer(f, delimiter=";")
            writer.writerow(columns)
            writers[key] = writer
        return writers[key], columns

    def get_binary_writer(d: date, kind: str, section: str, columns, key_columns):
        folder = fiscal_month_folder(d)
        month_dir = os.path.join(args.out_dir, folder)
        os.makedirs(month_dir, exist_ok=True)
        if section == "header":
            if kind == "invoice":
                filename = f"{d.year}_{d.month:02d}_invoice_binaries.csv"
            else:
                filename = f"{d.year}_{d.month:02d}_credit_note_binaries.csv"
        else:
            if kind == "invoice":
                filename = f"{d.year}_{d.month:02d}_invoice_lines_binaries.csv"
            else:
                filename = f"{d.year}_{d.month:02d}_credit_note_lines_binaries.csv"
        path = os.path.join(month_dir, filename)

        bin_cols = [c for c in columns if is_binary_column(c)]
        out_cols = key_columns + bin_cols

        key = (month_dir, kind, section, "bin")
        if key not in writers:
            f = open(path, "w", newline="", encoding=args.encoding)
            files[key] = f
            writer = csv.writer(f, delimiter=";")
            writer.writerow(out_cols)
            writers[key] = writer
        return writers[key], out_cols

    # Build filter fragments
    def build_filters(base_params, jex_list):
        clause = " WHERE h.TrxIsPosted = 1 AND h.TransactionDate >= ?"
        params = list(base_params)
        if end_date:
            clause += " AND h.TransactionDate < ?"
            params.append(end_date)
        if args.module:
            clause += " AND h.Module = ?"
            params.append(args.module)
        if jex_list:
            placeholders = ",".join(["?"] * len(jex_list))
            clause += f" AND h.JournalEx IN ({placeholders})"
            params.extend(jex_list)
        return clause, params

    try:
        cur = conn.cursor()

        def export_headers(kind, jex_list):
            if not jex_list:
                return
            header_clause, header_params = build_filters([start_date], jex_list)
            header_sql = (
                "SELECT h.* FROM JrnlHdr h"
                + header_clause
                + " ORDER BY h.TransactionDate, h.PostOrder"
            )
            cur.execute(header_sql, header_params)
            header_columns = [d[0] for d in cur.description]
            for row in cur:
                trx_date = row[header_columns.index("TransactionDate")] if "TransactionDate" in header_columns else row[0]
                if isinstance(trx_date, datetime):
                    trx_date = trx_date.date()
                elif not isinstance(trx_date, date):
                    trx_date = parse_date(str(trx_date))
                writer, out_cols = get_writer(trx_date, kind, "header", header_columns)
                idx = {c: header_columns.index(c) for c in out_cols if c in header_columns}
                out_row = []
                for c in out_cols:
                    if is_binary_column(c):
                        out_row.append("<binary>")
                    else:
                        out_row.append(format_value(row[idx[c]]))
                writer.writerow(out_row)

                # Binary columns to separate file (join key: PostOrder)
                if "PostOrder" in header_columns:
                    bin_writer, out_cols = get_binary_writer(trx_date, kind, "header", header_columns, ["PostOrder"])
                    idx = {c: header_columns.index(c) for c in out_cols if c in header_columns}
                    bin_row = []
                    for c in out_cols:
                        if c in idx:
                            bin_row.append(format_value(row[idx[c]]))
                        else:
                            bin_row.append("")
                    bin_writer.writerow(bin_row)

        def export_lines(kind, jex_list):
            if not jex_list:
                return
            line_clause, line_params = build_filters([start_date], jex_list)
            line_sql = (
                "SELECT r.* FROM JrnlRow r "
                "JOIN JrnlHdr h ON r.PostOrder = h.PostOrder "
                + line_clause
                + " AND r.RowNumber > 0"
                + " ORDER BY h.TransactionDate, h.PostOrder, r.RowNumber"
            )
            cur.execute(line_sql, line_params)
            line_columns = [d[0] for d in cur.description]
            for row in cur:
                trx_date = start_date
                if "RowDate" in line_columns:
                    rd = row[line_columns.index("RowDate")]
                    if isinstance(rd, datetime):
                        trx_date = rd.date()
                    elif isinstance(rd, date):
                        trx_date = rd
                    elif rd is not None:
                        trx_date = parse_date(str(rd))
                writer, out_cols = get_writer(trx_date, kind, "lines", line_columns)
                idx = {c: line_columns.index(c) for c in out_cols if c in line_columns}
                out_row = []
                for c in out_cols:
                    if is_binary_column(c):
                        out_row.append("<binary>")
                    else:
                        out_row.append(format_value(row[idx[c]]))
                writer.writerow(out_row)

                # Binary columns to separate file (join key: PostOrder + RowNumber)
                key_cols = []
                if "PostOrder" in line_columns:
                    key_cols.append("PostOrder")
                if "RowNumber" in line_columns:
                    key_cols.append("RowNumber")
                if key_cols:
                    bin_writer, out_cols = get_binary_writer(trx_date, kind, "lines", line_columns, key_cols)
                    idx = {c: line_columns.index(c) for c in out_cols if c in line_columns}
                    bin_row = []
                    for c in out_cols:
                        if c in idx:
                            bin_row.append(format_value(row[idx[c]]))
                        else:
                            bin_row.append("")
                    bin_writer.writerow(bin_row)

        export_headers("invoice", sorted(invoice_jex))
        export_headers("credit", sorted(credit_jex))
        export_lines("invoice", sorted(invoice_jex))
        export_lines("credit", sorted(credit_jex))

    finally:
        conn.close()
        for f in files.values():
            f.close()

    print("OK: export completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
