import io
import hashlib
import sqlite3
from datetime import date, datetime
from pathlib import Path
from functools import partial

import pandas as pd
import streamlit as st
from openpyxl.utils import get_column_letter

# Always store DB next to this file (prevents "db in wrong folder" issues)
DB_PATH = str(Path(__file__).with_name("payments.db"))

# -------------------------
# Helpers
# -------------------------
def format_date_eu(d) -> str:
    """Return dd-mm-yyyy. Accepts date/datetime/string."""
    dt = pd.to_datetime(d, errors="coerce")
    if pd.isna(dt):
        return str(d)
    return dt.strftime("%d-%m-%Y")


# -------------------------
# Excel helpers (auto-fit columns)
# -------------------------
def autosize_worksheet(ws, max_width: int = 60, min_width: int = 10):
    """
    Auto-size columns based on content length.
    Caps width to max_width to avoid huge columns.
    """
    for col_cells in ws.columns:
        col_letter = get_column_letter(col_cells[0].column)
        max_len = 0
        for cell in col_cells:
            val = cell.value
            if val is None:
                continue
            s = str(val)
            if len(s) > max_len:
                max_len = len(s)
        width = max(min_width, min(max_width, max_len + 2))
        ws.column_dimensions[col_letter].width = width


# -------------------------
# DB helpers + migration
# -------------------------
def ensure_vineyards_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vineyards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
        """
    )
    conn.commit()


def get_vineyards(conn) -> list[str]:
    cur = conn.execute("SELECT name FROM vineyards ORDER BY name")
    return [r[0] for r in cur.fetchall()]


def upsert_vineyard(conn, name: str):
    name = (name or "").strip()
    if not name:
        return
    conn.execute("INSERT OR IGNORE INTO vineyards (name) VALUES (?)", (name,))
    conn.commit()


def has_column(conn, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols


def ensure_import_hash_column(conn):
    # Add import_hash column if missing (for duplicate protection)
    if not has_column(conn, "transactions", "import_hash"):
        conn.execute("ALTER TABLE transactions ADD COLUMN import_hash TEXT")
        conn.commit()
        conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_import_hash ON transactions(import_hash)")
        conn.commit()


def compute_row_hash(vineyard, payer, txn_date, kind, amount_cents, reference) -> str:
    # Normalize date + kind so duplicates are reliably detected across imports
    v = (vineyard or "").strip()
    p = (payer or "").strip()
    k = (kind or "").strip().upper()

    dt = pd.to_datetime(txn_date, errors="coerce")
    d = dt.date().isoformat() if pd.notna(dt) else str(txn_date).strip()

    ref = (reference or "").strip()

    raw = f"{v}|{p}|{d}|{k}|{int(amount_cents)}|{ref}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    # Create NEW schema if not exists
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vineyard TEXT NOT NULL,
            payer TEXT NOT NULL,
            txn_date TEXT NOT NULL,
            kind TEXT NOT NULL,              -- PAYMENT | TRANSFER | INVOICE | CORRECTION
            amount_cents INTEGER NOT NULL,   -- can be negative for CORRECTION / opening balances
            reference TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # Detect if this is an OLD schema (no vineyard column)
    cur = conn.execute("PRAGMA table_info(transactions)")
    cols = [r[1] for r in cur.fetchall()]
    if "vineyard" not in cols:
        # Migrate old table
        conn.execute("ALTER TABLE transactions RENAME TO transactions_old")

        conn.execute(
            """
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vineyard TEXT NOT NULL,
                payer TEXT NOT NULL,
                txn_date TEXT NOT NULL,
                kind TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                reference TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        cur2 = conn.execute("PRAGMA table_info(transactions_old)")
        old_cols = [r[1] for r in cur2.fetchall()]
        old_cols_set = set(old_cols)

        # Supported old schema:
        # customer, txn_date, txn_type, amount_cents, reference?, created_at?
        if {"customer", "txn_date", "txn_type", "amount_cents"}.issubset(old_cols_set):
            has_reference = "reference" in old_cols_set
            has_created_at = "created_at" in old_cols_set

            if has_reference and has_created_at:
                conn.execute(
                    """
                    INSERT INTO transactions (id, vineyard, payer, txn_date, kind, amount_cents, reference, created_at)
                    SELECT
                        id,
                        customer AS vineyard,
                        '(UNKNOWN PAYER)' AS payer,
                        txn_date,
                        txn_type AS kind,
                        amount_cents,
                        reference,
                        created_at
                    FROM transactions_old
                    """
                )
            elif has_reference and not has_created_at:
                conn.execute(
                    """
                    INSERT INTO transactions (id, vineyard, payer, txn_date, kind, amount_cents, reference, created_at)
                    SELECT
                        id,
                        customer AS vineyard,
                        '(UNKNOWN PAYER)' AS payer,
                        txn_date,
                        txn_type AS kind,
                        amount_cents,
                        reference,
                        ?
                    FROM transactions_old
                    """,
                    (datetime.utcnow().isoformat(),),
                )
            elif not has_reference and has_created_at:
                conn.execute(
                    """
                    INSERT INTO transactions (id, vineyard, payer, txn_date, kind, amount_cents, reference, created_at)
                    SELECT
                        id,
                        customer AS vineyard,
                        '(UNKNOWN PAYER)' AS payer,
                        txn_date,
                        txn_type AS kind,
                        amount_cents,
                        NULL AS reference,
                        created_at
                    FROM transactions_old
                    """
                )
            else:
                conn.execute(
                    """
                    INSERT INTO transactions (id, vineyard, payer, txn_date, kind, amount_cents, reference, created_at)
                    SELECT
                        id,
                        customer AS vineyard,
                        '(UNKNOWN PAYER)' AS payer,
                        txn_date,
                        txn_type AS kind,
                        amount_cents,
                        NULL AS reference,
                        ?
                    FROM transactions_old
                    """,
                    (datetime.utcnow().isoformat(),),
                )

            conn.execute("DROP TABLE transactions_old")
            conn.commit()
        else:
            # Restore original to avoid data loss
            conn.execute("DROP TABLE transactions")
            conn.execute("ALTER TABLE transactions_old RENAME TO transactions")
            conn.commit()
            raise RuntimeError(
                "Found an unexpected older database schema. "
                "I did NOT overwrite anything. Please delete payments.db (if safe) or share the old columns."
            )

    # Ensure vineyards table exists
    ensure_vineyards_table(conn)

    # Convert any old REFUND rows to TRANSFER
    conn.execute("UPDATE transactions SET kind='TRANSFER' WHERE kind='REFUND'")
    conn.commit()

    # Ensure import_hash column exists for safe imports
    try:
        ensure_import_hash_column(conn)
    except Exception:
        pass

    # Populate vineyards table from existing transactions (so dropdown isn’t empty)
    cur3 = conn.execute("SELECT DISTINCT vineyard FROM transactions")
    for (v,) in cur3.fetchall():
        upsert_vineyard(conn, v)

    return conn


def to_cents(amount: float) -> int:
    return int(round(amount * 100))


def from_cents(cents: int) -> float:
    return cents / 100.0


def signed_amount(kind: str, amount: float) -> float:
    k = (kind or "").upper()
    if k == "PAYMENT":
        return amount
    if k in ("TRANSFER", "INVOICE"):
        return -amount
    if k == "CORRECTION":
        # Correction is stored as signed amount (can be + or -)
        return amount
    return 0.0


def insert_txn(conn, vineyard, payer, txn_date, kind, amount_cents, reference, import_hash: str | None = None):
    upsert_vineyard(conn, vineyard)
    if has_column(conn, "transactions", "import_hash"):
        conn.execute(
            """
            INSERT INTO transactions (vineyard, payer, txn_date, kind, amount_cents, reference, created_at, import_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vineyard.strip(),
                payer.strip(),
                txn_date,
                kind.strip().upper(),
                amount_cents,
                reference.strip() if reference else None,
                datetime.utcnow().isoformat(),
                import_hash,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO transactions (vineyard, payer, txn_date, kind, amount_cents, reference, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vineyard.strip(),
                payer.strip(),
                txn_date,
                kind.strip().upper(),
                amount_cents,
                reference.strip() if reference else None,
                datetime.utcnow().isoformat(),
            ),
        )
    conn.commit()


def update_txn(conn, txn_id, vineyard, payer, txn_date, kind, amount_cents, reference):
    upsert_vineyard(conn, vineyard)
    conn.execute(
        """
        UPDATE transactions
        SET vineyard=?, payer=?, txn_date=?, kind=?, amount_cents=?, reference=?
        WHERE id=?
        """,
        (
            vineyard.strip(),
            payer.strip(),
            txn_date,
            kind.strip().upper(),
            amount_cents,
            reference.strip() if reference else None,
            txn_id,
        ),
    )
    conn.commit()


def delete_txn(conn, txn_id):
    conn.execute("DELETE FROM transactions WHERE id=?", (txn_id,))
    conn.commit()


def fetch_all(conn) -> pd.DataFrame:
    # include import_hash if exists
    if has_column(conn, "transactions", "import_hash"):
        sql = """
            SELECT id, vineyard, payer, txn_date, kind, amount_cents, reference, created_at, import_hash
            FROM transactions
            ORDER BY txn_date DESC, id DESC
        """
    else:
        sql = """
            SELECT id, vineyard, payer, txn_date, kind, amount_cents, reference, created_at
            FROM transactions
            ORDER BY txn_date DESC, id DESC
        """
    df = pd.read_sql_query(sql, conn)

    if df.empty:
        return df

    df["amount"] = df["amount_cents"].apply(from_cents)
    df["signed_amount"] = df.apply(lambda r: signed_amount(r["kind"], r["amount"]), axis=1)
    df["date"] = pd.to_datetime(df["txn_date"], errors="coerce").dt.date
    df["reference"] = df["reference"].fillna("")

    kind_map = {
        "PAYMENT": "Payment received",
        "TRANSFER": "Transfer to vineyard",
        "INVOICE": "Monthly invoice (deduction)",
        "CORRECTION": "Correction",
    }
    df["type"] = df["kind"].map(kind_map).fillna(df["kind"])
    return df


def add_running_balance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds balance_after column (running balance per vineyard after each entry).
    Running balance is computed in chronological order by txn_date then id.
    """
    if df.empty:
        return df

    work = df.copy()
    work["_dt"] = pd.to_datetime(work["txn_date"], errors="coerce")
    work = work.sort_values(["vineyard", "_dt", "id"], ascending=[True, True, True])

    work["balance_after"] = work.groupby("vineyard")["signed_amount"].cumsum()

    out = df.merge(work[["id", "balance_after"]], on="id", how="left")
    return out


def balances_by_vineyard(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["vineyard", "balance"])
    out = (
        df.groupby("vineyard", as_index=False)["signed_amount"]
        .sum()
        .rename(columns={"signed_amount": "balance"})
        .sort_values("vineyard")
    )
    return out


def format_money(x: float, currency="EUR") -> str:
    sign = "-" if x < 0 else ""
    return f"{sign}{currency} {abs(x):,.2f}"


# -------------------------
# Excel export functions (auto-fit included)
# -------------------------
def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.book[sheet_name]
        ws.freeze_panes = "A2"
        autosize_worksheet(ws)
    return output.getvalue()


def vineyards_workbook_bytes(df_all: pd.DataFrame, currency_label: str = "EUR") -> bytes:
    """
    Creates an Excel workbook with:
      - Summary sheet (vineyard + final balance)
      - One sheet per vineyard with all entries + running balance (numeric)
    Auto-fits columns on all sheets.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if df_all.empty:
            pd.DataFrame(columns=["vineyard", "balance"]).to_excel(writer, index=False, sheet_name="Summary")
            ws = writer.book["Summary"]
            ws.freeze_panes = "A2"
            autosize_worksheet(ws)
            return output.getvalue()

        work = df_all.copy()
        if "balance_after" not in work.columns:
            work = add_running_balance(work)

        # Summary
        summary = balances_by_vineyard(work).copy()
        summary = summary.rename(columns={"balance": f"balance ({currency_label})"})
        summary.to_excel(writer, index=False, sheet_name="Summary")
        ws_sum = writer.book["Summary"]
        ws_sum.freeze_panes = "A2"
        autosize_worksheet(ws_sum)

        # One sheet per vineyard
        for vineyard, vdf in work.groupby("vineyard"):
            # reference moved right behind payer
            sheet_df = vdf.sort_values(["txn_date", "id"], ascending=[True, True])[
                ["txn_date", "payer", "reference", "type", "amount", "signed_amount", "balance_after"]
            ].copy()

            sheet_df = sheet_df.rename(
                columns={
                    "txn_date": "date",
                    "amount": f"amount ({currency_label})",
                    "signed_amount": f"signed amount ({currency_label})",
                    "balance_after": f"balance after ({currency_label})",
                }
            )

            # EU date formatting in workbook
            sheet_df["date"] = sheet_df["date"].apply(format_date_eu)

            # Excel sheet name rules: max 31 chars, no []:*?/\
            safe_name = "".join(ch for ch in str(vineyard) if ch not in r"[]:*?/\\")
            safe_name = safe_name[:31] if safe_name else "Vineyard"

            sheet_df.to_excel(writer, index=False, sheet_name=safe_name)
            ws = writer.book[safe_name]
            ws.freeze_panes = "A2"
            autosize_worksheet(ws)

    return output.getvalue()


# -------------------------
# Import helpers
# -------------------------
def import_transactions_from_df(conn, imp: pd.DataFrame, skip_duplicates: bool = True) -> tuple[int, int]:
    inserted = 0
    skipped = 0

    for _, r in imp.iterrows():
        vineyard = str(r["vineyard"]).strip()
        payer = str(r["payer"]).strip()
        txn_date = str(r["date"])
        kind = str(r["type"]).strip().upper()
        amount = float(r["amount"])
        reference = "" if pd.isna(r.get("reference", "")) else str(r.get("reference", "")).strip()

        amount_cents = to_cents(amount)
        row_hash = compute_row_hash(vineyard, payer, txn_date, kind, amount_cents, reference)

        if skip_duplicates and has_column(conn, "transactions", "import_hash"):
            cur = conn.execute("SELECT 1 FROM transactions WHERE import_hash=? LIMIT 1", (row_hash,))
            if cur.fetchone():
                skipped += 1
                continue

        insert_txn(conn, vineyard, payer, txn_date, kind, amount_cents, reference, import_hash=row_hash)
        inserted += 1

    return inserted, skipped


# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="Cellar Credits 💸", layout="wide")
st.title("💸 Cellar Credits 💸")
st.caption("Track payments received per vineyard, transfers to vineyards, monthly invoice deductions, and corrections.")

conn = get_conn()
df = fetch_all(conn)
df = add_running_balance(df)

# Sidebar settings
st.sidebar.header("Settings")
currency = st.sidebar.selectbox("Currency label", ["EUR", "GBP", "USD"], index=0)

# Filters
vineyard_list = get_vineyards(conn)
all_payers = sorted(df["payer"].unique().tolist()) if not df.empty else []

st.sidebar.markdown("---")
selected_vineyard = st.sidebar.selectbox("Filter by vineyard", ["(All)"] + vineyard_list)
selected_payer = st.sidebar.selectbox("Filter by payer", ["(All)"] + all_payers)

# Vineyard manager
st.sidebar.markdown("---")
st.sidebar.subheader("Vineyards")

vineyard_list = get_vineyards(conn)

new_vineyard = st.sidebar.text_input("Add new vineyard", placeholder="e.g. Château Example")
if st.sidebar.button("Add vineyard"):
    if not new_vineyard.strip():
        st.sidebar.error("Enter a vineyard name.")
    else:
        upsert_vineyard(conn, new_vineyard)
        st.sidebar.success("Vineyard added.")
        st.rerun()

with st.sidebar.expander("Delete a vineyard (only if unused)"):
    vineyard_list = get_vineyards(conn)
    del_choice = st.sidebar.selectbox("Select vineyard to delete", ["(Select)"] + vineyard_list)
    if st.sidebar.button("Delete selected vineyard"):
        if del_choice == "(Select)":
            st.sidebar.warning("Choose a vineyard first.")
        else:
            cur = conn.execute("SELECT COUNT(1) FROM transactions WHERE vineyard=?", (del_choice,))
            count = cur.fetchone()[0]
            if count > 0:
                st.sidebar.error("Cannot delete: vineyard has transactions.")
            else:
                conn.execute("DELETE FROM vineyards WHERE name=?", (del_choice,))
                conn.commit()
                st.sidebar.success("Deleted from dropdown list.")
                st.rerun()
# -------------------------
# Add transactions
# -------------------------
st.subheader("Add entry")

tab1, tab2 = st.tabs(["Payment / Transfer / Correction", "Monthly invoice (deduction)"])

with tab1:
    payer_key = "payer_add"

    # --- FORM (no buttons except st.form_submit_button) ---
    with st.form("add_payment_transfer", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([2, 2, 1, 2])

        vineyard_list = get_vineyards(conn)
        with c1:
            vineyard = st.selectbox("Vineyard *", options=vineyard_list, index=None, placeholder="Select vineyard")

        with c2:
            payer = st.text_input(
                "Payer *",
                key=payer_key,
                placeholder="e.g. Restaurant XYZ / Importer ABC",
            )

        with c3:
            txn_date = st.date_input("Date *", value=date.today())

        with c4:
            kind_label = st.selectbox("Type *", ["Payment received", "Transfer to vineyard", "Correction"])
            if kind_label == "Payment received":
                kind = "PAYMENT"
            elif kind_label == "Transfer to vineyard":
                kind = "TRANSFER"
            else:
                kind = "CORRECTION"

        if kind == "CORRECTION":
            amount = st.number_input("Amount * (can be negative)", value=0.00, step=0.01, format="%.2f")
        else:
            amount = st.number_input("Amount *", min_value=0.00, value=0.00, step=0.01, format="%.2f")

        reference = st.text_input("Reference (optional)", placeholder="e.g. invoice # / bank ref")

        submitted = st.form_submit_button("Add")

    # --- SUGGESTIONS (OUTSIDE FORM, in a NEW container) ---
    sugg_box = st.container()  # important: created outside form!
    with sugg_box:
        q = (st.session_state.get(payer_key) or "").strip().lower()
        if q:
            matches = [p for p in all_payers if q in p.lower()][:8]
            if matches:
                st.caption("Suggestions (click to use):")
                cols = st.columns(min(4, len(matches)))
                for i, p in enumerate(matches):
                    if cols[i % len(cols)].button(p, key=f"{payer_key}_sugg_{i}"):
                        st.session_state[payer_key] = p
                        st.rerun()

    # --- HANDLE SUBMIT (after the form) ---
    if submitted:
        if not vineyard:
            st.error("Select a vineyard.")
        elif not (st.session_state.get(payer_key) or "").strip():
            st.error("Payer is required.")
        elif float(amount) == 0:
            st.error("Amount must be non-zero.")
        else:
            insert_txn(
                conn,
                vineyard,
                st.session_state[payer_key],
                txn_date.isoformat(),
                kind,
                to_cents(float(amount)),
                reference,
            )
            st.success("Added.")
            st.rerun()

with tab2:
    with st.form("add_invoice", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 1, 2])

        vineyard_list = get_vineyards(conn)
        with c1:
            inv_vineyard = st.selectbox("Vineyard *", options=vineyard_list, index=None, placeholder="Select vineyard")
        with c2:
            inv_date = st.date_input("Invoice date *", value=date.today())
        with c3:
            inv_ref = st.text_input("Invoice reference (optional)", placeholder="e.g. Jan 2026 storage / invoice #")

        inv_amount = st.number_input(
            "Invoice amount (deduct or credit) *", value=0.00, step=0.01, format="%.2f"
        )

        submitted_inv = st.form_submit_button("Add invoice deduction")
        if submitted_inv:
            if not inv_vineyard:
                st.error("Select a vineyard.")
            elif inv_amount == 0:
                st.error("Amount must be non-zero.")
            else:
                insert_txn(
                    conn,
                    inv_vineyard,
                    "(VINEYARD INVOICE)",
                    inv_date.isoformat(),
                    "INVOICE",
                    to_cents(float(inv_amount)),
                    inv_ref,
                )
                st.success("Invoice deduction added.")
                st.rerun()

# Refresh df after changes/imports
df = fetch_all(conn)
df = add_running_balance(df)

# -------------------------
# Balances
# -------------------------
st.markdown("---")
st.subheader("Balances per vineyard")

bal_df = balances_by_vineyard(df)
if bal_df.empty:
    st.info("No entries yet.")
else:
    view_bal = bal_df.copy()
    view_bal["balance_num"] = view_bal["balance"].astype(float)

    # Total credit = sum of positive balances
    total_credit = float(view_bal.loc[view_bal["balance_num"] > 0, "balance_num"].sum())

    # (Optional) Total owed = sum of negative balances
    total_owed = float(view_bal.loc[view_bal["balance_num"] < 0, "balance_num"].sum())

    # Show formatted table
    view_bal["balance"] = view_bal["balance_num"].apply(lambda x: format_money(x, currency))
    view_bal = view_bal.drop(columns=["balance_num"])
    st.dataframe(view_bal, use_container_width=True, hide_index=True)

    # Show totals
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Total vineyard credit", format_money(total_credit, currency))
    with c2:
        st.metric("Total vineyard owed (negative balances)", format_money(total_owed, currency))


    # Simple balances export
    xlsx_bytes = df_to_excel_bytes(bal_df, sheet_name="Balances")
    st.download_button(
        "Download balances (Excel)",
        data=xlsx_bytes,
        file_name="balances_by_vineyard.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Accountant workbook export (tabs per vineyard)
    st.markdown("### Full overview")
    wb = vineyards_workbook_bytes(df, currency_label=currency)
    st.download_button(
        "Download full overview (Excel)",
        data=wb,
        file_name="vineyard_statements_by_vineyard.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -------------------------
# Entries overview + Excel export (with Balance after)
# -------------------------
st.markdown("---")
st.subheader("Entries")

if df.empty:
    st.info("No entries to show.")
else:
    view_df = df.copy()

    if selected_vineyard != "(All)":
        view_df = view_df[view_df["vineyard"] == selected_vineyard]
    if selected_payer != "(All)":
        view_df = view_df[view_df["payer"] == selected_payer]

    friendly = view_df[["id", "vineyard", "payer", "date", "type", "amount", "balance_after", "reference"]].copy()
    friendly["date"] = friendly["date"].apply(format_date_eu)

    def pretty_amount(row):
        amt = float(row["amount"])
        if row["type"] == "Payment received":
            return format_money(amt, currency)
        if row["type"] == "Correction":
            return format_money(amt, currency)  # already signed
        return "-" + format_money(amt, currency).replace(f"{currency} ", f"{currency} ")

    friendly["amount"] = friendly.apply(pretty_amount, axis=1)
    friendly["balance_after"] = friendly["balance_after"].apply(
        lambda x: format_money(float(x), currency) if pd.notna(x) else ""
    )
    friendly = friendly.rename(columns={"balance_after": "balance after"})

    st.dataframe(friendly, use_container_width=True, hide_index=True)

    # reference moved right behind payer, EU date for export
    export_cols = ["id", "vineyard", "payer", "reference", "txn_date", "kind", "signed_amount", "balance_after"]
    export_df = (
        view_df[export_cols].copy().sort_values(["vineyard", "txn_date", "id"], ascending=[True, True, True])
    )
    export_df["txn_date"] = export_df["txn_date"].apply(format_date_eu)
    export_df = export_df.rename(columns={
        "signed_amount": f"signed amount ({currency})",
        "balance_after": f"balance after ({currency})"
    })

    xlsx_bytes = df_to_excel_bytes(export_df, sheet_name="Entries")
    from datetime import date

    today = date.today().isoformat()

    if selected_vineyard == "(All)":
        fname = f"entries_all_{today}.xlsx"
    else:
        safe_vineyard = selected_vineyard.replace(" ", "_")
        fname = f"entries_{safe_vineyard}_{today}.xlsx"

    st.download_button(
        "Download shown entries (Excel)",
        data=xlsx_bytes,
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -------------------------
# Edit/delete
# -------------------------
st.markdown("---")
st.subheader("Edit or delete an entry")

if df.empty:
    st.write("—")
else:
    txn_id = st.selectbox("Select entry ID", df["id"].tolist())
    row = df[df["id"] == txn_id].iloc[0]

    with st.form("edit_form"):
        c1, c2, c3, c4 = st.columns([2, 2, 1, 2])

        vineyard_list = get_vineyards(conn)
        with c1:
            try:
                idx_v = vineyard_list.index(row["vineyard"])
            except ValueError:
                idx_v = None
            e_vineyard = st.selectbox(
                "Vineyard",
                options=vineyard_list,
                index=idx_v if idx_v is not None else None,
                placeholder="Select vineyard",
            )
        with c2:
            e_payer = st.text_input("Payer", value=row["payer"])
        with c3:
            e_date = st.date_input("Date", value=pd.to_datetime(row["txn_date"]).date())
        with c4:
            type_options = ["Payment received", "Transfer to vineyard", "Monthly invoice (deduction)", "Correction"]
            idx = type_options.index(row["type"]) if row["type"] in type_options else 0
            e_type_label = st.selectbox("Type", type_options, index=idx)

        kind_map_back = {
            "Payment received": "PAYMENT",
            "Transfer to vineyard": "TRANSFER",
            "Monthly invoice (deduction)": "INVOICE",
            "Correction": "CORRECTION",
        }
        e_kind = kind_map_back[e_type_label]

        if e_kind == "CORRECTION":
            e_amount = st.number_input("Amount (can be negative)", value=float(row["amount"]), step=0.01, format="%.2f")
        else:
            e_amount = st.number_input(
                "Amount", min_value=0.00, value=float(row["amount"]), step=0.01, format="%.2f"
            )

        e_ref = st.text_input("Reference", value=row["reference"])

        csave, cdel = st.columns([1, 1])
        with csave:
            save_btn = st.form_submit_button("Save changes")
        with cdel:
            delete_btn = st.form_submit_button("Delete entry")

        if save_btn:
            if not e_vineyard:
                st.error("Select a vineyard.")
            elif not e_payer.strip():
                st.error("Payer is required.")
            elif float(e_amount) == 0:
                st.error("Amount must be non-zero.")
            else:
                update_txn(
                    conn,
                    int(txn_id),
                    e_vineyard,
                    e_payer,
                    e_date.isoformat(),
                    e_kind,
                    to_cents(float(e_amount)),
                    e_ref,
                )
                st.success("Updated.")
                st.rerun()

        if delete_btn:
            delete_txn(conn, int(txn_id))
            st.success("Deleted.")
            st.rerun()

# -------------------------
# Import section (bottom, collapsible)
# -------------------------
st.markdown("---")
with st.expander("⚙️ Import from Excel (admin / occasional)", expanded=False):
    st.subheader("Import from Excel")

    imp_tab1, imp_tab2 = st.tabs(["Import opening balances (recommended)", "Import transactions history (optional)"])

    with imp_tab1:
        st.caption(
            "Use this to switch from your current Excel balances. "
            "Excel columns required: vineyard, balance. "
            "Positive balance = credit; negative = owes (deduction)."
        )
        bal_file = st.file_uploader("Upload opening balances (.xlsx)", type=["xlsx"], key="opening_balances_upload")

        if bal_file:
            imp = pd.read_excel(bal_file)
            imp.columns = [c.strip().lower() for c in imp.columns]

            if not {"vineyard", "balance"}.issubset(set(imp.columns)):
                st.error("Your Excel must contain columns: vineyard, balance")
            else:
                imp["vineyard"] = imp["vineyard"].astype(str).str.strip()
                imp["balance"] = pd.to_numeric(imp["balance"], errors="coerce")

                bad = imp["balance"].isna().sum()
                if bad:
                    st.error(f"{bad} rows have an invalid balance amount. Fix and re-upload.")
                else:
                    st.write("Preview:")
                    st.dataframe(imp, use_container_width=True, hide_index=True)

                    total = float(imp["balance"].sum())
                    st.info(f"Net total of balances (for reference): {format_money(total, currency)}")

                    if st.button("Import opening balances now"):
                        inserted = 0
                        for _, r in imp.iterrows():
                            v = r["vineyard"].strip()
                            bal = float(r["balance"])
                            if not v or bal == 0:
                                continue

                            # Opening balances are CORRECTION entries (signed)
                            kind = "CORRECTION"
                            amount_cents = to_cents(float(bal))  # can be + or -

                            txn_date = date.today().isoformat()
                            payer = "(OPENING BALANCE)"
                            reference = "Opening balance"

                            row_hash = compute_row_hash(v, payer, txn_date, kind, amount_cents, reference)

                            if has_column(conn, "transactions", "import_hash"):
                                cur = conn.execute(
                                    "SELECT 1 FROM transactions WHERE import_hash=? LIMIT 1", (row_hash,)
                                )
                                if cur.fetchone():
                                    continue

                            insert_txn(conn, v, payer, txn_date, kind, amount_cents, reference, import_hash=row_hash)
                            inserted += 1

                        st.success(f"Imported opening balances ({inserted} entries created).")
                        st.rerun()

    with imp_tab2:
        st.caption(
            "Import historical lines (if you want full statements). "
            "Required columns: vineyard, payer, date, type, amount. Optional: reference. "
            "Type must be PAYMENT / TRANSFER / INVOICE / CORRECTION. Amount for CORRECTION can be negative."
        )
        tx_file = st.file_uploader("Upload transactions history (.xlsx)", type=["xlsx"], key="transactions_upload")

        if tx_file:
            imp = pd.read_excel(tx_file)
            imp.columns = [c.strip().lower() for c in imp.columns]

            required = {"vineyard", "payer", "date", "type", "amount"}
            missing = required - set(imp.columns)
            if missing:
                st.error(f"Missing required columns: {', '.join(sorted(missing))}")
            else:
                if "reference" not in imp.columns:
                    imp["reference"] = ""

                imp["vineyard"] = imp["vineyard"].astype(str).str.strip()
                imp["payer"] = imp["payer"].astype(str).str.strip()
                imp["type"] = imp["type"].astype(str).str.strip().str.upper()
                imp["amount"] = pd.to_numeric(imp["amount"], errors="coerce")

                parsed_dates = pd.to_datetime(imp["date"], errors="coerce")
                imp["date"] = parsed_dates.dt.date.astype(str)

                valid_types = {"PAYMENT", "TRANSFER", "INVOICE", "CORRECTION"}
                bad_dates = parsed_dates.isna().sum()
                bad_amounts = imp["amount"].isna().sum()
                bad_types = (~imp["type"].isin(valid_types)).sum()

                missing_v = (imp["vineyard"].str.len() == 0).sum()
                missing_p = (imp["payer"].str.len() == 0).sum()

                # For PAYMENT/TRANSFER/INVOICE amount must be > 0; for CORRECTION it must be != 0
                non_positive_strict = ((imp["type"].isin(["PAYMENT", "TRANSFER", "INVOICE"])) & (imp["amount"] <= 0)).sum()
                zero_corrections = ((imp["type"] == "CORRECTION") & (imp["amount"] == 0)).sum()

                issues = []
                if missing_v:
                    issues.append(f"{missing_v} rows have empty vineyard")
                if missing_p:
                    issues.append(f"{missing_p} rows have empty payer")
                if bad_dates:
                    issues.append(f"{bad_dates} rows have invalid date")
                if bad_amounts:
                    issues.append(f"{bad_amounts} rows have invalid amount")
                if non_positive_strict:
                    issues.append(f"{non_positive_strict} rows have amount <= 0 for PAYMENT/TRANSFER/INVOICE (must be positive)")
                if zero_corrections:
                    issues.append(f"{zero_corrections} rows have CORRECTION amount = 0 (must be non-zero)")
                if bad_types:
                    issues.append(f"{bad_types} rows have invalid type (use PAYMENT/TRANSFER/INVOICE/CORRECTION)")

                if issues:
                    st.error("Fix these issues in Excel before importing:\n- " + "\n- ".join(issues))
                else:
                    prev = imp.copy()
                    prev["signed_amount"] = prev.apply(
                        lambda r: r["amount"]
                        if r["type"] in ("PAYMENT", "CORRECTION")
                        else -r["amount"],
                        axis=1,
                    )
                    st.write("Preview (first 50 rows):")
                    st.dataframe(prev.head(50), use_container_width=True, hide_index=True)

                    st.info(
                        f"Import total impact (payments + corrections - transfers - invoices): "
                        f"{format_money(float(prev['signed_amount'].sum()), currency)}"
                    )

                    skip_dupes = st.checkbox("Skip duplicates (recommended)", value=True)
                    if st.button("Import transactions now"):
                        inserted, skipped = import_transactions_from_df(conn, imp, skip_duplicates=skip_dupes)
                        st.success(f"Imported {inserted} rows. Skipped {skipped} duplicates.")
                        st.rerun()

