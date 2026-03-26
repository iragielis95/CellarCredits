import io
import os
import hashlib
from datetime import date
from functools import lru_cache

import pandas as pd
import streamlit as st
from openpyxl.utils import get_column_letter
from supabase import create_client, Client

# -----------------------------------------------------------------------------
# Streamlit Config
# -----------------------------------------------------------------------------
st.set_page_config(page_title="WLI Cellar Credits", layout="wide")
st.title("WLI Cellar Credits")
st.caption(
    "Track payments received per vineyard, transfers to vineyards, "
    "monthly invoice deductions, and corrections."
)

DEBUG = st.sidebar.checkbox("Debug mode", value=False)

# -----------------------------------------------------------------------------
# Config / Supabase
# -----------------------------------------------------------------------------
def _get_secret(path: list[str], default=None):
    """
    Safely read nested Streamlit secrets, e.g. ["supabase", "url"].
    """
    current = st.secrets
    try:
        for key in path:
            current = current[key]
        return current
    except Exception:
        return default


@lru_cache(maxsize=1)
def get_supabase() -> Client:
    """
    Supports both:
    - Azure / generic hosting via environment variables
    - Streamlit Cloud / local via st.secrets
    """
    url = (
        os.environ.get("SUPABASE_URL")
        or _get_secret(["supabase", "url"])
    )

    key = (
        os.environ.get("SUPABASE_PUBLISHABLE_KEY")
        or os.environ.get("SUPABASE_ANON_KEY")
        or os.environ.get("SUPABASE_KEY")
        or _get_secret(["supabase", "publishable_key"])
        or _get_secret(["supabase", "anon_key"])
        or _get_secret(["supabase", "key"])
    )

    if not url:
        raise ValueError("Missing Supabase URL. Set SUPABASE_URL or st.secrets['supabase']['url'].")
    if not key:
        raise ValueError(
            "Missing Supabase key. Set SUPABASE_PUBLISHABLE_KEY / SUPABASE_ANON_KEY / SUPABASE_KEY "
            "or the equivalent in st.secrets."
        )

    return create_client(url, key)


try:
    supabase = get_supabase()
    # Small health check
    supabase.table("vineyards").select("id").limit(1).execute()
    if DEBUG:
        st.success("✅ Connected to Supabase")
except Exception as e:
    st.error(f"Supabase setup/connection failed: {e}")
    st.stop()

# -----------------------------------------------------------------------------
# Helpers (presentation/Excel)
# -----------------------------------------------------------------------------
def format_date_eu(d) -> str:
    dt = pd.to_datetime(d, errors="coerce")
    if pd.isna(dt):
        return str(d)
    return dt.strftime("%d-%m-%Y")


def autosize_worksheet(ws, max_width: int = 60, min_width: int = 10):
    for col_cells in ws.columns:
        col_letter = get_column_letter(col_cells[0].column)
        max_len = 0
        for cell in col_cells:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max(min_width, min(max_width, max_len + 2))


def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.book[sheet_name]
        ws.freeze_panes = "A2"
        autosize_worksheet(ws)
    return output.getvalue()


def format_money(x: float, currency="EUR") -> str:
    return f"{currency} {x:,.2f}"


def sanitize_excel_sheet_name(name: str) -> str:
    cleaned = "".join(ch for ch in str(name) if ch not in r'[]:*?/\\')
    cleaned = cleaned.strip()[:31]
    return cleaned or "Sheet"


# -----------------------------------------------------------------------------
# Domain helpers
# -----------------------------------------------------------------------------
KIND_LABELS = {
    "PAYMENT": "Payment received",
    "TRANSFER": "Transfer to vineyard",
    "INVOICE": "Monthly invoice (deduction)",
    "CORRECTION": "Correction",
}
LABEL_TO_KIND = {v: k for k, v in KIND_LABELS.items()}
KIND_ORDER = ["PAYMENT", "TRANSFER", "INVOICE", "CORRECTION"]
CURRENCIES = ["EUR", "GBP", "USD"]


def to_cents(amount: float) -> int:
    return int(round(float(amount) * 100))


def from_cents(cents: int) -> float:
    return int(cents) / 100.0


def signed_amount(kind: str, amount_cents: int) -> float:
    """
    Signed value according to your business rules.
    """
    k = (kind or "").upper()
    amt = from_cents(amount_cents)

    if k == "PAYMENT":
        return abs(amt)
    if k in ("TRANSFER", "INVOICE"):
        return -abs(amt)
    if k == "CORRECTION":
        return amt
    return 0.0


def compute_row_hash(
    vineyard_name: str,
    payer: str,
    txn_date: str,
    kind: str,
    amount_cents: int,
    reference: str | None
) -> str:
    v = (vineyard_name or "").strip()
    p = (payer or "").strip()
    k = (kind or "").strip().upper()
    dt = pd.to_datetime(txn_date, errors="coerce")
    d = dt.date().isoformat() if pd.notna(dt) else str(txn_date).strip()
    ref = (reference or "").strip()
    raw = f"{v}|{p}|{d}|{k}|{int(amount_cents)}|{ref}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

# -----------------------------------------------------------------------------
# Supabase data helpers
# -----------------------------------------------------------------------------
def fetch_customers_df() -> pd.DataFrame:
    try:
        res = supabase.table("customers").select("*").order("created_at", desc=False).execute()
        return pd.DataFrame(res.data or [])
    except Exception as e:
        st.error(f"Failed to fetch customers: {e}")
        return pd.DataFrame()


def insert_customer(name: str, credits: int):
    return supabase.table("customers").insert(
        {"name": name.strip(), "credits": int(credits)}
    ).execute()


def update_customer_credits(cust_id: str, credits: int):
    return supabase.table("customers").update(
        {"credits": int(credits)}
    ).eq("id", cust_id).execute()


def delete_customer_by_id(cust_id: str):
    return supabase.table("customers").delete().eq("id", cust_id).execute()


def fetch_vineyards_df() -> pd.DataFrame:
    try:
        res = supabase.table("vineyards").select("*").order("name").execute()
        return pd.DataFrame(res.data or [])
    except Exception as e:
        st.error(f"Failed to fetch vineyards: {e}")
        return pd.DataFrame()


def get_or_create_vineyard_id(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise ValueError("Empty vineyard name")

    res = supabase.table("vineyards").select("id").eq("name", name).limit(1).execute()
    if res.data:
        return res.data[0]["id"]

    supabase.table("vineyards").insert({"name": name}).execute()

    res2 = supabase.table("vineyards").select("id").eq("name", name).limit(1).execute()
    if not res2.data:
        raise RuntimeError("Failed to create vineyard")
    return res2.data[0]["id"]


def delete_vineyard_by_id(vineyard_id: str):
    tx = supabase.table("transactions").select("id").eq("vineyard_id", vineyard_id).limit(1).execute()
    if tx.data:
        raise ValueError("Cannot delete: vineyard has transactions.")
    return supabase.table("vineyards").delete().eq("id", vineyard_id).execute()


def fetch_transactions_df(vineyard_id: str | None = None, payer: str | None = None) -> pd.DataFrame:
    try:
        q = supabase.table("transactions").select("*")
        if vineyard_id:
            q = q.eq("vineyard_id", vineyard_id)
        if payer and payer != "(All)":
            q = q.eq("payer", payer)

        q = q.order("txn_date", desc=False).order("created_at", desc=False)
        res = q.execute()
        return pd.DataFrame(res.data or [])
    except Exception as e:
        st.error(f"Failed to fetch transactions: {e}")
        return pd.DataFrame()


def insert_transaction(
    vineyard_id: str,
    payer: str,
    txn_date,
    kind: str,
    amount_cents: int,
    reference: str | None,
    import_hash: str | None = None,
):
    payload = {
        "vineyard_id": vineyard_id,
        "payer": payer.strip(),
        "txn_date": str(txn_date),
        "kind": kind.strip().upper(),
        "amount_cents": int(amount_cents),
        "reference": (reference or "").strip() or None,
    }
    if import_hash:
        payload["import_hash"] = import_hash

    return supabase.table("transactions").insert(payload).execute()


def update_transaction(
    txn_id: str,
    vineyard_id: str,
    payer: str,
    txn_date,
    kind: str,
    amount_cents: int,
    reference: str | None,
):
    payload = {
        "vineyard_id": vineyard_id,
        "payer": payer.strip(),
        "txn_date": str(txn_date),
        "kind": kind.strip().upper(),
        "amount_cents": int(amount_cents),
        "reference": (reference or "").strip() or None,
    }
    return supabase.table("transactions").update(payload).eq("id", txn_id).execute()


def delete_transaction_by_id(txn_id: str):
    return supabase.table("transactions").delete().eq("id", txn_id).execute()

# -----------------------------------------------------------------------------
# Derivations
# -----------------------------------------------------------------------------
def add_running_balance(df_tx: pd.DataFrame, df_v: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
    - vineyard name
    - date
    - amount (unsigned except correction, which keeps sign)
    - type
    - signed_amount
    - balance_after
    """
    if df_tx.empty:
        return df_tx.copy()

    out = df_tx.copy()

    vmap = dict(zip(df_v["id"], df_v["name"])) if not df_v.empty and {"id", "name"}.issubset(df_v.columns) else {}
    out["vineyard"] = out["vineyard_id"].map(vmap).fillna("(Unknown vineyard)")
    out["date"] = pd.to_datetime(out["txn_date"], errors="coerce").dt.date

    def ui_amount(row):
        kind = row["kind"]
        cents = int(row["amount_cents"])
        if kind == "CORRECTION":
            return from_cents(cents)
        return from_cents(abs(cents))

    out["amount"] = out.apply(ui_amount, axis=1)
    out["type"] = out["kind"].map(KIND_LABELS).fillna(out["kind"])
    out["signed_amount"] = out.apply(lambda r: signed_amount(r["kind"], r["amount_cents"]), axis=1)

    work = out.copy()
    work["_dt"] = pd.to_datetime(work["txn_date"], errors="coerce")
    sort_cols = ["vineyard", "_dt"]
    if "created_at" in work.columns:
        sort_cols.append("created_at")
    work = work.sort_values(sort_cols, ascending=True)
    work["balance_after"] = work.groupby("vineyard")["signed_amount"].cumsum()

    out = out.merge(work[["id", "balance_after"]], on="id", how="left")
    return out


def balances_by_vineyard(df_aug: pd.DataFrame) -> pd.DataFrame:
    if df_aug.empty:
        return pd.DataFrame(columns=["vineyard", "balance"])

    return (
        df_aug.groupby("vineyard", as_index=False)["signed_amount"]
        .sum()
        .rename(columns={"signed_amount": "balance"})
        .sort_values("vineyard")
    )


def vineyards_workbook_bytes(df_all: pd.DataFrame, currency_label: str = "EUR") -> bytes:
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if df_all.empty:
            pd.DataFrame(columns=["vineyard", "balance"]).to_excel(writer, index=False, sheet_name="Summary")
            ws = writer.book["Summary"]
            ws.freeze_panes = "A2"
            autosize_worksheet(ws)
            return output.getvalue()

        summary = balances_by_vineyard(df_all).copy()
        summary = summary.rename(columns={"balance": f"balance ({currency_label})"})
        summary.to_excel(writer, index=False, sheet_name="Summary")
        ws_sum = writer.book["Summary"]
        ws_sum.freeze_panes = "A2"
        autosize_worksheet(ws_sum)

        for vineyard, vdf in df_all.groupby("vineyard"):
            cols = ["txn_date", "payer", "reference", "type", "amount", "signed_amount", "balance_after"]
            available_cols = [c for c in cols if c in vdf.columns]
            sheet_df = vdf.sort_values(
                [c for c in ["txn_date", "created_at"] if c in vdf.columns],
                ascending=True
            )[available_cols].copy()

            sheet_df = sheet_df.rename(
                columns={
                    "txn_date": "date",
                    "amount": f"amount ({currency_label})",
                    "signed_amount": f"signed amount ({currency_label})",
                    "balance_after": f"balance after ({currency_label})",
                }
            )

            if "date" in sheet_df.columns:
                sheet_df["date"] = sheet_df["date"].apply(format_date_eu)

            safe_name = sanitize_excel_sheet_name(vineyard)
            sheet_df.to_excel(writer, index=False, sheet_name=safe_name)
            ws = writer.book[safe_name]
            ws.freeze_panes = "A2"
            autosize_worksheet(ws)

    return output.getvalue()

# -----------------------------------------------------------------------------
# UI: Customers
# -----------------------------------------------------------------------------
st.markdown("## Customers")

df_customers = fetch_customers_df()

with st.form("add_customer_form"):
    c1, c2 = st.columns([3, 1])
    with c1:
        name = st.text_input("Customer name")
    with c2:
        credits = st.number_input("Credits", min_value=0, step=1, value=0)
    submitted = st.form_submit_button("Add customer")

if submitted:
    if not name.strip():
        st.error("Please enter a customer name.")
    else:
        try:
            insert_customer(name, credits)
            st.success(f"Customer '{name.strip()}' added.")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to add customer: {e}")

st.markdown("### Customer list")
if not df_customers.empty:
    show_cols = [c for c in ["name", "credits", "created_at"] if c in df_customers.columns]
    st.dataframe(df_customers[show_cols], use_container_width=True, hide_index=True)
else:
    st.info("No customers yet.")

st.markdown("### Update customer credits")
if not df_customers.empty and "id" in df_customers.columns:
    id_to_name = dict(zip(df_customers["id"], df_customers["name"]))
    selected_cust = st.selectbox(
        "Select customer",
        options=df_customers["id"].tolist(),
        format_func=lambda x: id_to_name.get(x, x),
        key="update_customer_select",
    )
    current_credits = int(df_customers.loc[df_customers["id"] == selected_cust, "credits"].iloc[0])
    new_credits = st.number_input("New credit amount", min_value=0, step=1, value=current_credits)

    if st.button("Update credits"):
        try:
            update_customer_credits(selected_cust, new_credits)
            st.success(f"Updated credits for {id_to_name[selected_cust]}")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to update customer: {e}")
else:
    st.info("No customers to update.")

st.markdown("### Delete customer")
if not df_customers.empty and "id" in df_customers.columns:
    id_to_name = dict(zip(df_customers["id"], df_customers["name"]))
    del_id = st.selectbox(
        "Select customer to delete",
        options=df_customers["id"].tolist(),
        format_func=lambda x: id_to_name.get(x, x),
        key="delete_customer_select",
    )

    if st.button("Delete customer"):
        try:
            delete_customer_by_id(del_id)
            st.warning(f"Deleted customer: {id_to_name[del_id]}")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to delete customer: {e}")
else:
    st.info("No customers to delete.")

total_credits = int(df_customers["credits"].sum()) if not df_customers.empty and "credits" in df_customers.columns else 0
st.metric("Total customer credits", total_credits)

# -----------------------------------------------------------------------------
# Sidebar: Settings, Filters, Vineyard manager
# -----------------------------------------------------------------------------
st.sidebar.header("Settings")
currency = st.sidebar.selectbox("Currency label", CURRENCIES, index=0)

df_vineyards = fetch_vineyards_df()
vineyard_names = df_vineyards["name"].tolist() if not df_vineyards.empty and "name" in df_vineyards.columns else []

df_tx_all = fetch_transactions_df(None, None)
all_payers = sorted(df_tx_all["payer"].dropna().unique().tolist()) if not df_tx_all.empty and "payer" in df_tx_all.columns else []

st.sidebar.markdown("---")
selected_vineyard_name = st.sidebar.selectbox("Filter by vineyard", ["(All)"] + vineyard_names)
selected_payer = st.sidebar.selectbox("Filter by payer", ["(All)"] + all_payers)

st.sidebar.markdown("---")
st.sidebar.subheader("Vineyards")

new_vineyard = st.sidebar.text_input("Add new vineyard", placeholder="e.g. Château Example")
if st.sidebar.button("Add vineyard"):
    if not new_vineyard.strip():
        st.sidebar.error("Enter a vineyard name.")
    else:
        try:
            get_or_create_vineyard_id(new_vineyard.strip())
            st.sidebar.success("Vineyard added.")
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Failed to add vineyard: {e}")

with st.sidebar.expander("Delete a vineyard (only if unused)"):
    if df_vineyards.empty or "id" not in df_vineyards.columns:
        st.sidebar.info("No vineyards yet.")
    else:
        id_to_name_v = dict(zip(df_vineyards["id"], df_vineyards["name"]))
        del_vid = st.sidebar.selectbox(
            "Select vineyard",
            options=df_vineyards["id"].tolist(),
            format_func=lambda x: id_to_name_v.get(x, x),
            key="delete_vineyard_select",
        )
        if st.sidebar.button("Delete selected vineyard"):
            try:
                delete_vineyard_by_id(del_vid)
                st.sidebar.success("Deleted from dropdown list.")
                st.rerun()
            except Exception as e:
                st.sidebar.error(str(e))

selected_vineyard_id = None
if selected_vineyard_name != "(All)" and not df_vineyards.empty:
    match = df_vineyards.loc[df_vineyards["name"] == selected_vineyard_name]
    if not match.empty:
        selected_vineyard_id = match["id"].iloc[0]

# -----------------------------------------------------------------------------
# Add Transactions
# -----------------------------------------------------------------------------
st.subheader("Add entry")
tab1, tab2 = st.tabs(["Payment / Transfer / Correction", "Monthly invoice (deduction)"])

with tab1:
    payer_key = "payer_add"

    with st.form("add_payment_transfer", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([2, 2, 1, 2])

        with c1:
            vineyard = st.selectbox(
                "Vineyard *",
                options=vineyard_names,
                index=None if vineyard_names else None,
                placeholder="Select vineyard" if vineyard_names else "Add a vineyard first",
            )
        with c2:
            payer = st.text_input(
                "Payer *",
                key=payer_key,
                placeholder="e.g. Restaurant XYZ / Importer ABC"
            )
        with c3:
            txn_date = st.date_input("Date *", value=date.today())
        with c4:
            kind_label = st.selectbox(
                "Type *",
                ["Payment received", "Transfer to vineyard", "Correction"]
            )
            kind = LABEL_TO_KIND[kind_label]

        if kind == "CORRECTION":
            amount = st.number_input("Amount * (can be negative)", value=0.00, step=0.01, format="%.2f")
        else:
            amount = st.number_input("Amount *", min_value=0.00, value=0.00, step=0.01, format="%.2f")

        reference = st.text_input("Reference (optional)", placeholder="e.g. invoice # / bank ref")
        submitted_entry = st.form_submit_button("Add")

    q = (st.session_state.get(payer_key) or "").strip().lower()
    if q and all_payers:
        matches = [p for p in all_payers if q in p.lower()][:8]
        if matches:
            st.caption("Suggestions (click to use):")
            cols = st.columns(min(4, len(matches)))
            for i, p in enumerate(matches):
                if cols[i % len(cols)].button(p, key=f"{payer_key}_sugg_{i}"):
                    st.session_state[payer_key] = p
                    st.rerun()

    if submitted_entry:
        if not vineyard:
            st.error("Select a vineyard.")
        elif not (st.session_state.get(payer_key) or "").strip():
            st.error("Payer is required.")
        elif float(amount) == 0:
            st.error("Amount must be non-zero.")
        else:
            try:
                vid = get_or_create_vineyard_id(vineyard)
                amt_cents = to_cents(float(amount))
                if kind != "CORRECTION":
                    amt_cents = abs(amt_cents)
                insert_transaction(vid, st.session_state[payer_key], txn_date, kind, amt_cents, reference)
                st.success("Added.")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to add entry: {e}")

with tab2:
    with st.form("add_invoice", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 1, 2])

        with c1:
            inv_vineyard = st.selectbox(
                "Vineyard *",
                options=vineyard_names,
                index=None if vineyard_names else None,
                placeholder="Select vineyard" if vineyard_names else "Add a vineyard first",
                key="invoice_vineyard_select",
            )
        with c2:
            inv_date = st.date_input("Invoice date *", value=date.today())
        with c3:
            inv_ref = st.text_input("Invoice reference (optional)", placeholder="e.g. Jan 2026 storage / invoice #")

        inv_amount = st.number_input("Invoice amount (deduct or credit) *", value=0.00, step=0.01, format="%.2f")
        submitted_inv = st.form_submit_button("Add invoice deduction")

    if submitted_inv:
        if not inv_vineyard:
            st.error("Select a vineyard.")
        elif float(inv_amount) == 0:
            st.error("Amount must be non-zero.")
        else:
            try:
                vid = get_or_create_vineyard_id(inv_vineyard)
                amt_cents = abs(to_cents(float(inv_amount)))
                insert_transaction(
                    vid,
                    "(VINEYARD INVOICE)",
                    inv_date,
                    "INVOICE",
                    amt_cents,
                    inv_ref
                )
                st.success("Invoice deduction added.")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to add invoice: {e}")

# -----------------------------------------------------------------------------
# Refresh / derive
# -----------------------------------------------------------------------------
df_vineyards = fetch_vineyards_df()
df_tx_filtered = fetch_transactions_df(
    selected_vineyard_id,
    None if selected_payer == "(All)" else selected_payer
)
df_aug = add_running_balance(df_tx_filtered, df_vineyards)

# -----------------------------------------------------------------------------
# Balances per vineyard
# -----------------------------------------------------------------------------
st.markdown("---")
st.subheader("Balances per vineyard")

bal_df = balances_by_vineyard(df_aug)
if bal_df.empty:
    st.info("No entries yet.")
else:
    view_bal = bal_df.copy()
    view_bal["balance_num"] = view_bal["balance"].astype(float)
    total_credit = float(view_bal.loc[view_bal["balance_num"] > 0, "balance_num"].sum())
    total_owed = float(view_bal.loc[view_bal["balance_num"] < 0, "balance_num"].sum())

    view_bal["balance"] = view_bal["balance_num"].apply(lambda x: format_money(x, currency))
    view_bal = view_bal.drop(columns=["balance_num"])

    st.dataframe(view_bal, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Total vineyard credit", format_money(total_credit, currency))
    with c2:
        st.metric("Total vineyard owed (negative balances)", format_money(total_owed, currency))

    xlsx_bytes = df_to_excel_bytes(bal_df, sheet_name="Balances")
    st.download_button(
        "Download balances (Excel)",
        data=xlsx_bytes,
        file_name="balances_by_vineyard.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.markdown("### Full overview")
    wb = vineyards_workbook_bytes(df_aug, currency_label=currency)
    st.download_button(
        "Download full overview (Excel)",
        data=wb,
        file_name="vineyard_statements_by_vineyard.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -----------------------------------------------------------------------------
# Entries overview + export
# -----------------------------------------------------------------------------
st.markdown("---")
st.subheader("Entries")

if df_aug.empty:
    st.info("No entries to show.")
else:
    view_df = df_aug.copy()

    if selected_vineyard_name != "(All)":
        view_df = view_df[view_df["vineyard"] == selected_vineyard_name]
    if selected_payer != "(All)":
        view_df = view_df[view_df["payer"] == selected_payer]

    friendly = view_df[["id", "vineyard", "payer", "date", "type", "amount", "balance_after", "reference"]].copy()
    friendly["date"] = friendly["date"].apply(format_date_eu)

    def pretty_amount(row):
        amt = float(row["amount"])
        if row["type"] in ("Payment received", "Correction"):
            return format_money(amt, currency)
        return format_money(-abs(amt), currency)

    friendly["amount"] = friendly.apply(pretty_amount, axis=1)
    friendly["balance_after"] = friendly["balance_after"].apply(
        lambda x: format_money(float(x), currency) if pd.notna(x) else ""
    )
    friendly = friendly.rename(columns={"balance_after": "balance after"})

    st.dataframe(friendly, use_container_width=True, hide_index=True)

    export_cols = ["id", "vineyard", "payer", "reference", "txn_date", "kind", "signed_amount", "balance_after"]
    export_df = view_df[export_cols].copy()

    sort_cols = [c for c in ["vineyard", "txn_date", "created_at"] if c in view_df.columns]
    export_df = export_df.sort_values(sort_cols, ascending=True)

    export_df["txn_date"] = export_df["txn_date"].apply(format_date_eu)
    export_df = export_df.rename(
        columns={
            "signed_amount": f"signed amount ({currency})",
            "balance_after": f"balance after ({currency})"
        }
    )

    today = date.today().isoformat()
    if selected_vineyard_name == "(All)":
        fname = f"entries_all_{today}.xlsx"
    else:
        safe_vineyard = selected_vineyard_name.replace(" ", "_")
        fname = f"entries_{safe_vineyard}_{today}.xlsx"

    xlsx_bytes = df_to_excel_bytes(export_df, sheet_name="Entries")
    st.download_button(
        "Download shown entries (Excel)",
        data=xlsx_bytes,
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -----------------------------------------------------------------------------
# Edit / delete
# -----------------------------------------------------------------------------
st.markdown("---")
st.subheader("Edit or delete an entry")

if df_aug.empty:
    st.write("—")
else:
    txn_id = st.selectbox("Select entry ID", df_aug["id"].tolist())
    row = df_aug[df_aug["id"] == txn_id].iloc[0]

    with st.form("edit_form"):
        try:
            idx_v = vineyard_names.index(row["vineyard"])
        except ValueError:
            idx_v = None

        e_vineyard = st.selectbox(
            "Vineyard",
            options=vineyard_names,
            index=idx_v if idx_v is not None else None,
            placeholder="Select vineyard" if vineyard_names else "Add a vineyard first",
        )
        e_payer = st.text_input("Payer", value=row["payer"])
        e_date = st.date_input("Date", value=pd.to_datetime(row["txn_date"]).date())

        type_options = list(KIND_LABELS.values())
        idx = type_options.index(row["type"]) if row["type"] in type_options else 0
        e_type_label = st.selectbox("Type", type_options, index=idx)
        e_kind = LABEL_TO_KIND[e_type_label]

        if e_kind == "CORRECTION":
            e_amount = st.number_input(
                "Amount (can be negative)",
                value=float(row["amount"]),
                step=0.01,
                format="%.2f"
            )
        else:
            e_amount = st.number_input(
                "Amount",
                min_value=0.00,
                value=float(abs(row["amount"])),
                step=0.01,
                format="%.2f"
            )

        e_ref = st.text_input("Reference", value=row.get("reference") or "")

        csave, cdel = st.columns(2)
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
            try:
                vid = get_or_create_vineyard_id(e_vineyard)
                amt_cents = to_cents(float(e_amount))
                if e_kind != "CORRECTION":
                    amt_cents = abs(amt_cents)
                update_transaction(txn_id, vid, e_payer, e_date, e_kind, amt_cents, e_ref)
                st.success("Updated.")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to update: {e}")

    if delete_btn:
        try:
            delete_transaction_by_id(txn_id)
            st.success("Deleted.")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to delete: {e}")

# -----------------------------------------------------------------------------
# Import section
# -----------------------------------------------------------------------------
st.markdown("---")
with st.expander("⚙️ Import from Excel (admin / occasional)", expanded=False):
    st.subheader("Import from Excel")
    imp_tab1, imp_tab2 = st.tabs([
        "Import opening balances (recommended)",
        "Import transactions history (optional)"
    ])

    # -------------------------------------------------------------------------
    # Opening balances
    # -------------------------------------------------------------------------
    with imp_tab1:
        st.caption(
            "Use this to switch from your current Excel balances. "
            "Required columns: vineyard, balance. "
            "Positive = credit; negative = owes (deduction)."
        )

        bal_file = st.file_uploader(
            "Upload opening balances (.xlsx)",
            type=["xlsx"],
            key="opening_balances_upload"
        )

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
                    st.info(f"Net total of balances: {format_money(total, currency)}")

                    if st.button("Import opening balances now"):
                        inserted = 0
                        skipped = 0

                        for _, r in imp.iterrows():
                            v = r["vineyard"].strip()
                            bal = float(r["balance"])

                            if not v or bal == 0:
                                skipped += 1
                                continue

                            kind = "CORRECTION"
                            amount_cents = to_cents(bal)
                            txn_date = date.today().isoformat()
                            payer = "(OPENING BALANCE)"
                            reference = "Opening balance"
                            row_hash = compute_row_hash(v, payer, txn_date, kind, amount_cents, reference)

                            try:
                                vid = get_or_create_vineyard_id(v)
                                insert_transaction(vid, payer, txn_date, kind, amount_cents, reference, import_hash=row_hash)
                                inserted += 1
                            except Exception:
                                skipped += 1

                        st.success(f"Imported opening balances: {inserted} inserted, {skipped} skipped.")
                        st.rerun()

    # -------------------------------------------------------------------------
    # Transactions history
    # -------------------------------------------------------------------------
    with imp_tab2:
        st.caption(
            "Import historical lines if you want full statements. "
            "Required columns: vineyard, payer, date, type, amount. "
            "Optional: reference. "
            "Type must be PAYMENT / TRANSFER / INVOICE / CORRECTION."
        )

        tx_file = st.file_uploader(
            "Upload transactions history (.xlsx)",
            type=["xlsx"],
            key="transactions_upload"
        )

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

                non_positive_strict = (
                    (imp["type"].isin(["PAYMENT", "TRANSFER", "INVOICE"])) & (imp["amount"] <= 0)
                ).sum()
                zero_corrections = (
                    (imp["type"] == "CORRECTION") & (imp["amount"] == 0)
                ).sum()

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
                    issues.append(f"{non_positive_strict} rows have amount <= 0 for PAYMENT/TRANSFER/INVOICE")
                if zero_corrections:
                    issues.append(f"{zero_corrections} rows have CORRECTION amount = 0")
                if bad_types:
                    issues.append(f"{bad_types} rows have invalid type")

                if issues:
                    st.error("Fix these issues in Excel before importing:\n- " + "\n- ".join(issues))
                else:
                    prev = imp.copy()
                    prev["signed_amount"] = prev.apply(
                        lambda r: r["amount"] if r["type"] in ("PAYMENT", "CORRECTION") else -r["amount"],
                        axis=1
                    )

                    st.write("Preview (first 50 rows):")
                    st.dataframe(prev.head(50), use_container_width=True, hide_index=True)

                    st.info(
                        "Import total impact: "
                        f"{format_money(float(prev['signed_amount'].sum()), currency)}"
                    )

                    skip_dupes = st.checkbox("Skip duplicates (recommended)", value=True)

                    if st.button("Import transactions now"):
                        inserted = 0
                        skipped = 0

                        for _, r in imp.iterrows():
                            v = r["vineyard"].strip()
                            p = r["payer"].strip()
                            d = r["date"]
                            k = r["type"].upper()
                            amt = float(r["amount"])
                            ref = "" if pd.isna(r.get("reference", "")) else str(r.get("reference", "")).strip()

                            amt_cents = to_cents(amt)
                            if k != "CORRECTION":
                                amt_cents = abs(amt_cents)

                            row_hash = compute_row_hash(v, p, d, k, amt_cents, ref)

                            try:
                                vid = get_or_create_vineyard_id(v)
                                if skip_dupes:
                                    insert_transaction(vid, p, d, k, amt_cents, ref, import_hash=row_hash)
                                else:
                                    insert_transaction(vid, p, d, k, amt_cents, ref, import_hash=None)
                                inserted += 1
                            except Exception:
                                skipped += 1

                        st.success(f"Imported {inserted} rows. Skipped {skipped}.")
                        st.rerun()