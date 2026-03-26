import sqlite3
import pandas as pd
from supabase import create_client
import os

# === PAS DIT AAN ===
SQLITE_PATH = "payments.db"  # pad naar jouw lokale DB

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def get_or_create_vineyard_id(name: str) -> str:
    name = name.strip()
    res = supabase.table("vineyards").select("id").eq("name", name).limit(1).execute()
    if res.data:
        return res.data[0]["id"]
    supabase.table("vineyards").insert({"name": name}).execute()
    res2 = supabase.table("vineyards").select("id").eq("name", name).limit(1).execute()
    return res2.data[0]["id"]

def compute_row_hash(vineyard_name, payer, txn_date, kind, amount_cents, reference) -> str:
    import hashlib
    import pandas as pd
    v = (vineyard_name or "").strip()
    p = (payer or "").strip()
    k = (kind or "").strip().upper()
    dt = pd.to_datetime(txn_date, errors="coerce")
    d = dt.date().isoformat() if pd.notna(dt) else str(txn_date).strip()
    ref = (reference or "").strip()
    raw = f"{v}|{p}|{d}|{int(amount_cents)}|{ref}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def main():
    conn = sqlite3.connect(SQLITE_PATH)

    # 1) Vineyards inlezen en aanmaken
    df_v = pd.read_sql_query("SELECT DISTINCT name FROM vineyards", conn)
    for _, r in df_v.iterrows():
        try:
            get_or_create_vineyard_id(str(r["name"]))
        except Exception:
            pass

    # 2) Mapping naam -> id ophalen
    res = supabase.table("vineyards").select("id,name").execute()
    name_to_id = {r["name"]: r["id"] for r in (res.data or [])}

    # 3) Transactions uit SQLite lezen
    df_t = pd.read_sql_query("""
        SELECT id, vineyard, payer, txn_date, kind, amount_cents, reference, created_at
        FROM transactions
        ORDER BY txn_date, id
    """, conn)

    inserted = 0
    skipped = 0
    for _, r in df_t.iterrows():
        v = str(r["vineyard"]).strip()
        p = str(r["payer"]).strip()
        d = str(r["txn_date"])
        k = str(r["kind"]).strip().upper()
        amt = int(r["amount_cents"])
        ref = None if pd.isna(r.get("reference")) else str(r["reference"]).strip()

        vid = name_to_id.get(v) or get_or_create_vineyard_id(v)
        row_hash = compute_row_hash(v, p, d, k, amt, ref or "")

        try:
            supabase.table("transactions").insert({
                "vineyard_id": vid,
                "payer": p,
                "txn_date": d,
                "kind": k,
                "amount_cents": amt,
                "reference": ref,
                "import_hash": row_hash
            }).execute()
            inserted += 1
        except Exception:
            # Vaak duplicate door unique import_hash index
            skipped += 1
            continue

    print(f"Done. Inserted: {inserted}, skipped (duplicates or errors): {skipped}")

if __name__ == "__main__":
    main()