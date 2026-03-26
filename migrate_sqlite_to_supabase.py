"""
Secure SQLite → Supabase migration script
-----------------------------------------

✅ Uses environment variables ONLY
✅ NEVER hard‑codes Supabase keys
✅ Only uses the service role key locally during migration
✅ Safe duplicate detection using import_hash
✅ Will NOT run unless all env vars are available

Run manually:
    export SUPABASE_URL="..."
    export SUPABASE_SERVICE_ROLE_KEY="..."
    python migrate_sqlite_to_supabase.py

This file should NOT be deployed, NOT pushed to Azure,
and preferably kept out of GitHub.
"""

import os
import sqlite3
import pandas as pd
from supabase import create_client
import hashlib


# -----------------------------------------
# Config
# -----------------------------------------
SQLITE_PATH = "payments.db"  # your local SQLite DB file


# -----------------------------------------
# Load environment variables safely
# -----------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError(
        "❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY. "
        "Set them before running this script."
    )

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# -----------------------------------------
# Helpers
# -----------------------------------------
def compute_row_hash(vineyard_name, payer, txn_date, kind, amount_cents, reference):
    """Create deterministic hash so we can avoid duplicates."""
    v = (vineyard_name or "").strip()
    p = (payer or "").strip()
    k = (kind or "").strip().upper()
    d = str(pd.to_datetime(txn_date).date())
    ref = (reference or "").strip()

    raw = f"{v}|{p}|{d}|{int(amount_cents)}|{ref}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def get_or_create_vineyard_id(name: str) -> str:
    """Return Supabase vineyard.id, creating it if needed."""
    name = name.strip()

    res = (
        supabase.table("vineyards")
        .select("id")
        .eq("name", name)
        .limit(1)
        .execute()
    )

    if res.data:
        return res.data[0]["id"]

    # create
    supabase.table("vineyards").insert({"name": name}).execute()

    # fetch again
    res2 = (
        supabase.table("vineyards")
        .select("id")
        .eq("name", name)
        .limit(1)
        .execute()
    )
    return res2.data[0]["id"]


# -----------------------------------------
# Main Migration
# -----------------------------------------
def main():
    print("🔄 Starting SQLite → Supabase migration...")
    if not os.path.exists(SQLITE_PATH):
        raise FileNotFoundError(f"SQLite DB not found: {SQLITE_PATH}")

    conn = sqlite3.connect(SQLITE_PATH)

    # -------------------------
    # 1. Migrate vineyards
    # -------------------------
    df_vine = pd.read_sql_query("SELECT DISTINCT name FROM vineyards", conn)
    print(f"📌 Found {len(df_vine)} vineyards...")

    for _, row in df_vine.iterrows():
        name = str(row["name"]).strip()
        try:
            get_or_create_vineyard_id(name)
        except Exception as e:
            print(f"⚠️ Could not create/fetch vineyard '{name}': {e}")

    # Map vineyard names → ids
    res = supabase.table("vineyards").select("id,name").execute()
    name_to_id = {r["name"]: r["id"] for r in (res.data or [])}

    # -------------------------
    # 2. Migrate transactions
    # -------------------------
    df_tx = pd.read_sql_query(
        """
        SELECT id, vineyard, payer, txn_date, kind, amount_cents, reference, created_at
        FROM transactions
        ORDER BY txn_date, id
        """,
        conn,
    )
    print(f"📌 Found {len(df_tx)} transactions to import...")

    inserted = 0
    skipped = 0

    for _, r in df_tx.iterrows():
        v = str(r["vineyard"]).strip()
        p = str(r["payer"]).strip()
        d = str(r["txn_date"])
        k = str(r["kind"]).strip().upper()
        amt = int(r["amount_cents"])
        ref = None if pd.isna(r.get("reference")) else str(r["reference"]).strip()

        vid = name_to_id.get(v) or get_or_create_vineyard_id(v)
        row_hash = compute_row_hash(v, p, d, k, amt, ref)

        payload = {
            "vineyard_id": vid,
            "payer": p,
            "txn_date": d,
            "kind": k,
            "amount_cents": amt,
            "reference": ref,
            "import_hash": row_hash,
        }

        try:
            supabase.table("transactions").insert(payload).execute()
            inserted += 1
        except Exception:
            # usually duplicate import_hash
            skipped += 1

    # -------------------------
    # 3. Summary
    # -------------------------
    print(f"\n✅ Done.")
    print(f"   Inserted: {inserted}")
    print(f"   Skipped (duplicates/errors): {skipped}")


if __name__ == "__main__":
    main()