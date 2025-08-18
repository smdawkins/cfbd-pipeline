# CFBD Pipeline (Databricks)

## Quickstart
1. Create a Databricks **secret scope** `cfbd` and secret key `api_key`.
2. Import this repo as a **Databricks Repo**.
3. Open `notebooks/ingest_cfbd.py` and run with your params.
4. Open `notebooks/flatten_and_write.py`:
   - `mode=flatten_only` → wide table, arrays kept
   - `mode=explode_all`  → one row per leaf (e.g., team stats rows)
5. (Optional) Deploy the Job with **Asset Bundles** under `bundles/dev/bundle.yml`.

## Tables
- Bronze: `<catalog>.bronze.<endpoint>_raw`
- Silver: `<catalog>.silver.<endpoint>_flat` or `_flat_rows`
