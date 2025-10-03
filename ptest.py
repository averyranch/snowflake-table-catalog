# snow_data_quality_analysis.py
# Run with: streamlit run snow_data_quality_analysis.py
"""
SNOW Data Quality Analysis (Refined UI)
- Professional SNOW look (header no longer clipped)
- Prominent, intuitive 🔎 Metadata Search (column/table/type) with lazy parallel indexing
- Per-thread Trino connections for safe parallelism
- Optional warm cache (button-triggered, autoscaled workers)
- Full-table Data Quality (nulls, distincts exact/approx, min/max)
- Guided Blast Radius Studio (metadata or data-driven, parallel)
- No information_schema; excludes system/jmx/galaxy
"""

import io
import os
import re
import time
import math
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
import streamlit as st
from trino.dbapi import connect
from trino.auth import BasicAuthentication

# =========================
# Credentials (use secrets in prod)
# =========================
HOST = "ai2025-free-cluster.trino.galaxy.starburst.io"
USER = "rahul.yaksh@bofa.com/accountadmin"
PASSWORD = "Aarush2011@"  # Prefer: st.secrets["TRINO_PASSWORD"]

APP_TITLE = "SNOW Data Quality Analysis"
PREVIEW_DEFAULT = 50
PREVIEW_MIN, PREVIEW_MAX, PREVIEW_STEP = 10, 2000, 10
MAX_GRAPH_TABLES = 200
DEFAULT_WORKERS = min(16, max(4, (os.cpu_count() or 8)))

# Streamlit cache shims
cache_data = getattr(st, "cache_data", st.cache)
cache_resource = getattr(st, "cache_resource", st.cache)

# -------------------------
# Catalog/schema filters
# -------------------------
CATALOG_DENYLIST = {"system", "jmx", "galaxy"}
SCHEMA_DENY_EXACT = {"information_schema"}
SCHEMA_DENY_PREFIXES = ("information_schema", "_information_schema")

def is_allowed_catalog(name: str) -> bool:
    return name and name.lower() not in CATALOG_DENYLIST

def is_allowed_schema(name: str) -> bool:
    lname = (name or "").lower()
    if lname in SCHEMA_DENY_EXACT:
        return False
    return not any(lname.startswith(p) for p in SCHEMA_DENY_PREFIXES)

# =========================
# Styles (header not clipped)
# =========================
def apply_styles() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide", page_icon="❄️")
    st.markdown(
        """
        <style>
          :root{
            --ink:#0e1529; --muted:#6b7280; --primary:#1d4ed8; --bg:#0b1020;
            --card:#0f172a; --card2:#111827; --border:#1f2937; --ok:#22c55e; --warn:#f59e0b; --bad:#ef4444;
          }
          html, body, .block-container { background: var(--bg) !important; color:#e5e7eb; }
          .block-container { padding-top: 1.0rem !important; padding-bottom: 1rem; } /* more top pad fixes cut header */
          section[data-testid="stSidebar"] { width: 380px !important; background: #0b1225; }
          .appbar {
            display:flex; align-items:center; gap:.8rem; padding:.8rem 1rem; border:1px solid var(--border);
            border-radius:16px; background:linear-gradient(180deg,#0f172a, #0c1427);
            overflow: visible; /* ensure not clipped */
          }
          .app-title { font-size:1.3rem; line-height:1.3; white-space:nowrap; }
          .badge {
            display:inline-block; padding:.25rem .5rem; border:1px solid var(--border);
            border-radius:999px; font-size:.8rem; color:#9ca3af; background:#0e162d;
          }
          .kpi-grid { display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:12px; }
          .card { border:1px solid var(--border); border-radius:14px; padding:12px; background:var(--card); }
          .card h4 { margin:0 0 .35rem 0; }
          .pill { padding:.2rem .5rem; border-radius:999px; border:1px solid var(--border); background:#0d1731; font-size:.75rem; }
          .muted { color:#94a3b8; }
          .chip-ok{ color:#22c55e; border-color:#22c55e33; }
          .chip-warn{ color:#f59e0b; border-color:#f59e0b33; }
          .chip-bad{ color:#ef4444; border-color:#ef444433; }
          .hr { height:1px; background:var(--border); margin:8px 0 12px 0;}
          .cta { padding:.4rem .7rem; border:1px solid #2b3b66; border-radius:10px; background:#132144; }
          .toolbar { display:flex; gap:.5rem; align-items:center; flex-wrap:wrap; }
          .section-title { font-size:1.1rem; margin:.25rem 0 .25rem 0; }
          .chips { display:flex; gap:.4rem; flex-wrap:wrap; }
          .chip { padding:.2rem .5rem; border:1px solid var(--border); border-radius:999px; background:#0f1730; color:#cbd5e1; font-size:.8rem; }
          .grid3 { display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap:12px; }
          .grid2 { display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:12px; }
          .wide-table .stDataFrame { border-radius:12px; overflow:hidden; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div class="appbar">
          <div class="app-title">❄️ <strong>{APP_TITLE}</strong></div>
          <span class="badge">Catalog → Schema → Tables</span>
          <span class="badge">Profiling</span>
          <span class="badge">Full-table Quality</span>
          <span class="badge">Blast Radius Studio</span>
          <span class="badge">Metadata Search</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

# =========================
# Data Access Layer
# =========================
@dataclass(frozen=True)
class ConnConfig:
    host: str = HOST
    user: str = USER
    password: str = PASSWORD
    http_scheme: str = "https"
    port: int = 443

_thread = threading.local()  # per-thread Trino connection

def get_connection(cfg: ConnConfig = ConnConfig()):
    conn = getattr(_thread, "conn", None)
    if conn is None:
        conn = connect(
            host=cfg.host, port=cfg.port, http_scheme=cfg.http_scheme,
            user=cfg.user, auth=BasicAuthentication(cfg.user, cfg.password),
        )
        _thread.conn = conn
    return conn

def run_sql(sql: str) -> Tuple[List[tuple], List[str]]:
    cur = get_connection().cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return rows, cols
    finally:
        try: cur.close()
        except Exception: pass

def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

def pick_first_usable_location(catalogs: List[str]) -> Tuple[Optional[str], Optional[str]]:
    for cat in catalogs:
        try:
            schemas = list_schemas(cat)
        except Exception:
            continue
        usable = [s for s in schemas if is_allowed_schema(s)]
        if usable:
            return cat, usable[0]
    return None, None

# =========================
# Metadata services (no information_schema)
# =========================
@cache_data(ttl=3600, show_spinner=False)
def list_catalogs() -> List[str]:
    rows, _ = run_sql("SHOW CATALOGS")
    return [r[0] for r in rows if is_allowed_catalog(r[0])]

@cache_data(ttl=3600, show_spinner=False)
def list_schemas(catalog: str) -> List[str]:
    rows, _ = run_sql(f"SHOW SCHEMAS FROM {qident(catalog)}")
    return [r[0] for r in rows if is_allowed_schema(r[0])]

@cache_data(ttl=3600, show_spinner=False)
def list_tables(catalog: str, schema: str) -> List[str]:
    rows, _ = run_sql(f"SHOW TABLES FROM {qident(catalog)}.{qident(schema)}")
    return [r[0] for r in rows]

@cache_data(ttl=1800, show_spinner=False)
def list_columns_for_table(catalog: str, schema: str, table: str) -> pd.DataFrame:
    rows, cols = run_sql(f"SHOW COLUMNS FROM {qident(catalog)}.{qident(schema)}.{qident(table)}")
    cols_norm = [str(c).strip().lower() for c in cols]
    df = pd.DataFrame(rows, columns=cols_norm)
    if df.empty:
        try:
            r2, c2 = run_sql(f"DESCRIBE {qident(catalog)}.{qident(schema)}.{qident(table)}")
            df = pd.DataFrame(r2, columns=[str(c).strip().lower() for c in c2])
        except Exception:
            pass
    if df.empty:
        return pd.DataFrame(columns=["table_name", "column_name", "data_type", "ordinal_position"])
    rename_map: Dict[str, str] = {}
    if "column" in df.columns: rename_map["column"] = "column_name"
    if "type" in df.columns:   rename_map["type"]   = "data_type"
    df = df.rename(columns=rename_map)
    if not {"column_name","data_type"}.issubset(df.columns):
        return pd.DataFrame(columns=["table_name", "column_name", "data_type", "ordinal_position"])
    df.insert(0, "table_name", table)
    df["ordinal_position"] = range(1, len(df) + 1)
    return df[["table_name", "column_name", "data_type", "ordinal_position"]]

@cache_data(ttl=3600, show_spinner=False)
def list_columns_for_schema(catalog: str, schema: str, workers: int) -> pd.DataFrame:
    tables = list_tables(catalog, schema)
    parts: List[pd.DataFrame] = []
    if not tables:
        return pd.DataFrame(columns=["table_name","column_name","data_type","ordinal_position"])
    max_workers = max(1, min(workers, len(tables)))  # autoscale to table count
    def one(t: str):
        try: return list_columns_for_table(catalog, schema, t)
        except Exception: return pd.DataFrame()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for df in ex.map(one, tables):
            if df is not None and not df.empty:
                parts.append(df)
    if not parts:
        return pd.DataFrame(columns=["table_name","column_name","data_type","ordinal_position"])
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(["table_name","ordinal_position"]).reset_index(drop=True)

@cache_data(ttl=900, show_spinner=False)
def preview_table(catalog: str, schema: str, table: str, limit: int = PREVIEW_DEFAULT) -> pd.DataFrame:
    q = f"SELECT * FROM {qident(catalog)}.{qident(schema)}.{qident(table)} LIMIT {int(limit)}"
    rows, cols = run_sql(q)
    return pd.DataFrame(rows, columns=cols)

# =========================
# SHOW STATS helpers
# =========================
@cache_data(ttl=3600, show_spinner=False)
def show_stats(catalog: str, schema: str, table: str) -> Optional[pd.DataFrame]:
    try:
        q = f"SHOW STATS FOR {qident(catalog)}.{qident(schema)}.{qident(table)}"
        rows, cols = run_sql(q)
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return None

def _extract_row_count(stats_df: Optional[pd.DataFrame]) -> Optional[int]:
    if stats_df is None or stats_df.empty:
        return None
    if "row_count" in stats_df.columns:
        s = pd.to_numeric(stats_df["row_count"], errors="coerce").dropna()
        if not s.empty:
            return int(s.iloc[0])
    if "column" in stats_df.columns:
        for colname in ["value", "row_count", "rows_count", "rowcount"]:
            if colname in stats_df.columns:
                null_row = stats_df[stats_df["column"].isna()]
                if not null_row.empty:
                    s = pd.to_numeric(null_row[colname], errors="coerce").dropna()
                    if not s.empty:
                        return int(s.iloc[0])
    return None

def _extract_column_stat(stats_df: Optional[pd.DataFrame], column_name: str, field: str) -> Optional[float]:
    if stats_df is None or stats_df.empty or "column" not in stats_df.columns:
        return None
    actual_field = field
    if field == "number_of_distinct_values" and "number_of_distinct_values" not in stats_df.columns:
        if "distinct_values_count" in stats_df.columns:
            actual_field = "distinct_values_count"
        else:
            return None
    row = stats_df[stats_df["column"] == column_name]
    if row.empty:
        return None
    val = pd.to_numeric(row.iloc[0][actual_field], errors="coerce")
    return float(val) if pd.notna(val) else None

@cache_data(ttl=3600, show_spinner=False)
def table_row_count(catalog: str, schema: str, table: str, allow_fallback_sql: bool) -> Optional[int]:
    rc = _extract_row_count(show_stats(catalog, schema, table))
    if rc is not None:
        return rc
    if allow_fallback_sql:
        try:
            rows, _ = run_sql(f"SELECT COUNT(*) AS cnt FROM {qident(catalog)}.{qident(schema)}.{qident(table)}")
            return int(rows[0][0]) if rows else None
        except Exception:
            return None
    return None

# =========================
# FULL-TABLE Data Quality
# =========================
def _is_numeric(dt: str) -> bool:
    x = (dt or "").lower()
    return x.startswith(("int","bigint","smallint","tinyint","integer","double","real","float","decimal","numeric"))

def _is_temporal(dt: str) -> bool:
    x = (dt or "").lower()
    return any(k in x for k in ("date","timestamp","time"))

def _alias(prefix: str, col: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_]", "_", col)[:80]
    uniq = hashlib.sha1(col.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}__{safe}__{uniq}"

def _build_quality_sql(catalog: str, schema: str, table: str,
                       include_distinct: bool, prefer_exact_distinct: bool,
                       include_minmax: bool) -> Tuple[str, Dict[str,str]]:
    table_fqn = f"{qident(catalog)}.{qident(schema)}.{qident(table)}"
    aliases: Dict[str, str] = {}
    select_items = ["COUNT(*) AS __row_count"]
    cols_df = list_columns_for_table(catalog, schema, table)

    for _, r in cols_df.iterrows():
        col = r["column_name"]; dt = str(r["data_type"])
        a_n = _alias("n", col); aliases[a_n] = col
        select_items.append(f"COUNT_IF({qident(col)} IS NULL) AS {qident(a_n)}")

        if include_distinct:
            a_d = _alias("d", col); aliases[a_d] = col
            if prefer_exact_distinct:
                select_items.append(f"COUNT(DISTINCT {qident(col)}) AS {qident(a_d)}")
            else:
                select_items.append(f"approx_distinct({qident(col)}) AS {qident(a_d)}")

        if include_minmax and (_is_numeric(dt) or _is_temporal(dt)):
            a_min = _alias("min", col); aliases[a_min] = col
            a_max = _alias("max", col); aliases[a_max] = col
            select_items.append(f"MIN({qident(col)}) AS {qident(a_min)}")
            select_items.append(f"MAX({qident(col)}) AS {qident(a_max)}")

    return f"SELECT {', '.join(select_items)} FROM {table_fqn}", aliases

@cache_data(ttl=1800, show_spinner=False)
def full_table_quality(catalog: str, schema: str, table: str,
                       include_distinct: bool, prefer_exact_distinct: bool,
                       include_minmax: bool) -> pd.DataFrame:
    sql, aliases = _build_quality_sql(catalog, schema, table,
                                      include_distinct, prefer_exact_distinct, include_minmax)
    rows, cols = run_sql(sql)
    if not rows:
        return pd.DataFrame(columns=["column_name","nulls_count","nulls_fraction","distinct_count","min","max"])
    row_map = dict(zip(cols, rows[0]))
    total = int(row_map.get("__row_count", 0) or 0)

    cols_df = list_columns_for_table(catalog, schema, table)
    records = []
    for _, r in cols_df.iterrows():
        col = r["column_name"]
        rec = {"column_name": col, "nulls_count": None, "nulls_fraction": None, "distinct_count": None, "min": None, "max": None}
        for role in ("n","d","min","max"):
            for a, original in aliases.items():
                if original == col and a.startswith(f"{role}__"):
                    val = row_map.get(a)
                    if role == "n":
                        rec["nulls_count"] = int(val) if val is not None else None
                    elif role == "d":
                        try:
                            rec["distinct_count"] = int(val) if val is not None else None
                        except Exception:
                            rec["distinct_count"] = val
                    elif role == "min":
                        rec["min"] = val
                    elif role == "max":
                        rec["max"] = val
        if total and rec["nulls_count"] is not None:
            rec["nulls_fraction"] = rec["nulls_count"] / total
        records.append(rec)

    out = pd.DataFrame.from_records(records)
    out["nulls_count"] = out["nulls_count"].astype("Int64")
    out["distinct_count"] = out["distinct_count"].astype("Int64")
    return out

# =========================
# Search helpers
# =========================
def search_keyword(cols_df: pd.DataFrame, query: str, whole_word: bool = False) -> pd.DataFrame:
    if not query.strip(): return cols_df.iloc[0:0].copy()
    pattern = rf"\b{re.escape(query)}\b" if whole_word else re.escape(query)
    mask = (
        cols_df["table_name"].str.contains(pattern, case=False, regex=True) |
        cols_df["column_name"].str.contains(pattern, case=False, regex=True) |
        cols_df["data_type"].astype(str).str.contains(pattern, case=False, regex=True)
    )
    return cols_df[mask].copy()

def search_by_column(cols_df: pd.DataFrame, col_query: str, exact: bool = False, whole_word: bool = False) -> pd.DataFrame:
    if not col_query.strip(): return cols_df.iloc[0:0].copy()
    if exact:
        mask = cols_df["column_name"].str.lower() == col_query.strip().lower()
    else:
        pattern = rf"\b{re.escape(col_query)}\b" if whole_word else re.escape(col_query)
        mask = cols_df["column_name"].str.contains(pattern, case=False, regex=True)
    return cols_df[mask].copy()

# =========================
# Warm cache (parallel, button-triggered)
# =========================
@cache_data(ttl=3600, show_spinner=False)
def tables_with_rowcounts(catalog: str, schema: str, table_names: List[str], allow_fallback_sql: bool) -> pd.DataFrame:
    records = []
    for t in table_names:
        rc = table_row_count(catalog, schema, t, allow_fallback_sql=allow_fallback_sql)
        records.append({"table_name": t, "row_count": rc})
    return pd.DataFrame(records)

@cache_data(ttl=3600, show_spinner=False)
def column_profile(
    catalog: str,
    schema: str,
    table: str,
    allow_fallback_sql: bool,
    correct_inconsistent: bool,
    prefer_exact_distinct: bool,
) -> pd.DataFrame:
    cols_all = list_columns_for_schema(catalog, schema, workers=DEFAULT_WORKERS)
    stats = show_stats(catalog, schema, table)
    row_count = _extract_row_count(stats)

    sub = cols_all[cols_all["table_name"] == table][
        ["column_name", "data_type", "ordinal_position"]
    ].copy()

    sub["nulls_fraction"] = sub["column_name"].apply(lambda c: _extract_column_stat(stats, c, "nulls_fraction"))
    sub["distinct_count"] = sub["column_name"].apply(lambda c: _extract_column_stat(stats, c, "number_of_distinct_values"))
    sub["source_distinct"] = sub["distinct_count"].apply(lambda v: "stats" if pd.notna(v) else pd.NA)

    if row_count is not None:
        frac = sub["nulls_fraction"].clip(lower=0, upper=1)
        sub["nulls_fraction"] = frac
        sub["nulls_count"] = (frac.fillna(0) * row_count).round().astype("Int64")
    else:
        sub["nulls_count"] = pd.NA

    if correct_inconsistent and row_count is not None:
        sub["nulls_fraction"] = sub["nulls_fraction"].clip(lower=0, upper=1)
        sub["nulls_count"] = sub["nulls_count"].clip(lower=0, upper=row_count).astype("Int64")
        sub.loc[(sub["distinct_count"].notna()) & (sub["distinct_count"] > row_count), "distinct_count"] = row_count
        sub.loc[sub["source_distinct"].isna() & sub["distinct_count"].notna(), "source_distinct"] = "derived"

    sub["distinct_count"] = sub["distinct_count"].astype("Int64")
    sub = sub.sort_values("ordinal_position").reset_index(drop=True)
    return sub

def warm_cache_for_schema(
    catalog: str,
    schema: str,
    tables: List[str],
    limit: int,
    scope: str,
    workers: int,
    use_show_stats: bool,
    allow_fallback_sql_warm: bool,
    correct_inconsistent: bool,
    prefer_exact_distinct: bool,
) -> None:
    to_warm = tables[:limit]
    if not to_warm:
        return
    # prime schema columns
    try: _ = list_columns_for_schema(catalog, schema, workers=workers)
    except Exception: pass

    scope_stats  = scope in ("Row count + stats", "Row + stats + profile")
    scope_prof   = scope == "Row + stats + profile"
    allow_fallback_sql_warm = allow_fallback_sql_warm and use_show_stats
    max_workers = max(1, min(workers, len(to_warm)))  # autoscale

    progress = st.progress(0.0, text=f"Warming cache for {schema} …")
    total, done = len(to_warm), 0

    def work(table: str) -> str:
        try: _ = table_row_count(catalog, schema, table, allow_fallback_sql=allow_fallback_sql_warm)
        except Exception: pass
        if scope_stats:
            try: _ = show_stats(catalog, schema, table)
            except Exception: pass
        if scope_prof:
            try:
                _ = column_profile(
                    catalog, schema, table,
                    allow_fallback_sql=allow_fallback_sql_warm,
                    correct_inconsistent=correct_inconsistent,
                    prefer_exact_distinct=prefer_exact_distinct,
                )
            except Exception: pass
        return table

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(work, t): t for t in to_warm}
        for fut in as_completed(futures):
            _ = fut.result()
            done += 1
            progress.progress(done / total, text=f"Warming cache: {done}/{total}")
    progress.empty()
    st.success(f"Cache warmed for {done} table(s) in {schema} with {max_workers} worker(s).")

# =========================
# Blast Radius
# =========================
def _id_like(c: str) -> bool:
    x = c.lower()
    return x == "id" or x.endswith("_id") or x.endswith("id")

def _score_overlap(target_cols: Set[str], other_cols: Set[str]) -> Tuple[int, int, float]:
    inter = target_cols & other_cols
    id_hits = sum(1 for c in inter if _id_like(c))
    jaccard = (len(inter) / len(target_cols | other_cols)) if (target_cols | other_cols) else 0.0
    score = id_hits * 3 + len(inter)
    return score, len(inter), jaccard

def _candidate_pairs_same_name(catalog: str, schema: str, base_table: str, keys: List[str],
                               workers: int) -> List[Tuple[str, str, str]]:
    pairs: List[Tuple[str, str, str]] = []
    cols_schema = list_columns_for_schema(catalog, schema, workers=workers)
    keyset = {k.lower() for k in keys}
    for tbl, sub in cols_schema.groupby("table_name"):
        if tbl == base_table:
            continue
        names = set(sub["column_name"].str.lower().tolist())
        for k in keyset:
            if k in names:
                matched = sub[sub["column_name"].str.lower() == k]["column_name"].iloc[0]
                pairs.append((tbl, k, matched))
    return pairs

def _build_overlap_sql(catalog: str, schema: str, base_table: str, base_key: str,
                       other_table: str, other_col: str, sample_1000: bool) -> str:
    fqn_base  = f"{qident(catalog)}.{qident(schema)}.{qident(base_table)}"
    fqn_other = f"{qident(catalog)}.{qident(schema)}.{qident(other_table)}"
    limit_clause = " LIMIT 1000" if sample_1000 else ""
    return f"""
WITH tkeys AS (
  SELECT DISTINCT CAST({qident(base_key)} AS VARCHAR) AS k
  FROM {fqn_base}
  WHERE {qident(base_key)} IS NOT NULL
  {limit_clause}
)
SELECT
  (SELECT COUNT(*) FROM tkeys) AS sample_keys,
  COUNT(*) AS matched_keys
FROM tkeys t
WHERE EXISTS (
  SELECT 1 FROM {fqn_other} u
  WHERE CAST(u.{qident(other_col)} AS VARCHAR) = t.k
)
"""

# =========================
# UI Components
# =========================
def sidebar_controls(catalogs: List[str]) -> Dict[str, object]:
    st.sidebar.markdown("### 🌐 Workspace")
    if "catalog" not in st.session_state:
        st.session_state.catalog = catalogs[0]
    catalog = st.sidebar.selectbox("Catalog", catalogs, index=catalogs.index(st.session_state.catalog))
    if catalog != st.session_state.catalog:
        st.session_state.catalog = catalog
        new_schemas = list_schemas(catalog)
        st.session_state.schema = new_schemas[0] if new_schemas else None

    schemas = list_schemas(st.session_state.catalog)
    if not schemas:
        st.sidebar.error(f"No schemas in catalog '{st.session_state.catalog}'.")
        st.stop()
    if ("schema" not in st.session_state) or (st.session_state.schema not in schemas):
        st.session_state.schema = schemas[0]
    schema = st.sidebar.selectbox("Schema", schemas, index=schemas.index(st.session_state.schema))

    st.sidebar.markdown("### 🔎 Tables")
    table_filter = st.sidebar.text_input("Filter tables (contains)", value="")
    preview_limit = st.sidebar.slider("Preview rows", PREVIEW_MIN, PREVIEW_MAX, PREVIEW_DEFAULT, PREVIEW_STEP)

    st.sidebar.markdown("### ⚙️ Stats & Caching")
    use_show_stats = st.sidebar.checkbox("Use SHOW STATS", value=True)
    allow_fallback_sql = st.sidebar.checkbox("Allow COUNT/approx_distinct fallback", value=False)
    correct_inconsistent = st.sidebar.checkbox("Clip inconsistent stats", value=True)
    prefer_exact_distinct = st.sidebar.checkbox("Prefer exact DISTINCT when fixing", value=False)

    st.sidebar.markdown("### 🚀 Warm-up (on demand)")
    warm_limit = st.sidebar.slider("Max tables to warm", 5, 500, 60, 5)
    warm_scope = st.sidebar.selectbox("Warm scope", ["Row count only", "Row count + stats", "Row + stats + profile"], index=1)
    warm_workers = st.sidebar.slider("Parallel workers", 1, 32, DEFAULT_WORKERS)
    warm_allow_fallbacks = st.sidebar.checkbox("Allow fallbacks during warm (slow)", value=False)
    warm_click = st.sidebar.button("Warm cache now")

    st.sidebar.markdown("### 🧪 Data Quality")
    dq_include_distinct = st.sidebar.checkbox("Compute distinct counts", value=True)
    dq_prefer_exact = st.sidebar.checkbox("Use exact DISTINCT (vs approx)", value=False)
    dq_include_minmax = st.sidebar.checkbox("Min/Max for numeric & date", value=True)

    st.sidebar.markdown("### 🧨 Blast Radius (per schema)")
    blast_method = st.sidebar.selectbox("Mode", ["Metadata (fast)", "Data: sample 1,000 keys", "Data: ALL keys"], index=0)
    blast_workers = st.sidebar.slider("Blast parallel workers", 1, 32, DEFAULT_WORKERS)
    blast_topk = st.sidebar.slider("Show top related tables", 5, 50, 20, 1)

    return dict(
        schema=schema,
        table_filter=table_filter,
        preview_limit=preview_limit,
        use_show_stats=use_show_stats,
        allow_fallback_sql=allow_fallback_sql,
        correct_inconsistent=correct_inconsistent,
        prefer_exact_distinct=prefer_exact_distinct,
        warm_limit=warm_limit,
        warm_scope=warm_scope,
        warm_workers=warm_workers,
        warm_allow_fallbacks=warm_allow_fallbacks,
        warm_click=warm_click,
        dq_include_distinct=dq_include_distinct,
        dq_prefer_exact=dq_prefer_exact,
        dq_include_minmax=dq_include_minmax,
        blast_method=blast_method,
        blast_workers=blast_workers,
        blast_topk=blast_topk,
    )

def kpis(catalogs: List[str], schemas: List[str], tables: List[str]) -> None:
    st.markdown('<div class="kpi-grid">', unsafe_allow_html=True)
    for title, val in [("Catalogs", len(catalogs)), ("Schemas", len(schemas)), ("Tables", len(tables)), ("Default Workers", DEFAULT_WORKERS)]:
        st.markdown(f"""
        <div class="card">
          <h4>{title}</h4>
          <div style="font-size:1.4rem"><strong>{val:,}</strong></div>
          <div class="muted">session</div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

def table_browser(catalog: str, schema: str, tables_all: List[str], table_filter: str, allow_fallback_sql: bool) -> Tuple[Optional[str], pd.DataFrame]:
    shown = [t for t in tables_all if table_filter.lower() in t.lower()] if table_filter else tables_all
    rc_df = tables_with_rowcounts(catalog, schema, shown, allow_fallback_sql)
    tbl_df = pd.DataFrame({"table_name": shown}).merge(rc_df, on="table_name", how="left")
    tbl_df = tbl_df.sort_values(["table_name"]).reset_index(drop=True)

    left, right = st.columns([1.25, 1.75], gap="large")
    with left:
        st.markdown("#### 🗂️ Tables")
        st.dataframe(tbl_df, use_container_width=True, height=420)
        selected = st.selectbox("Select table", shown, index=0 if shown else None) if shown else None
    with right:
        st.markdown("#### 📐 Columns (selected)")
        if selected:
            st.caption(f"FQN: `{catalog}.{schema}.{selected}``")
            try:
                _raw = list_columns_for_table(catalog, schema, selected)
                st.dataframe(_raw, use_container_width=True, height=260)
            except Exception as e:
                st.warning(f"Columns error: {e}")
    return selected, tbl_df

def profiling_panel(catalog: str, schema: str, selected_table: Optional[str],
                    allow_fallback_sql: bool, correct_inconsistent: bool, prefer_exact_distinct: bool,
                    preview_limit: int) -> None:
    st.markdown("#### 📊 Column Profiling (cached)")
    if not selected_table:
        st.info("Select a table from Browse to see profiling.")
        return
    profile_df = column_profile(
        catalog, schema, selected_table,
        allow_fallback_sql=allow_fallback_sql,
        correct_inconsistent=correct_inconsistent,
        prefer_exact_distinct=prefer_exact_distinct,
    )
    if profile_df.empty:
        st.warning("No column metadata available. Try another table.")
        return
    ordered = profile_df[
        ["ordinal_position","column_name","data_type","nulls_count","nulls_fraction","distinct_count","source_distinct"]
    ].copy()
    st.dataframe(
        ordered, use_container_width=True, height=420, hide_index=True,
        column_config={
            "nulls_fraction": st.column_config.ProgressColumn("nulls_fraction", format="%.2f", min_value=0.0, max_value=1.0),
            "distinct_count": st.column_config.NumberColumn(format="%,d"),
            "nulls_count": st.column_config.NumberColumn(format="%,d"),
        }
    )
    if st.button("Preview data"):
        data = preview_table(catalog, schema, selected_table, preview_limit)
        st.dataframe(data, use_container_width=True, height=340)
        if not data.empty:
            buf = io.StringIO()
            data.to_csv(buf, index=False)
            st.download_button("Download CSV", data=buf.getvalue(), file_name=f"{selected_table}_preview.csv", mime="text/csv")

def data_quality_panel(catalog: str, schema: str, selected_table: Optional[str],
                       include_distinct: bool, prefer_exact_distinct: bool, include_minmax: bool) -> None:
    st.markdown("#### 🧪 Data Quality (full table — all rows)")
    if not selected_table:
        st.info("Select a table to compute full-table quality.")
        return
    go = st.button("Run full-table scan")
    if not go:
        st.caption("Click run to compute nulls, distincts (exact/approx), min/max over ALL rows.")
        return
    with st.spinner("Scanning table..."):
        dq = full_table_quality(
            catalog, schema, selected_table,
            include_distinct=include_distinct, prefer_exact_distinct=prefer_exact_distinct,
            include_minmax=include_minmax,
        )
    if dq.empty:
        st.warning("No data quality metrics returned. Check permissions or try another table.")
        return
    st.dataframe(
        dq, use_container_width=True, hide_index=True, height=460,
        column_config={
            "nulls_fraction": st.column_config.ProgressColumn("nulls_fraction", format="%.2f", min_value=0.0, max_value=1.0),
            "distinct_count": st.column_config.NumberColumn(format="%,d"),
            "nulls_count": st.column_config.NumberColumn(format="%,d"),
        }
    )
    st.download_button("Download Data Quality CSV", data=dq.to_csv(index=False), file_name=f"{selected_table}_data_quality.csv", mime="text/csv")

def metadata_search_panel(catalog: str, schema: str, workers: int) -> None:
    st.markdown("### 🔎 Metadata Search")
    col1, col2, col3 = st.columns([2,1,1])
    with col1:
        query = st.text_input("Search tables, columns, and data types", placeholder="e.g., account_id, email, date, bigint")
    with col2:
        mode = st.selectbox("Mode", ["Column name", "Keyword"], index=0)
    with col3:
        whole_word = st.checkbox("Whole word", value=False)

    # Lazy build: only index when user interacts
    if not query:
        st.caption("Type a query to index this schema (parallel) and search.")
        return

    with st.spinner("Indexing schema metadata (parallel) ..."):
        cols_df = list_columns_for_schema(catalog, schema, workers=workers)

    if cols_df.empty:
        st.warning("No metadata available for this schema.")
        return

    if mode == "Column name":
        exact = st.checkbox("Exact column name", value=False)
        hits = search_by_column(cols_df, query, exact=exact, whole_word=whole_word)
    else:
        hits = search_keyword(cols_df, query, whole_word=whole_word)

    if hits.empty:
        st.info("No matches found.")
        return

    st.caption("Results")
    agg = hits.groupby("table_name").size().reset_index(name="matching_columns").sort_values(
        ["matching_columns", "table_name"], ascending=[False, True]
    )

    st.markdown('<div class="grid2">', unsafe_allow_html=True)
    # small card grid summary
    for _, r in agg.head(8).iterrows():
        st.markdown(f"""
        <div class="card">
          <h4>🗂️ {r['table_name']}</h4>
          <div class="muted">Matches: <strong>{int(r['matching_columns'])}</strong></div>
          <div class="chips" style="margin-top:.25rem">
            <span class="chip">schema: {schema}</span>
            <span class="chip">catalog: {catalog}</span>
          </div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("#### Detailed matches")
    st.dataframe(hits[["table_name","column_name","data_type"]], use_container_width=True, hide_index=True, height=420)

def blast_radius_studio(catalog: str, schema: str, selected_table: Optional[str],
                        method: str, topk: int, workers: int) -> None:
    st.markdown("### 🧨 Blast Radius Studio")
    if not selected_table:
        st.info("Select a base table in Browse.")
        return

    cols_df = list_columns_for_table(catalog, schema, selected_table)
    if cols_df.empty:
        st.warning("No columns found for selected table.")
        return
    all_cols = cols_df["column_name"].tolist()
    default_keys = [c for c in all_cols if _id_like(c)]
    keys = st.multiselect(
        "Step 1 — Choose 1–3 key columns (id-like recommended)",
        options=all_cols,
        default=default_keys[:3] if default_keys else all_cols[:1],
    )
    if not keys:
        st.stop()

    st.write(f"**Step 2 — Mode:** {method}")
    sample_mode = (method == "Data: sample 1,000 keys")

    if st.button("Run Blast Radius"):
        if method == "Metadata (fast)":
            universe = list_columns_for_schema(catalog, schema, workers=workers)
            if universe.empty or "table_name" not in universe.columns or "column_name" not in universe.columns:
                st.info(f"No metadata available for schema '{schema}'."); return
            target_cols = set([c.lower() for c in all_cols])
            results = []
            for tbl, sub in universe.groupby("table_name"):
                if tbl == selected_table: continue
                other_cols = set(sub["column_name"].str.lower().tolist())
                score, inter_cnt, jacc = _score_overlap(target_cols, other_cols)
                if score > 0:
                    results.append({"table": tbl, "score": score, "shared_cols": inter_cnt, "jaccard": jacc})
            if not results:
                st.success(f"No likely-impact candidates found in schema '{schema}'.")
                return
            impact = pd.DataFrame(results).sort_values(["score","shared_cols","jaccard"], ascending=[False,False,False]).head(topk)

            st.markdown('<div class="grid3">', unsafe_allow_html=True)
            for _, r in impact.iterrows():
                cls = "chip-ok" if r["jaccard"]>=0.25 else ("chip-warn" if r["jaccard"]>=0.1 else "chip-bad")
                st.markdown(f"""
                <div class="card">
                  <h4>📎 {r['table']}</h4>
                  <div class="muted">Shared columns: <strong>{int(r['shared_cols'])}</strong></div>
                  <div class="muted">Score: <span class="pill">{int(r['score'])}</span></div>
                  <div style="margin-top:.3rem">Similarity:
                    <span class="pill {cls}">{r['jaccard']:.2f} jaccard</span>
                  </div>
                </div>
                """, unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            return

        st.caption(f"Analyzing value overlap for keys {', '.join(keys)} in `{schema}` ...")
        pairs = _candidate_pairs_same_name(catalog, schema, selected_table, keys, workers=workers)
        if not pairs:
            st.info("No tables in this schema share the same-named keys. Try a different key or table."); return

        status = st.empty()
        progress = st.progress(0.0)
        total, done = len(pairs), 0
        rows_out: List[Dict[str, object]] = []

        def work(pair: Tuple[str, str, str]) -> Optional[Dict[str, object]]:
            other_table, base_key_lc, other_col_name = pair
            base_key = next((k for k in keys if k.lower() == base_key_lc), keys[0])
            try:
                sql = _build_overlap_sql(catalog, schema, selected_table, base_key, other_table, other_col_name, sample_1000=sample_mode)
                rows, cols = run_sql(sql)
                if not rows: return None
                sample_keys, matched_keys = rows[0]
                coverage = (matched_keys / sample_keys) if sample_keys else 0.0
                return {
                    "other_table": other_table, "base_key": base_key, "other_col": other_col_name,
                    "sample_keys": int(sample_keys), "matched_keys": int(matched_keys), "coverage": coverage
                }
            except Exception as e:
                return {"other_table": other_table, "base_key": base_key, "other_col": other_col_name,
                        "sample_keys": 0, "matched_keys": 0, "coverage": 0.0, "error": str(e)}

        max_workers = max(1, min(workers, len(pairs)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(work, (tbl, k, col)): (tbl, k, col) for (tbl, k, col) in pairs}
            for fut in as_completed(futures):
                res = fut.result()
                if res: rows_out.append(res)
                done += 1
                progress.progress(done / total)
                if res and "error" not in res:
                    status.markdown(f"✅ `{res['other_table']}` on `{res['other_col']}` — {res['matched_keys']}/{res['sample_keys']} keys ({res['coverage']:.1%})")
                elif res:
                    status.markdown(f"⚠️ `{res['other_table']}` ({res['other_col']}): {res.get('error','error')}")

        progress.empty()
        if not rows_out:
            st.warning("No overlaps found with the chosen keys."); return

        df = pd.DataFrame(rows_out)
        best = (
            df.sort_values(["other_table","coverage","matched_keys"], ascending=[True, False, False])
              .groupby("other_table", as_index=False)
              .first()
              .sort_values(["coverage","matched_keys"], ascending=[False, False])
              .head(topk)
        )

        st.markdown('<div class="grid3">', unsafe_allow_html=True)
        for _, r in best.iterrows():
            cov = r["coverage"]
            cls = "chip-ok" if cov>=0.5 else ("chip-warn" if cov>=0.2 else "chip-bad")
            st.markdown(f"""
            <div class="card">
              <h4>🔗 {r['other_table']}</h4>
              <div class="muted">Key: <code>{r['base_key']}</code> → <code>{r['other_col']}</code></div>
              <div class="muted">Matched: <strong>{int(r['matched_keys']):,}</strong> / {int(r['sample_keys']):,}</div>
              <div style="margin-top:.3rem">Coverage:
                <span class="pill {cls}">{cov:.1%}</span>
              </div>
            </div>
            """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("#### Detailed results")
        st.dataframe(
            best[["other_table","base_key","other_col","matched_keys","sample_keys","coverage"]],
            use_container_width=True, hide_index=True, height=420,
            column_config={
                "matched_keys": st.column_config.NumberColumn(format="%,d"),
                "sample_keys": st.column_config.NumberColumn(format="%,d"),
                "coverage": st.column_config.ProgressColumn("coverage", min_value=0.0, max_value=1.0, format="%.2f"),
            }
        )

def sql_footer(preview_limit: int) -> None:
    with st.expander("SQL used (no information_schema)"):
        st.code(
f"""-- catalogs (filtered in app)
SHOW CATALOGS;

-- schemas (filtered in app)
SHOW SCHEMAS FROM {st.session_state.catalog};

-- tables
SHOW TABLES FROM {st.session_state.catalog}.{st.session_state.schema};

-- columns (per-table; no information_schema)
SHOW COLUMNS FROM {st.session_state.catalog}.{st.session_state.schema}.<table_name>;

-- stats
SHOW STATS FOR {st.session_state.catalog}.{st.session_state.schema}.<table_name>;

-- preview
SELECT * FROM {st.session_state.catalog}.{st.session_state.schema}.<selected_table> LIMIT {preview_limit};

-- data quality (full table)
SELECT COUNT(*) AS __row_count,
       COUNT_IF("col" IS NULL) AS n__col__XXXX,
       approx_distinct("col")  AS d__col__XXXX,   -- or COUNT(DISTINCT "col")
       MIN("col") AS min__col__XXXX, MAX("col") AS max__col__XXXX
FROM {st.session_state.catalog}.{st.session_state.schema}.<selected_table>;

-- blast radius (data, sample or all)
WITH tkeys AS (
  SELECT DISTINCT CAST("key_col" AS VARCHAR) AS k
  FROM {st.session_state.catalog}.{st.session_state.schema}.<base_table>
  WHERE "key_col" IS NOT NULL
  -- LIMIT 1000   -- include for 'sample 1,000'
)
SELECT (SELECT COUNT(*) FROM tkeys) AS sample_keys,
       COUNT(*) AS matched_keys
FROM tkeys t
WHERE EXISTS (
  SELECT 1 FROM {st.session_state.catalog}.{st.session_state.schema}.<other_table> u
  WHERE CAST(u."key_col" AS VARCHAR) = t.k
);
""", language="sql")

# =========================
# App Bootstrap
# =========================
def main():
    apply_styles()

    # Connectivity ping
    try:
        _ = run_sql("SELECT 1")
        st.success("Connected to Trino")
    except Exception as e:
        st.error(f"Connection failed: {e}")
        st.stop()

    catalogs = list_catalogs()
    if not catalogs:
        st.error("No catalogs visible (after filtering system catalogs).")
        st.stop()

    # Ensure usable selection
    need_pick = ("catalog" not in st.session_state) or (st.session_state.catalog not in catalogs)
    if need_pick:
        cat, sch = pick_first_usable_location(catalogs)
        if not cat or not sch:
            st.error("No schemas available in any visible catalog. Check permissions/catalog setup.")
            st.stop()
        st.session_state.catalog = cat
        st.session_state.schema  = sch

    # Sidebar & state
    controls = sidebar_controls(catalogs)
    st.session_state.schema = controls["schema"]

    # Main lists for browse
    schemas = list_schemas(st.session_state.catalog)
    tables_all = list_tables(st.session_state.catalog, st.session_state.schema)

    # KPI cards
    kpis(catalogs, schemas, tables_all)

    # ======== 🔎 Metadata Search (prominent) ========
    metadata_search_panel(
        st.session_state.catalog,
        st.session_state.schema,
        workers=DEFAULT_WORKERS
    )

    # ======== 🧭 Browse ========
    st.markdown("### 🧭 Browse")
    selected_table, tbl_df = table_browser(
        st.session_state.catalog, st.session_state.schema,
        tables_all, controls["table_filter"],
        allow_fallback_sql=controls["allow_fallback_sql"] if controls["use_show_stats"] else False
    )

    # ======== 🚀 Warm cache (on demand) ========
    if controls["warm_click"]:
        with st.spinner("Warming cache..."):
            warm_cache_for_schema(
                st.session_state.catalog, st.session_state.schema, tables_all,
                limit=controls["warm_limit"], scope=controls["warm_scope"], workers=controls["warm_workers"],
                use_show_stats=controls["use_show_stats"], allow_fallback_sql_warm=controls["warm_allow_fallbacks"],
                correct_inconsistent=controls["correct_inconsistent"], prefer_exact_distinct=controls["prefer_exact_distinct"],
            )

    # ======== 📊 Profiling ========
    st.markdown("### 📊 Profiling")
    profiling_panel(
        st.session_state.catalog, st.session_state.schema, selected_table,
        allow_fallback_sql=controls["allow_fallback_sql"] if controls["use_show_stats"] else False,
        correct_inconsistent=controls["correct_inconsistent"],
        prefer_exact_distinct=controls["prefer_exact_distinct"],
        preview_limit=controls["preview_limit"]
    )

    # ======== 🧪 Data Quality ========
    st.markdown("### 🧪 Data Quality")
    data_quality_panel(
        st.session_state.catalog, st.session_state.schema, selected_table,
        include_distinct=controls["dq_include_distinct"],
        prefer_exact_distinct=controls["dq_prefer_exact"],
        include_minmax=controls["dq_include_minmax"]
    )

    # ======== 🧨 Blast Radius ========
    blast_radius_studio(
        st.session_state.catalog, st.session_state.schema, selected_table,
        method=controls["blast_method"], topk=controls["blast_topk"], workers=controls["blast_workers"]
    )

    # ======== Optional small graph ========
    if st.checkbox("Show catalog graph", value=False):
        def esc(s: str) -> str: return s.replace('"', '\\"')
        cat_label, sch_label = f"Catalog: {st.session_state.catalog}", f"Schema: {st.session_state.schema}"
        dot = [
            "digraph G {", "  rankdir=LR;", '  node [shape=box, fontname="Helvetica"];',
            f'  "C::{esc(cat_label)}" [label="{esc(cat_label)}", style=filled, fillcolor="#193153", fontcolor="#e5e7eb"];',
            f'  "S::{esc(sch_label)}" [label="{esc(sch_label)}", style=filled, fillcolor="#1b2a4a", fontcolor="#e5e7eb"];',
            f'  "C::{esc(cat_label)}" -> "S::{esc(sch_label)}";'
        ]
        for t in tbl_df["table_name"].tolist()[:MAX_GRAPH_TABLES]:
            t_label = f"Table: {t}"
            dot.append(f'  "T::{esc(t_label)}" [label="{esc(t_label)}", shape=note, style=filled, fillcolor="#12233f", fontcolor="#e5e7eb"];')
            dot.append(f'  "S::{esc(sch_label)}" -> "T::{esc(t_label)}";')
        dot.append("}")
        st.graphviz_chart("\n".join(dot), use_container_width=True)

    sql_footer(controls["preview_limit"])

if __name__ == "__main__":
    main()

