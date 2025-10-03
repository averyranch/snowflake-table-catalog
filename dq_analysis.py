# snow_dq_enhanced.py
# Run with: streamlit run snow_dq_enhanced.py
"""
SNOW Data Quality & Blast Radius - Enhanced (Single File)

Includes:
- Clean, dark, modern UI (header, tabs, metric cards)
- Browse & preview tables
- Data Quality checks: per-column total rows, nulls, unique counts, min/max, % null, % unique
- Blast Radius: downstream "impact" proxy + Jaccard similarity to related tables, with explicit relationship reasons
- Metadata Search (column name / any field)
- Parallel execution & caching
- Arrow compatibility fixes (cast min/max to VARCHAR + sanitizer)
- Hardened blast radius logic to avoid KeyError: 'jaccard'
- Zero-row guard — if a table has 0 rows, show a warning and skip analysis

Latest changes:
- Tightened APPROX_DISTINCT to 1% error.
- Clamped unique_pct to never exceed 100%.
- SHOW STATS removed from Data Quality tab.
"""

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from trino.dbapi import connect
from trino.auth import BasicAuthentication

# =========================
# Configuration
# =========================
HOST = "ai2025-free-cluster.trino.galaxy.starburst.io"
USER = "rahul.yaksh@bofa.com/accountadmin"
PASSWORD = "Aarush2011@"

APP_TITLE = "SNOW Data Quality Platform"
DEFAULT_PREVIEW = 50
DEFAULT_WORKERS = min(16, max(4, (os.cpu_count() or 8)))

cache_data = getattr(st, "cache_data", st.cache_data) if hasattr(st, "cache_data") else st.cache
cache_resource = getattr(st, "cache_resource", st.cache_resource) if hasattr(st, "cache_resource") else st.cache

# Catalog/schema filters
CATALOG_DENYLIST = {"system", "jmx", "galaxy"}
SCHEMA_DENY_EXACT = {"information_schema"}
SCHEMA_DENY_PREFIXES = ("information_schema", "_information_schema")

# Relationship threshold
JACCARD_THRESHOLD = 0.35


# =========================
# Utilities
# =========================
def qident(name: str) -> str:
    """Quote identifiers safely for Trino."""
    if name.startswith('"') and name.endswith('"'):
        return name
    return '"' + name.replace('"', '""') + '"'


@cache_resource(show_spinner=False)
def get_connection():
    return connect(
        host=HOST,
        port=443,
        user=USER,
        http_scheme="https",
        auth=BasicAuthentication(USER, PASSWORD),
    )


def run_sql(sql: str) -> Tuple[List[Tuple], List[str]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return rows, cols


def is_allowed_catalog(name: str) -> bool:
    return name and name.lower() not in CATALOG_DENYLIST


def is_allowed_schema(name: str) -> bool:
    lname = (name or "").lower()
    if lname in SCHEMA_DENY_EXACT:
        return False
    return not any(lname.startswith(p) for p in SCHEMA_DENY_PREFIXES)


def ensure_arrow_compat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize a DataFrame so Streamlit/Arrow can serialize it without warnings.
    - Cast bytes-like objects to utf-8 strings.
    - Cast exotic object types to strings.
    - Replace NaNs with None.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == "object":
            out[col] = out[col].apply(
                lambda x: x.decode("utf-8", "replace") if isinstance(x, (bytes, bytearray))
                else (str(x) if isinstance(x, (dict, list, set, tuple, complex)) else x)
            )
    return out.where(pd.notnull(out), None)


# =========================
# Metadata Helpers
# =========================
@cache_data(ttl=900, show_spinner=False)
def list_catalogs() -> List[str]:
    rows, _ = run_sql("SHOW CATALOGS")
    cats = sorted([r[0] for r in rows if is_allowed_catalog(r[0])])
    return cats


@cache_data(ttl=900, show_spinner=False)
def list_schemas(catalog: str) -> List[str]:
    rows, _ = run_sql(f"SHOW SCHEMAS FROM {qident(catalog)}")
    schemas = sorted([r[0] for r in rows if is_allowed_schema(r[0])])
    return schemas


@cache_data(ttl=900, show_spinner=False)
def list_tables(catalog: str, schema: str) -> List[str]:
    rows, _ = run_sql(f"SHOW TABLES FROM {qident(catalog)}.{qident(schema)}")
    return sorted([r[0] for r in rows])


@cache_data(ttl=900, show_spinner=False)
def get_columns(catalog: str, schema: str, table: str) -> pd.DataFrame:
    """
    Return columns dataframe: table_name, column_name, data_type, ordinal_position
    """
    sql = f"""
    SELECT
      table_name,
      column_name,
      data_type,
      ordinal_position
    FROM {qident(catalog)}.information_schema.columns
    WHERE table_schema = {repr(schema)} AND table_name = {repr(table)}
    ORDER BY ordinal_position
    """
    rows, cols = run_sql(sql)
    return pd.DataFrame(rows, columns=cols)


@cache_data(ttl=900, show_spinner=False)
def get_all_columns_in_schema(catalog: str, schema: str) -> pd.DataFrame:
    """
    Returns columns for ALL tables within a schema.
    """
    sql = f"""
    SELECT
      table_name,
      column_name,
      data_type,
      ordinal_position
    FROM {qident(catalog)}.information_schema.columns
    WHERE table_schema = {repr(schema)}
    """
    rows, cols = run_sql(sql)
    return pd.DataFrame(rows, columns=cols)


@cache_data(ttl=900, show_spinner=False)
def preview_table(catalog: str, schema: str, table: str, limit: int = DEFAULT_PREVIEW) -> pd.DataFrame:
    q = f"SELECT * FROM {qident(catalog)}.{qident(schema)}.{qident(table)} LIMIT {int(limit)}"
    rows, cols = run_sql(q)
    return pd.DataFrame(rows, columns=cols)


# ---------- Zero-row check ----------
@cache_data(ttl=300, show_spinner=False)
def table_has_rows(catalog: str, schema: str, table: str) -> bool:
    """
    Lightweight emptiness probe (does NOT COUNT all rows).
    Returns True if table yields at least one row.
    """
    fq = f"{qident(catalog)}.{qident(schema)}.{qident(table)}"
    try:
        rows, _ = run_sql(f"SELECT 1 FROM {fq} LIMIT 1")
        return len(rows) > 0
    except Exception:
        # On error, don't block analysis—assume it might have rows.
        return True


# =========================
# Data Quality (per-column)
# =========================
def _dq_sql_for_column(catalog: str, schema: str, table: str, col: str) -> str:
    """
    Build per-column stats query:
      total_rows, null_count, approx_unique_count, min, max
    Arrow-safe: CAST min/max to VARCHAR to avoid mixed object types.
    Uses tighter APPROX_DISTINCT error (1%).
    """
    fq = f'{qident(catalog)}.{qident(schema)}.{qident(table)}'
    return f"""
    SELECT
      {repr(col)} AS column_name,
      COUNT(*) AS total_rows,
      SUM(CASE WHEN {qident(col)} IS NULL THEN 1 ELSE 0 END) AS null_count,
      APPROX_DISTINCT({qident(col)}, 0.01) AS approx_unique_count,  -- tighter error (~1%)
      CAST(TRY(MIN({qident(col)})) AS VARCHAR) AS min_value,
      CAST(TRY(MAX({qident(col)})) AS VARCHAR) AS max_value
    FROM {fq}
    """


@cache_data(ttl=900, show_spinner=False)
def compute_data_quality(catalog: str, schema: str, table: str, workers: int = DEFAULT_WORKERS) -> pd.DataFrame:
    cols_df = get_columns(catalog, schema, table)
    if cols_df.empty:
        return pd.DataFrame(columns=["column_name", "total_rows", "null_count", "approx_unique_count",
                                     "min_value", "max_value", "null_pct", "unique_pct"])

    col_names = cols_df["column_name"].tolist()

    results: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(run_sql, _dq_sql_for_column(catalog, schema, table, c)): c for c in col_names}
        for fut in as_completed(futures):
            col = futures[fut]
            try:
                rows, cols = fut.result()
                df = pd.DataFrame(rows, columns=cols)
                results.append(df)
            except Exception:
                results.append(pd.DataFrame([{
                    "column_name": col,
                    "total_rows": None,
                    "null_count": None,
                    "approx_unique_count": None,
                    "min_value": None,
                    "max_value": None
                }]))

    if not results:
        return pd.DataFrame(columns=["column_name", "total_rows", "null_count", "approx_unique_count",
                                     "min_value", "max_value", "null_pct", "unique_pct"])

    out = pd.concat(results, ignore_index=True).sort_values("column_name")

    # Percent helpers
    def pct(a, b):
        try:
            if b in (0, None):
                return None
            return round((a / b) * 100.0, 2)
        except Exception:
            return None

    out["null_pct"] = [pct(n, t) if n is not None and t is not None else None for n, t in zip(out["null_count"], out["total_rows"])]

    # Clamp unique_pct to 100%
    clamped_unique_pct = []
    for u, t in zip(out["approx_unique_count"], out["total_rows"]):
        if t in (0, None):
            clamped_unique_pct.append(None)
            continue
        # Ensure numeric; guard against None
        u_num = float(u) if u is not None else 0.0
        t_num = float(t)
        ratio = min(u_num, t_num) / t_num * 100.0
        clamped_unique_pct.append(round(ratio, 2))
    out["unique_pct"] = clamped_unique_pct

    return out[["column_name", "total_rows", "null_count", "approx_unique_count", "min_value", "max_value", "null_pct", "unique_pct"]]


# =========================
# Blast Radius & Jaccard
# =========================
def jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set([x.lower() for x in a]), set([x.lower() for x in b])
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


@cache_data(ttl=900, show_spinner=False)
def blast_radius_analysis(catalog: str, schema: str, table: str, neighbors_limit: int = 25) -> Tuple[float, pd.DataFrame]:
    """
    Computes a proxy for 'blast radius' for a table by:
    - Comparing column sets with every other table in the schema
    - Marking "related" tables where Jaccard >= threshold or share key-like columns
    - Blast Radius = (# related tables) / (total tables - 1)

    Returns (score, df) where df has:
      base_table, related_table, relationship, jaccard, shared_columns_count, total_columns, shared_columns_sample
    """
    all_tables = list_tables(catalog, schema)
    empty_cols = ["base_table", "related_table", "relationship", "jaccard", "shared_columns_count", "total_columns", "shared_columns_sample"]

    if not all_tables or table not in all_tables:
        return 0.0, pd.DataFrame(columns=empty_cols)

    base_cols_df = get_columns(catalog, schema, table)
    base_cols = base_cols_df["column_name"].tolist() if not base_cols_df.empty else []

    def looks_like_key(c: str) -> bool:
        cl = c.lower()
        return cl == "id" or cl.endswith("_id") or cl.endswith("id")

    keyish = {c.lower() for c in base_cols if looks_like_key(c)}
    base_set = set(c.lower() for c in base_cols)

    related_rows = []
    for t in all_tables:
        if t == table:
            continue
        df = get_columns(catalog, schema, t)
        other_cols = df["column_name"].tolist() if not df.empty else []
        jac = jaccard(base_cols, other_cols)
        other_set = set(c.lower() for c in other_cols)
        shared = sorted(list(base_set & other_set))

        # Relationship logic
        has_jaccard = jac >= JACCARD_THRESHOLD
        has_keyoverlap = bool(keyish and keyish.intersection(other_set))

        if has_jaccard or has_keyoverlap:
            if has_jaccard and has_keyoverlap:
                relation = "both (schema-similar + key-overlap)"
            elif has_jaccard:
                relation = f"schema-similar (Jaccard ≥ {JACCARD_THRESHOLD})"
            else:
                relation = "key-overlap"

            related_rows.append({
                "base_table": table,
                "related_table": t,
                "relationship": relation,
                "jaccard": round(jac, 3),
                "shared_columns_count": len(shared),
                "total_columns": len(other_cols),
                "shared_columns_sample": ", ".join(shared[:8])
            })

    # Build DF with guaranteed columns
    related_df = pd.DataFrame(related_rows, columns=empty_cols)
    if related_df.empty:
        return 0.0, related_df

    # Sort: higher Jaccard first, then more shared cols
    sort_keys = [c for c in ["jaccard", "shared_columns_count"] if c in related_df.columns]
    if sort_keys:
        related_df = related_df.sort_values(sort_keys, ascending=[False] * len(sort_keys))

    total_others = max(len(all_tables) - 1, 1)
    score = min(1.0, len(related_df) / total_others)

    if neighbors_limit and not related_df.empty:
        related_df = related_df.head(neighbors_limit)

    return round(score, 3), related_df.reset_index(drop=True)


# =========================
# Enhanced Styles
# =========================
def apply_styles() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide", page_icon="❄️")
    st.markdown(
        """
        <style>
          :root {
            --primary: #2563eb;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --bg-dark: #0f172a;
            --bg-card: #1e293b;
            --border: #334155;
            --text-muted: #94a3b8;
          }

          html, body, .block-container {
            background: var(--bg-dark) !important;
            color: #f1f5f9;
          }

          .block-container {
            padding-top: 1.5rem !important;
            max-width: 1400px;
          }

          .app-header {
            background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%);
            padding: 1.25rem 1.5rem;
            border-radius: 16px;
            margin-bottom: 1.25rem;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
          }

          .app-header h1 {
            margin: 0;
            color: white;
            font-size: 1.8rem;
            font-weight: 600;
          }

          .app-header p {
            margin: 0.35rem 0 0 0;
            color: #dbeafe;
            opacity: 0.9;
          }

          .metric-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.0rem;
            text-align: center;
            transition: transform 0.2s;
          }

          .metric-card:hover {
            transform: translateY(-2px);
            border-color: var(--primary);
          }

          .metric-value {
            font-size: 1.6rem;
            font-weight: 700;
            color: var(--primary);
            margin: 0.4rem 0;
          }

          .metric-label {
            color: var(--text-muted);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
          }

          .status-badge {
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
          }

          .status-success { background: rgba(16,185,129,0.2); color: var(--success); border: 1px solid var(--success); }
          .status-danger { background: rgba(239,68,68,0.2); color: var(--danger); border: 1px solid var(--danger); }

          section[data-testid="stSidebar"] { background: #0c1324; border-right: 1px solid var(--border); }
          section[data-testid="stSidebar"] .stMarkdown h3 {
            color: var(--primary); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em;
            margin-top: 1.1rem; margin-bottom: 0.6rem;
          }

          .stDataFrame { border-radius: 8px !important; overflow: hidden; }

          .stButton > button {
            border-radius: 8px; font-weight: 500; transition: all 0.2s;
          }
          .stButton > button:hover { transform: translateY(-1px); box-shadow: 0 4px 6px rgba(0,0,0,0.2); }

          .stTabs [data-baseweb="tab-list"] {
            gap: 8px; background: var(--bg-card); padding: 0.5rem; border-radius: 12px;
          }
          .stTabs [data-baseweb="tab"] { border-radius: 8px; padding: 0.6rem 1.0rem; font-weight: 500; }

          .info-box {
            background: var(--bg-card);
            border-left: 4px solid var(--primary);
            padding: 0.9rem;
            border-radius: 8px;
            margin: 0.8rem 0;
          }
        </style>
        """,
        unsafe_allow_html=True
    )


# =========================
# UI Components
# =========================
def render_header():
    st.markdown(
        f"""
        <div class="app-header">
            <h1>❄️ {APP_TITLE}</h1>
            <p>Comprehensive data quality, blast radius analysis, and metadata exploration</p>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_metrics(catalogs: List[str], schemas: List[str], tables: List[str]):
    cols = st.columns(4)
    metrics = [
        ("Catalogs", len(catalogs), "📊"),
        ("Schemas", len(schemas), "📁"),
        ("Tables", len(tables), "🗂️"),
        ("Workers", DEFAULT_WORKERS, "⚡")
    ]
    for col, (label, value, icon) in zip(cols, metrics):
        with col:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div style="font-size: 1.3rem;">{icon}</div>
                    <div class="metric-value">{value:,}</div>
                    <div class="metric-label">{label}</div>
                </div>
                """,
                unsafe_allow_html=True
            )


def sidebar_config() -> Dict:
    st.sidebar.title("⚙️ Configuration")

    # Connection Status
    st.sidebar.markdown("### 🔌 Connection")
    try:
        _ = run_sql("SELECT 1")
        st.sidebar.markdown('<span class="status-badge status-success">● Connected</span>', unsafe_allow_html=True)
    except Exception as e:
        st.sidebar.markdown('<span class="status-badge status-danger">● Disconnected</span>', unsafe_allow_html=True)
        st.sidebar.error(f"Error: {str(e)[:160]}")
        st.stop()

    st.sidebar.markdown("### 📍 Location")
    catalogs = list_catalogs()
    if not catalogs:
        st.sidebar.error("No catalogs available")
        st.stop()

    if "catalog" not in st.session_state:
        st.session_state.catalog = catalogs[0]

    catalog = st.sidebar.selectbox("Catalog", catalogs, index=catalogs.index(st.session_state.catalog) if st.session_state.catalog in catalogs else 0)
    st.session_state.catalog = catalog

    schemas = list_schemas(catalog)
    if not schemas:
        st.sidebar.error("No schemas available")
        st.stop()

    if "schema" not in st.session_state or st.session_state.schema not in schemas:
        st.session_state.schema = schemas[0]

    schema = st.sidebar.selectbox("Schema", schemas, index=schemas.index(st.session_state.schema) if st.session_state.schema in schemas else 0)
    st.session_state.schema = schema

    tables = list_tables(catalog, schema)
    if "preview_limit" not in st.session_state:
        st.session_state.preview_limit = DEFAULT_PREVIEW

    st.sidebar.markdown("### 📦 Preview")
    preview_limit = st.sidebar.slider("Preview rows", 10, 500, st.session_state.preview_limit, step=10)
    st.session_state.preview_limit = preview_limit

    st.sidebar.markdown("### 🧵 Parallelism")
    workers = st.sidebar.slider("Workers", 2, 32, DEFAULT_WORKERS)

    return {"catalog": catalog, "schema": schema, "preview_limit": preview_limit, "workers": workers}


def render_table_browser(catalog: str, schema: str, preview_limit: int) -> Optional[str]:
    st.subheader("📊 Browse & Profile")
    tables = list_tables(catalog, schema)
    if not tables:
        st.info("No tables in this schema")
        return None

    left, right = st.columns([2, 3])
    with left:
        selected = st.selectbox("Select a table", tables)
        cols_df = get_columns(catalog, schema, selected)
        st.markdown("**Columns**")
        st.dataframe(ensure_arrow_compat(cols_df), use_container_width=True, height=280)

    with right:
        st.markdown("**Preview**")
        with st.spinner("Loading preview..."):
            try:
                df = preview_table(catalog, schema, selected, limit=preview_limit)
                if df is None or df.empty:
                    st.info("Table is empty")
                else:
                    st.dataframe(ensure_arrow_compat(df), use_container_width=True, height=360)
            except Exception as e:
                st.error(f"Preview failed: {str(e)[:200]}")

    return selected


def render_data_quality_tab(catalog: str, schema: str, selected_table: Optional[str], workers: int):
    st.subheader("🧪 Data Quality")
    if not selected_table:
        st.info("👈 Select a table from the browser to analyze")
        return

    st.markdown(f"**Analyzing:** `{catalog}.{schema}.{selected_table}`")

    if st.button("▶️ Run Data Quality", type="primary", use_container_width=True):
        # Zero-row guard
        if not table_has_rows(catalog, schema, selected_table):
            st.warning("Row count is zero — nothing to analyze.")
            return

        with st.spinner("Computing per-column quality metrics..."):
            dq = compute_data_quality(catalog, schema, selected_table, workers=workers)
            if dq is not None and not dq.empty:
                st.success("✓ Data quality analysis complete")
                st.dataframe(ensure_arrow_compat(dq), use_container_width=True, height=420)
                csv = dq.to_csv(index=False)
                st.download_button("⬇️ Download DQ Report", csv, f"{selected_table}_dq.csv", "text/csv", use_container_width=True)
            else:
                st.warning("No statistics available for this table")


def render_blast_radius_tab(catalog: str, schema: str, selected_table: Optional[str]):
    st.subheader("💥 Blast Radius & Similarity")
    if not selected_table:
        st.info("👈 Select a table from the browser to analyze")
        return

    st.markdown(
        f"""
        <div class="info-box">
          <b>Blast Radius</b> estimates how many tables in the schema are likely impacted
          if the selected table changes/breaks. It uses <i>column-set similarity</i>
          (Jaccard) and key-like overlaps (id, *_id) as a proxy in the absence of explicit lineage/foreign keys.
          <div style="margin-top: 6px;">
            <b>Base Table:</b> <code>{catalog}.{schema}.{selected_table}</code>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    with st.spinner("Analyzing related tables..."):
        score, related = blast_radius_analysis(catalog, schema, selected_table, neighbors_limit=25)

    c1, c2 = st.columns([1, 3])
    with c1:
        st.markdown(
            f"""
            <div class="metric-card">
                <div style="font-size: 1.3rem;">🔥</div>
                <div class="metric-value">{score:.2f}</div>
                <div class="metric-label">Blast Radius (0..1)</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c2:
        if related is None or related.empty or "jaccard" not in related.columns:
            st.info("No closely related tables detected in this schema.")
        else:
            st.markdown("**Related tables & relationships**")
            st.dataframe(
                ensure_arrow_compat(
                    related[
                        ["base_table", "related_table", "relationship", "jaccard",
                         "shared_columns_count", "total_columns", "shared_columns_sample"]
                    ]
                ),
                use_container_width=True,
                height=420
            )
            csv = related.to_csv(index=False)
            st.download_button("⬇️ Download Relationships", csv, f"{selected_table}_relationships.csv", "text/csv", use_container_width=True)


def render_search_tab(catalog: str, schema: str):
    st.subheader("🔎 Metadata Search")
    st.markdown(
        """
        <div class="info-box">
          Search across all tables and columns in the selected schema. Use this to find
          tables containing specific columns or data types.
        </div>
        """,
        unsafe_allow_html=True
    )
    col1, col2 = st.columns([3, 1])
    with col1:
        query = st.text_input("Search query", placeholder="e.g., customer_id, email, timestamp...", label_visibility="collapsed")
    with col2:
        mode = st.selectbox("Mode", ["Column name", "Any field"], label_visibility="collapsed")

    if query:
        all_cols = get_all_columns_in_schema(catalog, schema)
        if all_cols.empty:
            st.info("No metadata found.")
            return

        q = query.lower()
        if mode == "Column name":
            mask = all_cols["column_name"].str.lower().str.contains(re.escape(q), na=False)
        else:
            mask = (
                all_cols["table_name"].str.lower().str.contains(re.escape(q), na=False) |
                all_cols["column_name"].str.lower().str.contains(re.escape(q), na=False) |
                all_cols["data_type"].str.lower().str.contains(re.escape(q), na=False)
            )
        out = all_cols.loc[mask].sort_values(["table_name", "ordinal_position"])
        st.markdown(f"**Results: {len(out):,}**")
        st.dataframe(ensure_arrow_compat(out), use_container_width=True, height=420)


# =========================
# Main Application
# =========================
def main():
    apply_styles()
    render_header()

    config = sidebar_config()
    catalog = config["catalog"]
    schema = config["schema"]

    catalogs = list_catalogs()
    schemas = list_schemas(catalog)
    tables = list_tables(catalog, schema)

    render_metrics(catalogs, schemas, tables)

    st.markdown("---")

    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Browse & Profile",
        "🧪 Data Quality",
        "💥 Blast Radius",
        "🔎 Search"
    ])

    with tab1:
        selected_table = render_table_browser(catalog, schema, config["preview_limit"])
        st.session_state.selected_table = selected_table

    with tab2:
        render_data_quality_tab(catalog, schema, st.session_state.get("selected_table"), workers=config["workers"])

    with tab3:
        render_blast_radius_tab(catalog, schema, st.session_state.get("selected_table"))

    with tab4:
        render_search_tab(catalog, schema)


if __name__ == "__main__":
    main()

