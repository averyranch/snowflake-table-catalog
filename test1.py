# catalog_explorer_pro.py
# Run with: streamlit run catalog_explorer_pro.py
"""
Starburst/Trino catalog explorer with cached row counts, cached column profiling,
and optional warm-cache on first load for fast user UX.

Highlights:
- Avoids system catalogs (e.g., 'system', 'jmx', 'galaxy') via denylist
- Avoids information_schema entirely (no queries against it)
- Column metadata from SHOW COLUMNS per table (cached) with header normalization
- Robust init: auto-picks first usable catalog+schema if current is empty
"""

import io
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from trino.dbapi import connect
from trino.auth import BasicAuthentication

# ========= Locked credentials (consider secrets manager) =========
HOST = "ai2025-free-cluster.trino.galaxy.starburst.io"   # no https://
USER = "rahul.yaksh@bofa.com/accountadmin"               # include /role
PASSWORD = "Aarush2011@"                                 # rotate as needed
# ================================================================

PAGE_TITLE = "⭐ Starburst Table Catalog"
PREVIEW_DEFAULT = 50
PREVIEW_MIN, PREVIEW_MAX, PREVIEW_STEP = 10, 1000, 10
MAX_GRAPH_TABLES = 200

# Cache fallbacks (Streamlit>=1.18 has cache_data/resource)
cache_data = getattr(st, "cache_data", st.cache)
cache_resource = getattr(st, "cache_resource", st.cache)

# -------------------------
# Denylists / filters
# -------------------------
CATALOG_DENYLIST = {"system", "jmx", "galaxy"}  # hide catalogs you don't want to browse
SCHEMA_DENY_EXACT = {"information_schema"}      # classic info schema
SCHEMA_DENY_PREFIXES = ("information_schema", "_information_schema")

def is_allowed_catalog(name: str) -> bool:
    return name and name.lower() not in CATALOG_DENYLIST

def is_allowed_schema(name: str) -> bool:
    lname = (name or "").lower()
    if lname in SCHEMA_DENY_EXACT:
        return False
    return not any(lname.startswith(p) for p in SCHEMA_DENY_PREFIXES)

# =========================
# Page setup & styles
# =========================
def apply_styles() -> None:
    st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🧭")
    st.markdown(
        """
        <style>
          :root { --radius: 16px; }
          .block-container { padding-top: 1rem; padding-bottom: 1rem; }
          section[data-testid="stSidebar"] { width: 380px !important; }
          .kpi { background: rgba(0,0,0,.03); border: 1px solid #e5e7eb; border-radius: 16px; padding: .75rem; }
          div[data-testid="stExpander"] { border-radius: 16px !important; border: 1px solid #e5e7eb; }
          pre, code { border-radius: 12px !important; }
          hr { border-top: 1px solid #e5e7eb; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title(f"{PAGE_TITLE} (Catalog → Schema → Tables)")

# =========================
# Data Access Layer (DAL)
# =========================
@dataclass(frozen=True)
class ConnConfig:
    host: str = HOST
    user: str = USER
    password: str = PASSWORD
    http_scheme: str = "https"
    port: int = 443

@cache_resource(show_spinner=False)
def get_connection(cfg: ConnConfig = ConnConfig()):
    return connect(
        host=cfg.host,
        port=cfg.port,
        http_scheme=cfg.http_scheme,
        user=cfg.user,
        auth=BasicAuthentication(cfg.user, cfg.password),
    )

def run_sql(sql: str) -> Tuple[List[tuple], List[str]]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return rows, cols
    finally:
        try:
            cur.close()
        except Exception:
            pass

# ---------- Helpers ----------
def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

def pick_first_usable_location(catalogs: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return the first (catalog, schema) pair that has at least one allowed schema.
    If none exist, returns (None, None).
    """
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
# Metadata Services (no information_schema)
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
    """
    Use SHOW COLUMNS (no information_schema).
    Returns: table_name, column_name, data_type, ordinal_position
    """
    rows, cols = run_sql(f"SHOW COLUMNS FROM {qident(catalog)}.{qident(schema)}.{qident(table)}")

    # Normalize header case (Trino typically returns 'Column','Type','Extra','Comment')
    cols_norm = [str(c).strip().lower() for c in cols]
    df = pd.DataFrame(rows, columns=cols_norm)

    if df.empty:
        # Try a fallback for engines that prefer DESCRIBE
        try:
            rows2, cols2 = run_sql(f"DESCRIBE {qident(catalog)}.{qident(schema)}.{qident(table)}")
            cols2_norm = [str(c).strip().lower() for c in cols2]
            df = pd.DataFrame(rows2, columns=cols2_norm)
        except Exception:
            pass

    if df.empty:
        return pd.DataFrame(columns=["table_name", "column_name", "data_type", "ordinal_position"])

    # Canonicalize to expected names
    rename_map: Dict[str, str] = {}
    if "column" in df.columns: rename_map["column"] = "column_name"
    if "type"   in df.columns: rename_map["type"]   = "data_type"
    if "col_name" in df.columns: rename_map["col_name"] = "column_name"  # some connectors
    if "data_type" not in df.columns and "type" not in rename_map:
        # If an exotic header appears, try best-effort pick
        guess_type_col = next((c for c in df.columns if "type" in c), None)
        if guess_type_col:
            rename_map[guess_type_col] = "data_type"

    df = df.rename(columns=rename_map)

    # Require the two core columns; otherwise return empty stub
    if not {"column_name", "data_type"}.issubset(set(df.columns)):
        return pd.DataFrame(columns=["table_name", "column_name", "data_type", "ordinal_position"])

    # Add table name + ordinal based on row order
    df.insert(0, "table_name", table)
    df["ordinal_position"] = range(1, len(df) + 1)
    df = df[["table_name", "column_name", "data_type", "ordinal_position"]]
    return df

@cache_data(ttl=3600, show_spinner=False)
def list_columns_for_schema(catalog: str, schema: str) -> pd.DataFrame:
    """
    Collect columns for all tables in a schema via SHOW COLUMNS per table.
    Cached to avoid re-scans; used by search & profiling.
    """
    tables = list_tables(catalog, schema)
    parts: List[pd.DataFrame] = []
    for t in tables:
        try:
            df_t = list_columns_for_table(catalog, schema, t)
            if not df_t.empty:
                parts.append(df_t)
        except Exception:
            # skip individual table errors, keep going
            continue

    if not parts:
        # Return a typed empty frame so downstream UI doesn't silently die
        return pd.DataFrame(columns=["table_name", "column_name", "data_type", "ordinal_position"])

    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(["table_name", "ordinal_position"]).reset_index(drop=True)

@cache_data(ttl=900, show_spinner=False)
def preview_table(catalog: str, schema: str, table: str, limit: int = PREVIEW_DEFAULT) -> pd.DataFrame:
    q = f"SELECT * FROM {qident(catalog)}.{qident(schema)}.{qident(table)} LIMIT {int(limit)}"
    rows, cols = run_sql(q)
    return pd.DataFrame(rows, columns=cols)

# =========================
# Stats / Profiling (cached)
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

def _build_missing_aggs_sql(table_fqn: str, need_nulls_cols: List[str], need_distinct_cols: List[str], prefer_exact_distinct: bool) -> Optional[str]:
    select_items: List[str] = []
    for col in need_nulls_cols:
        select_items.append(f"COUNT_IF({qident(col)} IS NULL) AS n__{col}")
    for col in need_distinct_cols:
        if prefer_exact_distinct:
            select_items.append(f"COUNT(DISTINCT {qident(col)}) AS d__{col}")
        else:
            select_items.append(f"approx_distinct({qident(col)}) AS d__{col}")
    if not select_items:
        return None
    return f"SELECT {', '.join(select_items)} FROM {table_fqn}"

@cache_data(ttl=3600, show_spinner=False)
def column_profile(
    catalog: str,
    schema: str,
    table: str,
    allow_fallback_sql: bool,
    correct_inconsistent: bool,
    prefer_exact_distinct: bool,
) -> pd.DataFrame:
    """
    Cached per-table profile: column_name, data_type, ordinal_position,
    nulls_fraction, nulls_count, distinct_count, source_distinct
    """
    cols_all = list_columns_for_schema(catalog, schema)
    stats = show_stats(catalog, schema, table)
    row_count = _extract_row_count(stats)

    sub = cols_all[cols_all["table_name"] == table][
        ["column_name", "data_type", "ordinal_position"]
    ].copy()

    # Initial from SHOW STATS
    sub["nulls_fraction"] = sub["column_name"].apply(lambda c: _extract_column_stat(stats, c, "nulls_fraction"))
    sub["distinct_count"] = sub["column_name"].apply(lambda c: _extract_column_stat(stats, c, "number_of_distinct_values"))
    sub["source_distinct"] = sub["distinct_count"].apply(lambda v: "stats" if pd.notna(v) else pd.NA)

    # Derive nulls_count if row_count known
    if row_count is not None:
        frac = sub["nulls_fraction"].clip(lower=0, upper=1)
        sub["nulls_fraction"] = frac
        sub["nulls_count"] = (frac.fillna(0) * row_count).round().astype("Int64")
    else:
        sub["nulls_count"] = pd.NA

    # Identify missing pieces
    need_nulls_cols = sub.loc[sub["nulls_count"].isna(), "column_name"].tolist()
    need_distinct_cols = sub.loc[sub["distinct_count"].isna(), "column_name"].tolist()

    # Optional fallback SQL to fill missing stats
    if allow_fallback_sql and (need_nulls_cols or need_distinct_cols):
        sql = _build_missing_aggs_sql(
            f"{qident(catalog)}.{qident(schema)}.{qident(table)}",
            need_nulls_cols,
            need_distinct_cols,
            prefer_exact_distinct=prefer_exact_distinct
        )
        if sql:
            try:
                rows, cols = run_sql(sql)
                if rows:
                    agg = dict(zip(cols, rows[0]))
                    if need_nulls_cols:
                        for col in need_nulls_cols:
                            v = agg.get(f"n__{col}", None)
                            if v is not None:
                                sub.loc[sub["column_name"] == col, "nulls_count"] = int(v)
                                if row_count is None:
                                    try:
                                        rc_rows, _ = run_sql(f"SELECT COUNT(*) AS cnt FROM {qident(catalog)}.{qident(schema)}.{qident(table)}")
                                        row_count = int(rc_rows[0][0]) if rc_rows else None
                                    except Exception:
                                        row_count = None
                                if row_count:
                                    sub.loc[sub["column_name"] == col, "nulls_fraction"] = float(v) / row_count
                    if need_distinct_cols:
                        for col in need_distinct_cols:
                            v = agg.get(f"d__{col}", None)
                            if v is not None:
                                sub.loc[sub["column_name"] == col, "distinct_count"] = int(v)
                                sub.loc[sub["column_name"] == col, "source_distinct"] = "exact" if prefer_exact_distinct else "approx"
            except Exception:
                pass

    # Normalization / repair for inconsistencies
    if correct_inconsistent and row_count is not None:
        sub["nulls_fraction"] = sub["nulls_fraction"].clip(lower=0, upper=1)
        sub["nulls_count"] = sub["nulls_count"].clip(lower=0, upper=row_count).astype("Int64")

        mask_bad = (sub["distinct_count"].notna()) & (sub["distinct_count"] > row_count)
        bad_cols = sub.loc[mask_bad, "column_name"].tolist()

        if bad_cols and allow_fallback_sql and prefer_exact_distinct:
            select_items = [f"COUNT(DISTINCT {qident(c)}) AS ed__{c}" for c in bad_cols]
            sql = f"SELECT {', '.join(select_items)} FROM {qident(catalog)}.{qident(schema)}.{qident(table)}"
            try:
                rows, cols = run_sql(sql)
                if rows:
                    agg = dict(zip(cols, rows[0]))
                    for c in bad_cols:
                        val = agg.get(f"ed__{c}", None)
                        if val is not None:
                            sub.loc[sub["column_name"] == c, "distinct_count"] = int(val)
                            sub.loc[sub["column_name"] == c, "source_distinct"] = "exact"
            except Exception:
                pass

        sub.loc[(sub["distinct_count"].notna()) & (sub["distinct_count"] > row_count), "distinct_count"] = row_count
        sub.loc[sub["source_distinct"].isna() & sub["distinct_count"].notna(), "source_distinct"] = "derived"

    sub["distinct_count"] = sub["distinct_count"].astype("Int64")
    sub = sub.sort_values("ordinal_position").reset_index(drop=True)
    return sub

@cache_data(ttl=3600, show_spinner=False)
def tables_with_rowcounts(catalog: str, schema: str, table_names: List[str], allow_fallback_sql: bool) -> pd.DataFrame:
    records = []
    for t in table_names:
        rc = table_row_count(catalog, schema, t, allow_fallback_sql=allow_fallback_sql)
        records.append({"table_name": t, "row_count": rc})
    return pd.DataFrame(records)

# =========================
# Search helpers
# =========================
def search_keyword(cols_df: pd.DataFrame, query: str, whole_word: bool = False) -> pd.DataFrame:
    if not query.strip():
        return cols_df.iloc[0:0].copy()
    pattern = rf"\b{re.escape(query)}\b" if whole_word else re.escape(query)
    mask = (
        cols_df["table_name"].str.contains(pattern, case=False, regex=True) |
        cols_df["column_name"].str.contains(pattern, case=False, regex=True) |
        cols_df["data_type"].astype(str).str.contains(pattern, case=False, regex=True)
    )
    return cols_df[mask].copy()

def search_by_column(cols_df: pd.DataFrame, col_query: str, exact: bool = False, whole_word: bool = False) -> pd.DataFrame:
    if not col_query.strip():
        return cols_df.iloc[0:0].copy()
    if exact:
        mask = cols_df["column_name"].str.lower() == col_query.strip().lower()
    else:
        pattern = rf"\b{re.escape(col_query)}\b" if whole_word else re.escape(col_query)
        mask = cols_df["column_name"].str.contains(pattern, case=False, regex=True)
    return cols_df[mask].copy()

def build_match_tree_dot(query: str, hits: pd.DataFrame) -> str:
    def q(s: str) -> str: return (s or "").replace('"', '\\"')
    tables = sorted(hits["table_name"].unique().tolist())
    dot = [
        "digraph G {",
        "  rankdir=LR;",
        '  node [shape=box, fontname="Helvetica"];',
        f'  "Q::{q(query)}" [label="🔎 {q(query)}", style=filled, fillcolor="#F1F5F9"];',
    ]
    for t in tables:
        dot.append(f'  "T::{q(t)}" [label="{q(t)}", shape="folder", style=filled, fillcolor="#E3F2FD"];')
        dot.append(f'  "Q::{q(query)}" -> "T::{q(t)}";')
        sub = hits[hits["table_name"] == t][["column_name","data_type"]].drop_duplicates()
        for _, r in sub.iterrows():
            c_label = f'{r["column_name"]}\\n({r["data_type"]})'
            dot.append(f'  "C::{q(t)}::{q(r["column_name"])}" [label="{q(c_label)}", shape="note", style=filled, fillcolor="#FFFDE7"];')
            dot.append(f'  "T::{q(t)}" -> "C::{q(t)}::{q(r["column_name"])}";')
    dot.append("}")
    return "\n".join(dot)

# =========================
# UI Components
# =========================
def sidebar_controls(catalogs: List[str]) -> Dict[str, object]:
    st.sidebar.header("Browse")

    # Initialize catalog / schema safely
    if "catalog" not in st.session_state:
        st.session_state.catalog = catalogs[0]

    catalog = st.sidebar.selectbox("Catalog", catalogs, index=catalogs.index(st.session_state.catalog))

    if catalog != st.session_state.catalog:
        st.session_state.catalog = catalog
        new_schemas = list_schemas(st.session_state.catalog)
        st.session_state.schema = new_schemas[0] if new_schemas else None

    schemas = list_schemas(st.session_state.catalog)
    if not schemas:
        st.sidebar.error(f"No schemas in catalog '{st.session_state.catalog}'.")
        st.stop()

    if ("schema" not in st.session_state) or (st.session_state.schema not in schemas):
        st.session_state.schema = schemas[0]

    schema_idx = schemas.index(st.session_state.schema) if st.session_state.schema in schemas else 0
    schema = st.sidebar.selectbox("Schema", schemas, index=schema_idx)
    if schema != st.session_state.schema:
        st.session_state.schema = schema

    st.sidebar.header("Filter & Preview")
    table_filter = st.sidebar.text_input("Filter tables (contains)", value="")
    preview_limit = st.sidebar.slider("Preview rows", PREVIEW_MIN, PREVIEW_MAX, PREVIEW_DEFAULT, PREVIEW_STEP)
    show_graph = st.sidebar.checkbox("Show graph view", value=True)

    st.sidebar.header("Stats & Cache")
    use_show_stats = st.sidebar.checkbox("Use SHOW STATS (recommended)", value=True)
    allow_fallback_sql = st.sidebar.checkbox(
        "Allow SQL fallbacks (COUNT/approx_distinct)", value=False,
        help="May scan data. Use only if SHOW STATS is unavailable or incomplete."
    )
    correct_inconsistent = st.sidebar.checkbox("Correct inconsistent stats (clip to row_count)", value=True)
    prefer_exact_distinct = st.sidebar.checkbox(
        "Prefer exact DISTINCT when fixing", value=False,
        help="If enabled with fallbacks, uses COUNT(DISTINCT col) for offending columns."
    )

    warm_on_load = st.sidebar.checkbox("Warm cache on load (this schema)", value=True)
    warm_limit = st.sidebar.slider("Max tables to warm", 5, 200, 30, 5)

    st.sidebar.header("Metadata Search")
    search_mode = st.sidebar.radio("Mode", ["Column name", "Keyword"], horizontal=True)
    q = st.sidebar.text_input("Search", placeholder="e.g., account_id, email, date")
    exact = False
    whole_word = False
    if search_mode == "Column name":
        exact = st.sidebar.checkbox("Exact column name", value=False)
        whole_word = st.sidebar.checkbox("Whole word", value=False)
    else:
        whole_word = st.sidebar.checkbox("Whole word", value=False)

    return dict(
        table_filter=table_filter,
        preview_limit=preview_limit,
        show_graph=show_graph,
        search_mode=search_mode,
        query=q,
        exact=exact,
        whole_word=whole_word,
        use_show_stats=use_show_stats,
        allow_fallback_sql=allow_fallback_sql,
        correct_inconsistent=correct_inconsistent,
        prefer_exact_distinct=prefer_exact_distinct,
        warm_on_load=warm_on_load,
        warm_limit=warm_limit,
    )

def kpis(catalogs: List[str], schemas: List[str], tables: List[str]) -> None:
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Catalogs", len(catalogs))
    with c2: st.metric("Schemas in catalog", len(schemas))
    with c3: st.metric("Tables in schema", len(tables))

def table_browser(catalog: str, schema: str, tables_all: List[str], table_filter: str, allow_fallback_sql: bool) -> Tuple[Optional[str], pd.DataFrame]:
    tables_shown = [t for t in tables_all if table_filter.lower() in t.lower()] if table_filter else tables_all

    # row_count column (cached)
    rc_df = tables_with_rowcounts(catalog, schema, tables_shown, allow_fallback_sql)
    tbl_df = pd.DataFrame({"table_name": tables_shown}).merge(rc_df, on="table_name", how="left")
    tbl_df = tbl_df.sort_values(["table_name"]).reset_index(drop=True)

    left, _right = st.columns([1.15, 1.85], gap="large")
    with left:
        st.subheader("Tables")
        st.dataframe(tbl_df, use_container_width=True, height=460)
        selected_table = st.selectbox("Select table", tables_shown, index=0 if tables_shown else None) if tables_shown else None

    # Debug peek to verify metadata is flowing
    with st.expander("Debug: raw columns for selected table"):
        try:
            _raw = list_columns_for_table(catalog, schema, selected_table) if selected_table else pd.DataFrame()
            st.dataframe(_raw, use_container_width=True)
        except Exception as e:
            st.write(f"Error loading columns: {e}")

    return selected_table, tbl_df

def column_panel(
    catalog: str,
    schema: str,
    selected_table: Optional[str],
    preview_limit: int,
    allow_fallback_sql: bool,
    correct_inconsistent: bool,
    prefer_exact_distinct: bool,
) -> None:
    st.subheader("Columns / Profiling")
    if not selected_table:
        st.info("No table selected.")
        return

    profile_df = column_profile(
        catalog, schema, selected_table,
        allow_fallback_sql=allow_fallback_sql,
        correct_inconsistent=correct_inconsistent,
        prefer_exact_distinct=prefer_exact_distinct,
    )

    if profile_df.empty:
        st.warning(
            "No column metadata available. Possible reasons:\n"
            "• You don’t have permission to DESCRIBE/SHOW COLUMNS for this table.\n"
            "• The connector doesn’t return standard headers.\n"
            "• This is a special object (e.g., function, materialized view without describe support).\n\n"
            "Tip: Try a different table, or enable SQL fallbacks to gather basic stats."
        )
        return

    ordered = profile_df[
        ["ordinal_position", "column_name", "data_type", "nulls_count", "nulls_fraction", "distinct_count", "source_distinct"]
    ].copy()
    st.dataframe(ordered, use_container_width=True, height=340, hide_index=True)
    st.caption(f"FQN: {catalog}.{schema}.{selected_table}")

    # Preview & download
    if st.button("Preview data"):
        data = preview_table(catalog, schema, selected_table, preview_limit)
        st.dataframe(data, use_container_width=True, height=340)
        if not data.empty:
            buf = io.StringIO()
            data.to_csv(buf, index=False)
            st.download_button("Download CSV", data=buf.getvalue(), file_name=f"{selected_table}_preview.csv", mime="text/csv")

def graph_panel(catalog: str, schema: str, tables_shown: List[str]) -> None:
    st.subheader("Graph view")
    def esc(s: str) -> str: return s.replace('"', '\\"')
    cat_label, sch_label = f"Catalog: {catalog}", f"Schema: {schema}"
    dot = [
        "digraph G {", "  rankdir=LR;", '  node [shape=box, fontname="Helvetica"];',
        f'  "C::{esc(cat_label)}" [label="{esc(cat_label)}", style=filled, fillcolor="#E8F5E9"];',
        f'  "S::{esc(sch_label)}" [label="{esc(sch_label)}", style=filled, fillcolor="#E3F2FD"];',
        f'  "C::{esc(cat_label)}" -> "S::{esc(sch_label)}";'
    ]
    for t in tables_shown[:MAX_GRAPH_TABLES]:
        t_label = f"Table: {t}"
        dot.append(f'  "T::{esc(t_label)}" [label="{esc(t_label)}", shape=note, style=filled, fillcolor="#FFFDE7"];')
        dot.append(f'  "S::{esc(sch_label)}" -> "T::{esc(t_label)}";')
    dot.append("}")
    st.graphviz_chart("\n".join(dot), use_container_width=True)

def column_search_panel(catalog: str, schema: str, query: str, exact: bool, whole_word: bool) -> None:
    st.subheader("🔎 Column-name Search")
    cols_df = list_columns_for_schema(catalog, schema)

    if not query:
        st.info("Enter a column name (e.g., `account_id`) to see all tables containing it.")
        return

    hits = search_by_column(cols_df, query, exact=exact, whole_word=whole_word)
    if hits.empty:
        st.warning("No tables contain a column matching that query.")
        return

    agg = hits.groupby("table_name").size().reset_index(name="matching_columns")
    agg = agg.sort_values(["matching_columns", "table_name"], ascending=[False, True])
    st.dataframe(agg, use_container_width=True, hide_index=True)

    for _, row in agg.iterrows():
        t = row["table_name"]
        with st.expander(f"🗂️ {t} — {row['matching_columns']} match(es)"):
            sub = hits[hits["table_name"] == t][["column_name", "data_type"]].drop_duplicates()
            st.dataframe(sub, use_container_width=True, hide_index=True)

    try:
        dot = build_match_tree_dot(query, hits)
        st.graphviz_chart(dot, use_container_width=True)
    except Exception:
        st.info("Graph view unavailable; showing list instead.")
        for t, sub in hits.groupby("table_name"):
            st.write(f"- **{t}**")
            for _, r in sub.iterrows():
                st.write(f"  - {r['column_name']} ({r['data_type']})")

def keyword_search_panel(catalog: str, schema: str, query: str, whole_word: bool) -> None:
    st.subheader("🔎 Keyword Search (table/column/type)")
    cols_df = list_columns_for_schema(catalog, schema)

    if not query:
        st.info("Enter keywords to search table names, column names, and data types.")
        return

    hits = search_keyword(cols_df, query, whole_word=whole_word)
    if hits.empty:
        st.warning("No matches found.")
        return

    st.dataframe(hits, use_container_width=True, hide_index=True)

    try:
        dot = build_match_tree_dot(query, hits)
        st.graphviz_chart(dot, use_container_width=True)
    except Exception:
        st.info("Graph view unavailable; showing list instead.")
        for t, sub in hits.groupby("table_name"):
            st.write(f"- **{t}**")
            for _, r in sub.iterrows():
                st.write(f"  - {r['column_name']} ({r['data_type']})")

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

-- stats (cached)
SHOW STATS FOR {st.session_state.catalog}.{st.session_state.schema}.<table_name>;

-- preview
SELECT * FROM {st.session_state.catalog}.{st.session_state.schema}.<selected_table> LIMIT {preview_limit};"""
        )

# =========================
# Cache warm-up
# =========================
def warm_cache_for_schema(
    catalog: str,
    schema: str,
    tables: List[str],
    limit: int,
    allow_fallback_sql: bool,
    correct_inconsistent: bool,
    prefer_exact_distinct: bool,
) -> None:
    to_warm = tables[:limit]
    if not to_warm:
        return
    progress = st.progress(0.0, text=f"Warming cache for {schema} …")
    for i, t in enumerate(to_warm, start=1):
        # Precompute & cache row_count and full profile
        try:
            _ = table_row_count(catalog, schema, t, allow_fallback_sql=allow_fallback_sql)
        except Exception:
            pass
        try:
            _ = column_profile(
                catalog, schema, t,
                allow_fallback_sql=allow_fallback_sql,
                correct_inconsistent=correct_inconsistent,
                prefer_exact_distinct=prefer_exact_distinct,
            )
        except Exception:
            pass
        progress.progress(i / len(to_warm), text=f"Warming cache: {i}/{len(to_warm)} ({t})")
    progress.empty()
    st.success(f"Cache warmed for first {len(to_warm)} table(s) in {schema}.")

# =========================
# App Bootstrap
# =========================
def main():
    apply_styles()

    # Ping
    try:
        _ = run_sql("SELECT 1")
        st.success("Connected")
    except Exception as e:
        st.error(f"Connection failed: {e}")
        st.stop()

    # ---------- Initialization (robust) ----------
    catalogs = list_catalogs()
    if not catalogs:
        st.error("No catalogs visible (after filtering system catalogs).")
        st.stop()

    # Need a valid starting point?
    need_pick = ("catalog" not in st.session_state) or (st.session_state.catalog not in catalogs)
    if need_pick:
        cat, sch = pick_first_usable_location(catalogs)
        if not cat or not sch:
            st.error("No schemas available in any visible catalog. Check permissions/catalog setup.")
            st.stop()
        st.session_state.catalog = cat
        st.session_state.schema  = sch

    # Validate selection in case permissions changed
    if st.session_state.catalog not in catalogs:
        cat, sch = pick_first_usable_location(catalogs)
        if not cat or not sch:
            st.error("No schemas available in any visible catalog. Check permissions/catalog setup.")
            st.stop()
        st.session_state.catalog = cat
        st.session_state.schema  = sch

    current_schemas = list_schemas(st.session_state.catalog)
    usable_schemas = [s for s in current_schemas if is_allowed_schema(s)]

    if not usable_schemas:
        # Try moving to another catalog that has schemas
        cat, sch = pick_first_usable_location(catalogs)
        if not cat or not sch:
            st.error(
                f"No schemas in any visible catalog (after filtering info schemas). "
                f"Last attempted catalog: '{st.session_state.catalog}'."
            )
            st.stop()
        st.session_state.catalog = cat
        st.session_state.schema  = sch
        usable_schemas = [sch]

    if ("schema" not in st.session_state) or (st.session_state.schema not in usable_schemas):
        st.session_state.schema = usable_schemas[0]

    # Sidebar
    controls = sidebar_controls(catalogs)

    # Warm cache on load (only once per catalog+schema+settings key)
    warm_key = f"warm::{st.session_state.catalog}::{st.session_state.schema}::af={controls['allow_fallback_sql']}::fix={controls['correct_inconsistent']}::ex={controls['prefer_exact_distinct']}::lim={controls['warm_limit']}"
    if controls["warm_on_load"] and not st.session_state.get(warm_key, False):
        tables_all_for_warm = list_tables(st.session_state.catalog, st.session_state.schema)
        warm_cache_for_schema(
            st.session_state.catalog,
            st.session_state.schema,
            tables_all_for_warm,
            limit=controls["warm_limit"],
            allow_fallback_sql=controls["allow_fallback_sql"] if controls["use_show_stats"] else False,
            correct_inconsistent=controls["correct_inconsistent"],
            prefer_exact_distinct=controls["prefer_exact_distinct"],
        )
        st.session_state[warm_key] = True

    # Main lists
    schemas = list_schemas(st.session_state.catalog)
    tables_all = list_tables(st.session_state.catalog, st.session_state.schema)
    kpis(catalogs, schemas, tables_all)

    # Tables + row_count + selection
    selected_table, tbl_df = table_browser(
        st.session_state.catalog,
        st.session_state.schema,
        tables_all,
        controls["table_filter"],
        allow_fallback_sql=controls["allow_fallback_sql"] if controls["use_show_stats"] else False
    )

    # Column profiling for selected table (cached)
    column_panel(
        st.session_state.catalog,
        st.session_state.schema,
        selected_table,
        controls["preview_limit"],
        allow_fallback_sql=controls["allow_fallback_sql"] if controls["use_show_stats"] else False,
        correct_inconsistent=controls["correct_inconsistent"],
        prefer_exact_distinct=controls["prefer_exact_distinct"],
    )

    # Optional graph of Catalog -> Schema -> Tables
    if controls["show_graph"]:
        graph_panel(st.session_state.catalog, st.session_state.schema, tbl_df["table_name"].tolist())

    # Metadata search GUI (using SHOW COLUMNS cache)
    if controls["search_mode"] == "Column name":
        column_search_panel(
            st.session_state.catalog, st.session_state.schema,
            controls["query"], controls["exact"], controls["whole_word"]
        )
    else:
        keyword_search_panel(
            st.session_state.catalog, st.session_state.schema,
            controls["query"], controls["whole_word"]
        )

    sql_footer(controls["preview_limit"])

if __name__ == "__main__":
    main()

