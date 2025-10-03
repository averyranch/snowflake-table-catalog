
# ptest_tabs.py
# Run with: streamlit run ptest_tabs.py
import streamlit as st

# Import the original app's functions
import ptest as app

def main():
    # IMPORTANT: Call the original apply_styles() FIRST so its st.set_page_config runs
    app.apply_styles()

    # Connectivity ping
    try:
        _ = app.run_sql("SELECT 1")
        st.success("Connected to Trino")
    except Exception as e:
        st.error(f"Connection failed: {e}")
        st.stop()

    # Load workspace (catalog/schema) via original sidebar
    catalogs = app.list_catalogs()
    if not catalogs:
        st.error("No catalogs visible (after filtering system catalogs).")
        st.stop()

    controls = app.sidebar_controls(catalogs)

    # Resolve tables for the selected catalog/schema
    tables = app.list_tables(st.session_state.catalog, st.session_state.schema)

    # Tabs across the top
    tab_browse, tab_search, tab_profile, tab_quality, tab_blast = st.tabs(
        ["🗂️ Browse", "🔎 Search", "📊 Profiling", "🧪 Data Quality", "🧨 Blast Radius"]
    )

    # Tab 1: Browse (tables, columns, preview)
    with tab_browse:
        try:
            selected_table, _ = app.table_browser(
                st.session_state.catalog,
                st.session_state.schema,
                tables,
                controls.get("table_filter", ""),
                controls.get("allow_fallback_sql", True),
            )
        except TypeError:
            # Backward compatibility if signature differs
            selected_table, _ = app.table_browser(
                st.session_state.catalog,
                st.session_state.schema,
                tables,
                controls.get("allow_fallback_sql", True),
            )
        # Keep selection in session for other tabs
        if selected_table:
            st.session_state["selected_table"] = selected_table
        else:
            selected_table = st.session_state.get("selected_table")

    # Tab 2: Metadata Search
    with tab_search:
        app.metadata_search_panel(
            st.session_state.catalog,
            st.session_state.schema,
            controls.get("blast_workers", 4)  # reuse worker count
        )

    # Tab 3: Column Profiling
    with tab_profile:
        app.profiling_panel(
            st.session_state.catalog,
            st.session_state.schema,
            selected_table,
            controls.get("allow_fallback_sql", True),
            controls.get("correct_inconsistent", True),
            controls.get("prefer_exact_distinct", False),
            controls.get("preview_limit", 50),
        )

    # Tab 4: Full-table Data Quality
    with tab_quality:
        app.data_quality_panel(
            st.session_state.catalog,
            st.session_state.schema,
            selected_table,
            controls.get("dq_include_distinct", True),
            controls.get("prefer_exact_distinct", False),
            controls.get("dq_include_minmax", True),
        )

    # Tab 5: Blast Radius Studio
    with tab_blast:
        app.blast_radius_studio(
            st.session_state.catalog,
            st.session_state.schema,
            selected_table,
            method=controls.get("blast_method", "metadata"),
            topk=controls.get("blast_topk", 12),
            workers=controls.get("blast_workers", 4),
        )

    # Footer with sample SQL (from original app)
    app.sql_footer(controls.get("preview_limit", 50))

if __name__ == "__main__":
    main()
