import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import warnings
import re

# --------------------------------------------------
# OPTIONAL: suppress macOS SSL warning
# --------------------------------------------------
warnings.filterwarnings(
    "ignore",
    message="urllib3 v2 only supports OpenSSL"
)

# --------------------------------------------------
# STARBURST GALAXY CONFIG
# --------------------------------------------------
HOST = "talktodata-free-cluster.trino.galaxy.starburst.io"
PORT = 443

# ⚠️ DEMO ONLY – move to st.secrets in real usage
USER_RAW = "rahul.yaksh@bofa.com/accountadmin"
PASSWORD_RAW = "Aarush2011@"

LLM_ID = "gpt4min"

DEFAULT_CATALOG = "tpch"
DEFAULT_SCHEMA = "sf1"

USER = quote_plus(USER_RAW)
PASSWORD = quote_plus(PASSWORD_RAW)

# --------------------------------------------------
# SQLALCHEMY ENGINE
# --------------------------------------------------
@st.cache_resource
def get_engine():
    url = (
        f"trino://{USER}:{PASSWORD}@{HOST}:{PORT}/"
        f"{DEFAULT_CATALOG}/{DEFAULT_SCHEMA}"
        f"?http_scheme=https"
    )
    return create_engine(url)

engine = get_engine()

# --------------------------------------------------
# SQL EXTRACTION & VALIDATION (FIXED)
# --------------------------------------------------
def extract_select_sql(text: str):
    """
    Extract the first SELECT statement from LLM output.
    """
    if not text:
        return None

    cleaned = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE).strip()

    match = re.search(
        r"(select\s+.*?)(;|$)",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL
    )

    if not match:
        return None

    return match.group(1).strip()


def is_valid_analytics_sql(sql: str) -> bool:
    """
    Semantic (not brittle) analytics validation.
    """
    s = sql.lower()

    # Must be SELECT
    if not s.startswith("select"):
        return False

    # Must read from tpch.tiny tables
    if not re.search(r"\bfrom\s+tpch\.tiny\.", s):
        return False

    # Block VALUES-only or dummy queries
    if re.search(r"\bvalues\s*\(", s):
        return False

    # Block literal-only selects
    if re.match(r"select\s+['\"\d]", s):
        return False

    # Block schema listing artifacts
    if re.search(r"select\s+distinct\s+table\b", s):
        return False

    return True

# --------------------------------------------------
# STREAMLIT UI
# --------------------------------------------------
st.set_page_config(page_title="Talk to Data – Starburst AI (RAG)", layout="wide")
st.title("💬 Talk to Your Data (Starburst AI + RAG)")

st.caption(
    "Ask questions → Catalog RAG → AI reasoning → Governed execution"
)

# --------------------------------------------------
# MODE SELECTION
# --------------------------------------------------
mode = st.radio(
    "Choose mode",
    ["Explain (RAG only)", "Execute (Generate SQL + Run)"],
    horizontal=True
)

# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# --------------------------------------------------
# EXAMPLE QUESTIONS
# --------------------------------------------------
st.markdown("### 🔍 Example questions")

examples = [
    "Who are the top 10 customers by revenue?",
    "How do we calculate total revenue per customer?",
    "Which tables are involved in customer revenue analysis?",
    "How concentrated is revenue among top customers?"
]

cols = st.columns(len(examples))
for i, q in enumerate(examples):
    if cols[i].button(q):
        st.session_state["prefill_prompt"] = q

# --------------------------------------------------
# CLEAR CHAT
# --------------------------------------------------
if st.button("🧹 Clear Chat"):
    st.session_state.chat_history = []
    st.rerun()

# --------------------------------------------------
# DISPLAY CHAT HISTORY
# --------------------------------------------------
for role, message in st.session_state.chat_history:
    with st.chat_message(role):
        st.markdown(message)

# --------------------------------------------------
# CHAT INPUT (STREAMLIT SAFE)
# --------------------------------------------------
user_input = st.chat_input("Ask a question about your data...")

if not user_input and "prefill_prompt" in st.session_state:
    user_input = st.session_state.pop("prefill_prompt")

# --------------------------------------------------
# HANDLE USER INPUT
# --------------------------------------------------
if user_input:
    st.session_state.chat_history.append(("user", user_input))
    with st.chat_message("user"):
        st.markdown(user_input)

    escaped_prompt = user_input.replace("'", "''")

    # --------------------------------------------------
    # CATALOG RAG CONTEXT
    # --------------------------------------------------
    catalog_cte = """
    WITH cols AS (
      SELECT
        table_name,
        array_agg(column_name ORDER BY ordinal_position) AS colnames
      FROM tpch.information_schema.columns
      WHERE table_schema = 'tiny'
      GROUP BY table_name
    ),
    ctx AS (
      SELECT
        CAST(
          array_agg(
            CAST(
              json_object(
                KEY 'table' VALUE table_name,
                KEY 'cols'  VALUE json_format(CAST(colnames AS JSON))
              ) AS JSON
            )
          ) AS JSON
        ) AS j
      FROM cols
    )
    """

    # --------------------------------------------------
    # EXPLAIN MODE
    # --------------------------------------------------
    if mode == "Explain (RAG only)":
        ai_sql = f"""
        {catalog_cte}
        SELECT starburst.ai.prompt(
          CONCAT(
            'You are an enterprise analytics assistant. ',
            'Answer using ONLY the TPCH catalog below. ',
            'Do not invent tables or columns. ',
            'Do not write SQL unless asked. ',
            'User question: {escaped_prompt} ',
            'CATALOG JSON: ',
            json_format((SELECT j FROM ctx))
          ),
          '{LLM_ID}'
        ) AS answer
        """

        with st.chat_message("assistant"):
            with st.spinner("Reasoning over catalog..."):
                df = pd.read_sql(ai_sql, engine)
                answer = df.iloc[0, 0]
                st.markdown(answer)
                st.session_state.chat_history.append(("assistant", answer))

    # --------------------------------------------------
    # EXECUTE MODE (FIXED)
    # --------------------------------------------------
    else:
        sql_gen = f"""
        {catalog_cte}
        SELECT starburst.ai.prompt(
          CONCAT(
            'You are an expert Trino SQL analyst. ',
            'Generate ONE executable Trino SELECT query that answers: ',
            '{escaped_prompt}. ',
            'Rules: ',
            '- Query MUST read from tpch.tiny tables ',
            '- Query MUST compute numeric metrics (SUM, COUNT, AVG, etc.) ',
            '- Use joins where appropriate ',
            '- DO NOT use VALUES ',
            '- DO NOT return table names or metadata ',
            '- Return ONLY a single SELECT statement ',
            '- No markdown, no comments ',
            '- Limit to 10 rows ',
            'CATALOG JSON: ',
            json_format((SELECT j FROM ctx))
          ),
          '{LLM_ID}'
        ) AS generated_sql
        """

        with st.chat_message("assistant"):
            with st.spinner("Generating SQL..."):
                raw_sql = pd.read_sql(sql_gen, engine).iloc[0, 0]

            clean_sql = extract_select_sql(raw_sql)

            if not clean_sql:
                st.error("❌ No executable SELECT statement found.")
                with st.expander("Raw LLM output"):
                    st.code(raw_sql)
            elif not is_valid_analytics_sql(clean_sql):
                st.error("❌ Generated SQL is not a valid analytics query.")
                with st.expander("Rejected SQL"):
                    st.code(clean_sql)
            else:
                with st.spinner("Executing query..."):
                    results_df = pd.read_sql(clean_sql, engine)

                st.markdown("### 📊 Results")
                st.dataframe(results_df)

                if results_df.shape[1] >= 2:
                    st.markdown("### 📈 Visualization")
                    st.bar_chart(results_df.set_index(results_df.columns[0]))

                with st.expander("Show generated SQL"):
                    st.code(clean_sql, language="sql")

                st.session_state.chat_history.append(
                    ("assistant", "Query executed successfully.")
                )
