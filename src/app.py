# app_streamlit_no_metadata.py
import streamlit as st
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Any, Dict

from dotenv import load_dotenv
load_dotenv()

# ensure local module import path
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))

# Try import real QA system; keep error for fallback
try:
    from LLM_query_generator import AgriculturalQASystem  # type: ignore
    _import_err = None
except Exception as e:
    AgriculturalQASystem = None  # type: ignore
    _import_err = e

st.set_page_config(page_title="AgroInsight AI (No Metadata)", page_icon="🌾")

st.title("AgroInsight AI — No Metadata Preview")
st.write("Ask questions about Indian agricultural data (data.gov.in).")

# Session state defaults
st.session_state.setdefault("qa_system", None)

# Minimal Mock fallback
class MockQA:
    def __init__(self, db_path: str = "data/agricultural_data.db"):
        self.db_path = db_path
    def answer_question(self, question: str) -> Dict[str, Any]:
        import sqlite3, pandas as pd
        if os.path.exists(self.db_path) and "rice" in question.lower() and "2014" in question:
            sql = "SELECT state, SUM(production) as total_production FROM crop_production WHERE year=2014 AND lower(crop) LIKE '%rice%' GROUP BY state ORDER BY total_production DESC LIMIT 5;"
            conn = sqlite3.connect(self.db_path)
            try:
                df = pd.read_sql_query(sql, conn)
            finally:
                conn.close()
            return {"success": True, "answer": f"Fallback: top rice states (2014):\n\n{df.to_string(index=False)}", "sql_query": sql, "data": df, "metadata": {"tables_used": ["crop_production"], "row_count": len(df)}}
        return {"success": True, "answer": "Fallback: real QA not initialized. Ensure DB and API keys are configured.", "sql_query": None, "data": None, "metadata": {}}

def try_init_real_qa(db_path="data/agricultural_data.db"):
    if AgriculturalQASystem is None:
        raise RuntimeError(f"Cannot import LLM_query_generator: {_import_err}")
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY") or os.getenv("OPENROUTER_API_KEY")
    # Try common constructor patterns
    try:
        return AgriculturalQASystem(db_path=db_path, api_key=api_key, model="gemini-2.0-flash-exp")
    except TypeError:
        pass
    try:
        return AgriculturalQASystem(db_path, api_key)
    except TypeError:
        pass
    try:
        return AgriculturalQASystem(db_path)
    except Exception as e:
        raise RuntimeError(f"Failed to instantiate AgriculturalQASystem: {e}")

def get_qa_system():
    if st.session_state.qa_system is not None:
        return st.session_state.qa_system
    # Try real
    try:
        qa = try_init_real_qa()
        st.session_state.qa_system = qa
        return qa
    except Exception:
        # fallback
        qa = MockQA()
        st.session_state.qa_system = qa
        return qa

# Optional initialize button
if st.button("Initialize QA system"):
    try:
        _ = get_qa_system()
        st.success("QA system initialized (real or fallback).")
    except Exception as e:
        st.error(f"Initialization error: {e}")

# Question input & ask
question = st.text_input("Ask a question (e.g., 'Which state produced the most rice in 2014?')", "")
if st.button("Get Answer") and question.strip():
    qa = get_qa_system()
    if not hasattr(qa, "answer_question"):
        st.error("QA system doesn't expose answer_question(). Check initialization.")
    else:
        with st.spinner("Generating answer..."):
            try:
                result = qa.answer_question(question)
            except Exception as e:
                st.error(f"Runtime error while calling answer_question(): {e}")
                st.stop()
        if not result:
            st.error("No result returned.")
        else:
            st.subheader("Answer")
            st.write(result.get("answer", ""))

            # Keep SQL and data expanders if present, but DO NOT show metadata preview
            if result.get("sql_query"):
                with st.expander("🔍 View SQL Query"):
                    st.code(result["sql_query"], language="sql")

            if result.get("data") is not None:
                try:
                    df = result["data"]
                    if hasattr(df, "head"):
                        with st.expander(f"📊 View Data ({len(df)} rows)" if hasattr(df, "__len__") else "📊 View Data"):
                            st.dataframe(df)
                except Exception:
                    st.write("Could not display data preview.")

# Small footer
st.markdown("---")
st.markdown("<div style='text-align:center; color: #718096;'>AgroInsight AI — metadata preview removed</div>", unsafe_allow_html=True)
