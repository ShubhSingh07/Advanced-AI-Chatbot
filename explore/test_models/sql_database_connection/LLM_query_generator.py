"""
Streamlit-based SQL QnA Chatbot
Clean, simple, and perfect for your use case
"""
import sys
import streamlit as st
import sqlite3
from typing import Optional

# Check if running with streamlit
try:
    from streamlit.runtime.scriptrunner import get_script_run_ctx
    if get_script_run_ctx() is None:
        print("\n" + "="*80)
        print("❌ ERROR: This is a Streamlit app!")
        print("="*80)
        print("\nYou must run it with: streamlit run src/app.py")
        print("\nDo NOT use: python src/app.py")
        print("="*80 + "\n")
        sys.exit(1)
except ImportError:
    print("❌ Streamlit not installed! Install with: pip install streamlit")
    sys.exit(1)

# Configure page
st.set_page_config(
    page_title="SQL QnA Chatbot",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state for chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

if "db_path" not in st.session_state:
    st.session_state.db_path = None


# ====== BACKEND FUNCTIONS ======

def generate_sql_query(question: str, db_path: str) -> str:
    """
    AI Agent: Generate SQL query from natural language question
    
    TODO: Replace with your actual AI agent (LangChain, OpenAI, etc.)
    """
    # Placeholder - replace with your SQL AI agent
    question_lower = question.lower()
    
    if "show" in question_lower and "table" in question_lower:
        return "SELECT name FROM sqlite_master WHERE type='table';"
    elif "count" in question_lower or "how many" in question_lower:
        return "SELECT COUNT(*) as count FROM api_data LIMIT 10;"
    elif "select" in question_lower:
        # If user writes SQL directly, use it
        return question
    else:
        return "SELECT * FROM api_data LIMIT 10;"


def execute_sql_query(query: str, db_path: str) -> tuple[bool, str, list]:
    """
    Execute SQL query on the database
    
    Returns:
        (success, message, results)
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        conn.close()
        
        return True, f"Query executed successfully. Found {len(results)} rows.", (columns, results)
    
    except Exception as e:
        return False, f"Error executing query: {str(e)}", []


def format_query_results(columns: list, results: list, max_rows: int = 10) -> str:
    """
    Format SQL results into readable text
    """
    if not results:
        return "No results found."
    
    # Limit results
    results = results[:max_rows]
    
    # Create formatted response
    response = f"Found {len(results)} rows:\n\n"
    
    # Add table header
    response += "| " + " | ".join(columns) + " |\n"
    response += "|" + "|".join(["---" for _ in columns]) + "|\n"
    
    # Add rows
    for row in results:
        response += "| " + " | ".join(str(cell) for cell in row) + " |\n"
    
    if len(results) >= max_rows:
        response += f"\n... (showing first {max_rows} rows)"
    
    return response


def process_question(question: str, db_path: str) -> str:
    """
    Main processing pipeline:
    1. Question -> AI Agent -> SQL Query
    2. SQL Query -> Database -> Results
    3. Results -> Formatted Answer
    """
    # Step 1: Generate SQL query
    with st.spinner("🤔 Generating SQL query..."):
        sql_query = generate_sql_query(question, db_path)
    
    # Show the generated query
    st.info(f"**Generated SQL Query:**\n```sql\n{sql_query}\n```")
    
    # Step 2: Execute query
    with st.spinner("🔍 Executing query..."):
        success, message, results = execute_sql_query(sql_query, db_path)
    
    if not success:
        return f"❌ {message}"
    
    # Step 3: Format results
    if results:
        columns, rows = results
        formatted_results = format_query_results(columns, rows)
        return f"✅ {message}\n\n{formatted_results}"
    else:
        return message


# ====== STREAMLIT UI ======

# Sidebar for configuration
with st.sidebar:
    st.title("⚙️ Configuration")
    
    # Database upload/connection
    st.subheader("Database Connection")
    
    uploaded_file = st.file_uploader(
        "Upload SQLite Database",
        type=['db', 'sqlite', 'sqlite3'],
        help="Upload your SQLite database file"
    )
    
    if uploaded_file:
        # Save uploaded file temporarily
        db_path = f"temp_{uploaded_file.name}"
        with open(db_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.session_state.db_path = db_path
        st.success(f"✅ Database loaded: {uploaded_file.name}")
    
    # Or use existing database path
    manual_path = st.text_input(
        "Or enter database path:",
        value="/Users/shubh/Project/RAG-Projects/Advanced-AI-Chatbot/data/api_data.py/sql/crop_production.db"
    )
    
    if st.button("Connect to Database"):
        import os
        if os.path.exists(manual_path):
            st.session_state.db_path = manual_path
            st.success(f"✅ Connected to: {manual_path}")
        else:
            st.error(f"❌ Database not found: {manual_path}")
    
    st.divider()
    
    # Clear chat button
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.rerun()
    
    st.divider()
    
    # Info section
    st.subheader("ℹ️ How to Use")
    st.markdown("""
    1. Upload or connect to a database
    2. Ask questions in natural language
    3. The AI will generate SQL queries
    4. See results instantly!
    
    **Example questions:**
    - "Show all tables"
    - "How many records are there?"
    - "Show me the first 10 rows"
    """)


# Main chat interface
st.title("🤖 SQL QnA Chatbot")
st.markdown("Ask questions about your data in natural language!")

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask a question about your data..."):
    # Check if database is connected
    if not st.session_state.db_path:
        st.error("⚠️ Please connect to a database first!")
    else:
        # Add user message to chat
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Generate response
        with st.chat_message("assistant"):
            response = process_question(prompt, st.session_state.db_path)
            st.markdown(response)
        
        # Add assistant response to chat
        st.session_state.messages.append({"role": "assistant", "content": response})


# Display database info if connected
if st.session_state.db_path:
    with st.expander("📊 Database Information"):
        try:
            conn = sqlite3.connect(st.session_state.db_path)
            cursor = conn.cursor()
            
            # Get table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            st.write("**Available Tables:**")
            for table in tables:
                st.write(f"- {table[0]}")
            
            conn.close()
        except Exception as e:
            st.error(f"Error reading database: {e}")


# Footer
st.divider()
st.caption("💡 Tip: Try asking 'Show me all tables' to get started!")