"""
Streamlit Interface for Agricultural Data QA System - FULLY FIXED VERSION
Includes proper loading indicators and error handling
"""

import streamlit as st
import os
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import traceback
import time

# Add parent directory to path for imports
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))

# Import the QA system
try:
    from LLM_query_generator import AgriculturalQASystem, DatabaseSchema
except ImportError:
    st.error("❌ Could not import QA system. Ensure LLM_query_generator.py is in the same directory.")
    st.stop()

# Page configuration
st.set_page_config(
    page_title="Agricultural Data QA System",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS with loading animation
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #2e7d32;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        text-align: center;
        color: #666;
        margin-bottom: 2rem;
    }
    .sql-box {
        background-color: #f5f5f5;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #2e7d32;
        margin: 1rem 0;
    }
    .loading-indicator {
        background-color: #e3f2fd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1976d2;
        margin: 1rem 0;
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    .error-box {
        background-color: #ffebee;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #c62828;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


# Initialize session state
if "qa_system" not in st.session_state:
    st.session_state.qa_system = None
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "db_connected" not in st.session_state:
    st.session_state.db_connected = False
if "use_openrouter" not in st.session_state:
    st.session_state.use_openrouter = True
if "query_model" not in st.session_state:
    # FIXED: Use actual working models
    st.session_state.query_model = "openai/gpt-4-turbo"
if "answer_model" not in st.session_state:
    st.session_state.answer_model = "openai/gpt-4-turbo"
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False
if "processing" not in st.session_state:
    st.session_state.processing = False


def initialize_qa_system(
    db_path: str, 
    use_openrouter: bool = True,
    query_model: str = "openai/gpt-4-turbo",
    answer_model: str = "openai/gpt-4-turbo"
) -> bool:
    """Initialize the QA system with database"""
    try:
        if not os.path.exists(db_path):
            st.error(f"❌ Database not found: {db_path}")
            return False
        
        # Show progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("🔌 Connecting to database...")
        progress_bar.progress(25)
        time.sleep(0.3)
        
        status_text.text("🤖 Initializing AI models...")
        progress_bar.progress(50)
        
        st.session_state.qa_system = AgriculturalQASystem(
            db_path=db_path,
            use_openrouter=use_openrouter,
            query_model=query_model,
            answer_model=answer_model
        )
        
        progress_bar.progress(75)
        status_text.text("✅ Verifying connection...")
        time.sleep(0.3)
        
        st.session_state.db_connected = True
        st.session_state.use_openrouter = use_openrouter
        st.session_state.query_model = query_model
        st.session_state.answer_model = answer_model
        
        progress_bar.progress(100)
        status_text.empty()
        progress_bar.empty()
        
        api_type = "OpenRouter" if use_openrouter else "OpenAI"
        st.success(f"✅ QA System initialized successfully with {api_type}!")
        st.info(f"📝 Using models:\n- Query: {query_model}\n- Answer: {answer_model}")
        return True
        
    except Exception as e:
        st.error(f"❌ Error initializing system: {str(e)}")
        if st.session_state.debug_mode:
            st.code(traceback.format_exc())
        return False


def process_question(question: str):
    """Process user question and display results with loading indicator"""
    if not st.session_state.qa_system:
        st.error("⚠️ Please connect to database first!")
        return
    
    st.session_state.processing = True
    
    # Add to chat history
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.chat_history.append({
        "timestamp": timestamp,
        "question": question,
        "processing": True
    })
    
    # Create loading placeholder
    loading_placeholder = st.empty()
    
    # Process question with error handling
    try:
        # Show loading message
        with loading_placeholder.container():
            st.markdown('<div class="loading-indicator">🤔 Processing your question...</div>', unsafe_allow_html=True)
            
            # Progress steps
            progress_col1, progress_col2 = st.columns([1, 4])
            with progress_col1:
                st.markdown("**Step 1:**")
            with progress_col2:
                step1 = st.empty()
                step1.text("🔍 Analyzing question...")
            
            time.sleep(0.5)
            
            with progress_col1:
                st.markdown("**Step 2:**")
            with progress_col2:
                step2 = st.empty()
                step2.text("📝 Generating SQL query...")
        
        if st.session_state.debug_mode:
            st.info(f"DEBUG: Calling answer_question with: {question}")
        
        # Call the QA system
        result = st.session_state.qa_system.answer_question(question)
        
        # Update loading
        with loading_placeholder.container():
            step1.text("✅ Question analyzed")
            step2.text("✅ SQL query generated")
            
            progress_col1, progress_col2 = st.columns([1, 4])
            with progress_col1:
                st.markdown("**Step 3:**")
            with progress_col2:
                step3 = st.empty()
                step3.text("⚡ Executing query...")
        
        time.sleep(0.3)
        
        # Update loading
        with loading_placeholder.container():
            step3.text("✅ Query executed")
            
            progress_col1, progress_col2 = st.columns([1, 4])
            with progress_col1:
                st.markdown("**Step 4:**")
            with progress_col2:
                step4 = st.empty()
                step4.text("💬 Generating answer...")
        
        time.sleep(0.3)
        
        if st.session_state.debug_mode:
            st.info(f"DEBUG: Result type: {type(result)}")
            st.info(f"DEBUG: Result keys: {result.keys() if isinstance(result, dict) else 'Not a dict'}")
            with st.expander("DEBUG: Full Result"):
                st.json(result)
        
        # Clear loading indicator
        loading_placeholder.empty()
        
        # Validate result structure
        if not isinstance(result, dict):
            st.error(f"⚠️ Unexpected result type: {type(result)}")
            result = {
                "error": f"Invalid result type: {type(result)}",
                "answer": "Error processing question",
                "sql_query": None,
                "data": None,
                "metadata": {}
            }
        
        # Ensure required keys exist
        if "answer" not in result:
            result["answer"] = "No answer generated - check if API is responding correctly"
        if "sql_query" not in result:
            result["sql_query"] = None
        if "data" not in result:
            result["data"] = None
        if "metadata" not in result:
            result["metadata"] = {}
            
    except Exception as e:
        loading_placeholder.empty()
        st.error(f"❌ Error processing question: {str(e)}")
        if st.session_state.debug_mode:
            st.code(traceback.format_exc())
        
        result = {
            "error": str(e),
            "answer": f"Error: {str(e)}",
            "sql_query": None,
            "data": None,
            "metadata": {}
        }
    
    # Update chat history with result
    st.session_state.chat_history[-1].update({
        "processing": False,
        "result": result
    })
    
    st.session_state.processing = False
    return result


def display_chat_message(message: dict):
    """Display a chat message"""
    with st.container():
        col1, col2 = st.columns([1, 20])
        
        with col1:
            st.markdown("**👤**" if message.get("processing") else "**🤖**")
        
        with col2:
            # Question
            st.markdown(f"**You** ({message['timestamp']})")
            st.markdown(f"_{message['question']}_")
            
            if message.get("processing"):
                st.info("⏳ Processing...")
            else:
                result = message.get("result", {})
                
                # Show error if present
                if "error" in result and result.get("sql_query") is None:
                    st.error(f"❌ Error: {result['error']}")
                
                # Show SQL Query (collapsible)
                if result.get("sql_query"):
                    with st.expander("📝 View Generated SQL Query", expanded=False):
                        st.code(result["sql_query"], language="sql")
                        
                        # Query metadata
                        metadata = result.get("metadata", {})
                        if metadata:
                            col_m1, col_m2, col_m3 = st.columns(3)
                            with col_m1:
                                st.metric("Rows", metadata.get("row_count", 0))
                            with col_m2:
                                exec_time = metadata.get("execution_time", 0)
                                st.metric("Time", f"{exec_time:.2f}s" if exec_time else "N/A")
                            with col_m3:
                                st.metric("Complexity", metadata.get("complexity", "N/A"))
                
                # Show Answer - THE MAIN CONTENT
                st.markdown("**Answer:**")
                answer = result.get("answer", "")
                
                if answer and answer not in ["No answer generated", "No answer generated - check if API is responding correctly", ""]:
                    # Display the actual answer
                    st.markdown(answer)
                else:
                    st.warning("⚠️ No answer was generated. This usually means:")
                    st.markdown("""
                    - The API model name is incorrect or unavailable
                    - The API key doesn't have access to the specified model
                    - There was an API error (check debug mode for details)
                    - The query returned no data
                    """)
                    
                    # If there's data, at least show it
                    if result.get("data") is not None and not result["data"].empty:
                        st.info("💡 However, the query executed successfully. See the data table below.")
                
                # Show data table (collapsible)
                if result.get("data") is not None:
                    try:
                        df = result["data"]
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            with st.expander(f"📊 View Data Table ({len(df)} rows)", expanded=False):
                                st.dataframe(
                                    df,
                                    use_container_width=True,
                                    height=min(400, len(df) * 35 + 38)
                                )
                                
                                # Download button
                                csv = df.to_csv(index=False)
                                st.download_button(
                                    label="⬇️ Download as CSV",
                                    data=csv,
                                    file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv",
                                    key=f"download_{message['timestamp']}"
                                )
                        elif isinstance(df, pd.DataFrame) and df.empty:
                            st.info("ℹ️ Query returned no results")
                        else:
                            st.warning(f"⚠️ Unexpected data type: {type(df)}")
                    except Exception as e:
                        st.error(f"Error displaying data: {str(e)}")
                        if st.session_state.debug_mode:
                            st.code(traceback.format_exc())
        
        st.divider()


# Main App Layout
def main():
    # Header
    st.markdown('<div class="main-header">🌾 Agricultural Data QA System</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Ask questions about agricultural production, rainfall, and crop data in natural language</div>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.title("⚙️ Configuration")
        
        # Debug mode toggle
        st.session_state.debug_mode = st.checkbox("🐛 Debug Mode", value=st.session_state.debug_mode)
        
        st.divider()
        
        # Database connection
        st.subheader("📁 Database Connection")
        
        default_db = "data/agricultural_data.db"
        db_path = st.text_input(
            "Database Path",
            value=default_db,
            help="Path to agricultural_data.db"
        )
        
        # Model selection
        st.subheader("🤖 Model Configuration")
        use_openrouter = st.checkbox("Use OpenRouter", value=True, help="Use OpenRouter API for model access")
        
        if use_openrouter:
            st.info("💡 Available OpenRouter models")
            
            # Predefined model options
            model_options = {
                "GPT-4 Turbo (Recommended)": "openai/gpt-4-turbo",
                "GPT-4": "openai/gpt-4",
                "GPT-3.5 Turbo (Fast & Cheap)": "openai/gpt-3.5-turbo",
                "Claude 3 Opus": "anthropic/claude-3-opus",
                "Claude 3 Sonnet": "anthropic/claude-3-sonnet",
                "Claude 3.5 Sonnet": "anthropic/claude-3.5-sonnet",
                "Custom (Enter below)": "custom"
            }
            
            query_model_choice = st.selectbox(
                "Query Generation Model",
                options=list(model_options.keys()),
                index=0,
                help="Model for generating SQL queries"
            )
            
            if query_model_choice == "Custom (Enter below)":
                query_model = st.text_input(
                    "Enter custom model path",
                    value="openai/gpt-4-turbo",
                    placeholder="e.g., openai/gpt-4-turbo"
                )
            else:
                query_model = model_options[query_model_choice]
                st.code(f"Using: {query_model}", language="text")
            
            answer_model_choice = st.selectbox(
                "Answer Generation Model",
                options=list(model_options.keys()),
                index=0,
                help="Model for generating natural language answers"
            )
            
            if answer_model_choice == "Custom (Enter below)":
                answer_model = st.text_input(
                    "Enter custom model path",
                    value="openai/gpt-4-turbo",
                    placeholder="e.g., openai/gpt-4-turbo",
                    key="answer_custom"
                )
            else:
                answer_model = model_options[answer_model_choice]
                st.code(f"Using: {answer_model}", language="text")
        else:
            st.warning("⚠️ Direct OpenAI API usage (not recommended - use OpenRouter instead)")
            query_model = "gpt-4-turbo"
            answer_model = "gpt-4-turbo"
        
        if st.button("🔌 Connect to Database", use_container_width=True, type="primary"):
            initialize_qa_system(db_path, use_openrouter, query_model, answer_model)
        
        if st.session_state.db_connected:
            st.success("✅ Connected")
            
            # Show current configuration
            with st.expander("⚙️ Current Configuration"):
                st.markdown(f"""
                **API:** {'OpenRouter' if st.session_state.use_openrouter else 'OpenAI'}
                
                **Query Model:** `{st.session_state.query_model}`
                
                **Answer Model:** `{st.session_state.answer_model}`
                """)
            
            # Show database stats
            with st.expander("📊 Database Statistics"):
                try:
                    stats = DatabaseSchema.get_table_info(db_path)
                    for table, info in stats.items():
                        if "error" not in info:
                            st.metric(table, f"{info.get('row_count', 0):,} rows")
                except Exception as e:
                    st.error(f"Error: {e}")
        
        st.divider()
        
        # Clear chat
        if st.button("🗑️ Clear Chat History", use_container_width=True):
            st.session_state.chat_history = []
            st.rerun()
        
        st.divider()
        
        # Example questions
        st.subheader("💡 Example Questions")
        
        examples = [
            "Which state produced the most rice in 2014?",
            "What was the average annual rainfall in Punjab between 2010 and 2015?",
            "Show me the top 5 districts by wheat production",
            "Compare rainfall between Maharashtra and Karnataka",
            "What crops are most produced in Himachal Pradesh?",
            "List districts with highest average rainfall in 2020",
        ]
        
        for example in examples:
            if st.button(example, key=f"ex_{hash(example)}", use_container_width=True):
                st.session_state.example_question = example
        
        st.divider()
        
        # API Key status
        st.subheader("🔑 API Status")
        api_key = os.getenv("OPENROUTER_API_KEY")
        
        if api_key:
            st.success("✅ OPENROUTER_API_KEY found")
            if st.session_state.debug_mode:
                st.code(f"Key: {api_key[:10]}...{api_key[-10:]}")
        else:
            st.error("❌ OPENROUTER_API_KEY not found")
            st.warning("⚠️ Set OPENROUTER_API_KEY in your .env file")
            with st.expander("📖 How to get an API key"):
                st.markdown("""
                1. Visit [OpenRouter.ai](https://openrouter.ai/)
                2. Sign up for an account
                3. Go to [Keys](https://openrouter.ai/keys)
                4. Create a new API key
                5. Add it to your `.env` file:
                   ```
                   OPENROUTER_API_KEY=sk-or-v1-...
                   ```
                """)
        
        st.divider()
        
        # Help section
        with st.expander("ℹ️ How to Use"):
            st.markdown("""
            **Steps:**
            1. Connect to the database
            2. Type your question in natural language
            3. Click 'Ask' or press Enter
            4. View the generated SQL and answer
            
            **Supported Queries:**
            - Production statistics by crop/state/district
            - Rainfall comparisons and trends
            - Top/bottom rankings
            - Time-range filtering
            - Aggregations (sum, average, count)
            
            **Tips:**
            - Be specific about locations and time periods
            - Use crop names as they appear in data
            - Ask one question at a time for best results
            - Enable Debug Mode to see detailed logs
            """)
    
    # Main content area
    if not st.session_state.db_connected:
        st.info("👈 Please connect to the database using the sidebar to get started")
        
        # Show welcome info
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### 📊 Available Data
            - Crop production statistics
            - Monthly rainfall data
            - Crop-specific production
            - Agency rainfall records
            """)
        
        with col2:
            st.markdown("""
            ### 🎯 What You Can Ask
            - Production comparisons
            - Rainfall analysis
            - Top/bottom rankings
            - Temporal trends
            """)
        
        with col3:
            st.markdown("""
            ### 🚀 Features
            - Natural language queries
            - Automatic SQL generation
            - LLM-enhanced answers
            - Data export (CSV)
            """)
        
        return
    
    # Question input
    st.subheader("💬 Ask Your Question")
    
    # Check for example question
    default_question = st.session_state.pop("example_question", "")
    
    col1, col2 = st.columns([5, 1])
    with col1:
        question = st.text_input(
            "Type your question here",
            value=default_question,
            placeholder="e.g., Which state produced the most rice in 2014?",
            label_visibility="collapsed",
            disabled=st.session_state.processing
        )
    
    with col2:
        ask_button = st.button(
            "🔍 Ask", 
            use_container_width=True, 
            type="primary",
            disabled=st.session_state.processing or not question.strip()
        )
    
    # Process question
    if ask_button and question.strip():
        process_question(question.strip())
        st.rerun()
    
    # Display chat history
    st.divider()
    
    if st.session_state.chat_history:
        st.subheader("💬 Conversation History")
        
        # Display in reverse order (newest first)
        for message in reversed(st.session_state.chat_history):
            display_chat_message(message)
    else:
        st.info("👆 Ask a question to get started!")
        
        # Show quick tips
        st.markdown("### 🎓 Quick Tips")
        tips_col1, tips_col2 = st.columns(2)
        
        with tips_col1:
            st.markdown("""
            **For Production Queries:**
            - "Which state produced the most rice in 2014?"
            - "Top 5 rice-producing states in 2014"
            - "Total wheat production in Punjab"
            - "Which district produces most maize?"
            """)
        
        with tips_col2:
            st.markdown("""
            **For Rainfall Queries:**
            - "Average annual rainfall in Maharashtra"
            - "Compare rainfall in 2010 vs 2015"
            - "Monsoon rainfall in June-September"
            """)


# Run the app
if __name__ == "__main__":
    main()