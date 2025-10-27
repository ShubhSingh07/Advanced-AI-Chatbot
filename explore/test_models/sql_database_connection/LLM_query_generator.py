"""
SQL AI Agent - Compatible with OpenRouter
Works with openai>=2.0.0 and OpenRouter API
"""

import sqlite3
import json
import os
from typing import Dict, Optional, Tuple
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class SQLAgent:
    """SQL Agent using OpenRouter API"""
    
    def __init__(self, db_path: str, api_key: Optional[str] = None):
        """
        Initialize SQL Agent
        
        Args:
            db_path: Path to SQLite database
            api_key: OpenRouter API key (or set OPENAI_API_KEY env var)
        """
        # Convert to absolute path and check if exists
        self.db_path = os.path.abspath(db_path)
        
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(
                f"❌ Database not found at: {self.db_path}\n"
                f"Current directory: {os.getcwd()}\n"
                f"Please check the path or create the database first."
            )
        
        print(f"✅ Database found at: {self.db_path}\n")
        
        # Initialize OpenAI client pointing to OpenRouter
        self.client = OpenAI(
            api_key=api_key or os.getenv("OPENAI_API_KEY"),
            base_url="https://openrouter.ai/api/v1"
        )
        
    def get_database_schema(self) -> str:
        """Get database schema information"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all tables first
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            if not tables:
                conn.close()
                return "Error: No tables found in database"
            
            schema = f"Database: {os.path.basename(self.db_path)}\n"
            schema += f"Available tables: {[t[0] for t in tables]}\n\n"
            
            # Get info for api_data table (or first table if api_data doesn't exist)
            target_table = "api_data" if ("api_data",) in tables else tables[0][0]
            
            cursor.execute(f"PRAGMA table_info({target_table})")
            columns = cursor.fetchall()
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {target_table} LIMIT 3")
            samples = cursor.fetchall()
            
            conn.close()
            
            schema += f"Table: {target_table}\n\nColumns:\n"
            for col in columns:
                schema += f"- {col[1]} ({col[2]})\n"
            
            schema += "\nSample Data:\n"
            schema += str(samples[:2])
            
            schema += """

Important Notes:
1. area_ and production_ are stored as TEXT but contain numeric values
2. Use CAST(column AS REAL) for numeric operations
3. Season values: 'Kharif', 'Rabi', 'Autumn', 'Whole Year'
4. production_ may contain 'NA' for missing data
5. Use LIKE '%keyword%' for partial text matching
6. Always add LIMIT clause to prevent huge result sets
"""
            return schema
            
        except sqlite3.Error as e:
            return f"Error reading schema: {str(e)}"
    
    def generate_sql_query(self, user_question: str, model: str = "openai/gpt-4o-mini") -> Dict:
        """Convert natural language to SQL using GPT via OpenRouter"""
        
        schema = self.get_database_schema()
        
        system_prompt = f"""You are a SQL expert. Convert natural language questions into SQLite queries.

{schema}

Rules:
1. Only generate SELECT queries (no INSERT, UPDATE, DELETE)
2. Always use proper SQLite syntax
3. Handle NULL values and 'NA' strings appropriately
4. Use CAST for numeric operations on TEXT columns
5. Always add LIMIT clause (default 100 if not specified)
6. Return ONLY valid JSON with 'query' and 'explanation' keys

Response format (JSON only):
{{
    "query": "SELECT...",
    "explanation": "This query will..."
}}
"""
        
        try:
            # OpenRouter API call
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_question}
                ],
                temperature=0.1,
                max_tokens=500,
                response_format={"type": "json_object"}  # Force JSON response
            )
            
            # Extract response content
            result_text = response.choices[0].message.content
            
            # Parse JSON
            result = json.loads(result_text)
            return result
            
        except Exception as e:
            return {
                "query": None,
                "explanation": None,
                "error": f"Error: {str(e)}"
            }
    
    def validate_sql_query(self, query: str) -> Tuple[bool, str]:
        """Validate SQL query for safety"""
        
        if not query or not isinstance(query, str):
            return False, "Invalid query"
        
        dangerous_keywords = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 
            'CREATE', 'TRUNCATE', 'EXEC', 'EXECUTE', 'PRAGMA'
        ]
        
        query_upper = query.upper()
        
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                return False, f"Dangerous keyword detected: {keyword}"
        
        if not query_upper.strip().startswith('SELECT'):
            return False, "Only SELECT queries are allowed"
        
        return True, "Query is safe"
    
    def execute_query(self, query: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Execute SQL query and return results"""
        
        # Validate first
        is_valid, message = self.validate_sql_query(query)
        if not is_valid:
            return None, f"Validation failed: {message}"
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            
            conn.close()
            
            return {
                'columns': column_names,
                'data': results,
                'row_count': len(results)
            }, None
            
        except sqlite3.Error as e:
            return None, f"SQL Error: {str(e)}"
    
    def format_results(self, results: Dict, max_rows: int = 50) -> str:
        """Format query results in a readable table"""
        
        if not results or not results.get('data'):
            return "\n❌ No results found.\n"
        
        columns = results['columns']
        data = results['data']
        row_count = results['row_count']
        
        # Calculate column widths
        col_widths = [len(str(col)) for col in columns]
        for row in data[:max_rows]:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)[:30]))
        
        # Build output
        total_width = sum(col_widths) + len(columns) * 3
        output = "\n" + "=" * total_width + "\n"
        output += "📊 Query Results\n"
        output += "=" * total_width + "\n"
        
        # Header
        header = " | ".join([f"{col:<{col_widths[i]}}" for i, col in enumerate(columns)])
        output += header + "\n"
        output += "-" * total_width + "\n"
        
        # Rows
        display_rows = min(max_rows, len(data))
        for row in data[:display_rows]:
            row_str = " | ".join([
                f"{str(val)[:30]:<{col_widths[i]}}" 
                for i, val in enumerate(row)
            ])
            output += row_str + "\n"
        
        if row_count > display_rows:
            output += f"\n... ({row_count - display_rows} more rows)\n"
        
        output += "=" * total_width + "\n"
        output += f"✅ Total rows: {row_count}\n"
        
        return output
    
    def ask(self, user_question: str, model: str = "openai/gpt-4o-mini") -> Dict:
        """
        Main method: Process natural language question
        
        Args:
            user_question: Question in natural language
            model: OpenRouter model (e.g., "openai/gpt-4o-mini", "anthropic/claude-3.5-sonnet")
        
        Returns:
            Dict with success status, query, results, etc.
        """
        
        print(f"\n{'='*80}")
        print(f"🤔 Question: {user_question}")
        print(f"{'='*80}\n")
        
        # Step 1: Generate SQL
        print("🔄 Generating SQL query...")
        sql_response = self.generate_sql_query(user_question, model=model)
        
        if 'error' in sql_response:
            print(f"❌ Error: {sql_response['error']}\n")
            return {
                'success': False,
                'error': sql_response['error']
            }
        
        query = sql_response.get('query')
        explanation = sql_response.get('explanation', '')
        
        if not query:
            print("❌ No query generated\n")
            return {
                'success': False,
                'error': 'No query generated'
            }
        
        print(f"✅ Generated SQL:\n   {query}\n")
        print(f"📝 Explanation: {explanation}\n")
        
        # Step 2: Execute query
        print("⚙️  Executing query...")
        results, error = self.execute_query(query)
        
        if error:
            print(f"❌ Execution Error: {error}\n")
            return {
                'success': False,
                'query': query,
                'error': error
            }
        
        # Step 3: Format and display results
        formatted_results = self.format_results(results)
        print(formatted_results)
        
        return {
            'success': True,
            'query': query,
            'explanation': explanation,
            'results': results,
            'formatted_results': formatted_results
        }


# ============================================================================
# HELPER: Create Sample Database
# ============================================================================

def create_sample_database(db_path: str):
    """Create a sample crop production database for testing"""
    
    print(f"🔨 Creating sample database at: {db_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_data (
            state TEXT,
            district TEXT,
            crop TEXT,
            season TEXT,
            year_ INTEGER,
            area_ TEXT,
            production_ TEXT
        )
    """)
    
    # Sample data
    sample_data = [
        ("Punjab", "Amritsar", "Rice", "Kharif", 2010, "1500", "4500"),
        ("Punjab", "Ludhiana", "Wheat", "Rabi", 2010, "2000", "6000"),
        ("Kerala", "Alappuzha", "Coconut", "Whole Year", 2010, "800", "2400"),
        ("Maharashtra", "Pune", "Rice", "Kharif", 2010, "1200", "3600"),
        ("Karnataka", "Mysore", "Wheat", "Rabi", 2010, "1000", "3000"),
    ]
    
    cursor.executemany("""
        INSERT INTO api_data VALUES (?, ?, ?, ?, ?, ?, ?)
    """, sample_data)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Sample database created with {len(sample_data)} records\n")


# ============================================================================
# MAIN - Example Usage
# ============================================================================

if __name__ == "__main__":
    import sys
    
    # Check for API key
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ Error: OPENAI_API_KEY not found!")
        print("\n🔧 Setup instructions:")
        print("1. Create a .env file in your project root")
        print("2. Add: OPENAI_API_KEY=sk-or-v1-your-openrouter-key-here")
        print("3. Get key from: https://openrouter.ai/keys")
        sys.exit(1)
    
    # Database path - adjust this to match your setup
    db_path = "/Users/shubh/Project/RAG-Projects/Advanced-AI-Chatbot/data/api_data.py/sql/crop_production.db"
    
    # Create sample database if it doesn't exist
    if not os.path.exists(db_path):
        print(f"⚠️  Database not found at: {db_path}")
        response = input("Would you like to create a sample database? (y/n): ")
        if response.lower() == 'y':
            create_sample_database(db_path)
        else:
            print("\n💡 Please create the database or update db_path in the code.")
            sys.exit(1)
    
    # Initialize agent
    try:
        agent = SQLAgent(
            db_path=db_path,
            api_key=api_key
        )
        print("✅ SQL Agent initialized successfully with OpenRouter!\n")
    except FileNotFoundError as e:
        print(str(e))
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error initializing agent: {str(e)}")
        sys.exit(1)
    
    # Example questions
    test_questions = [
        "Show me rice production in 2010",
        "Which state produces the most coconut?",
        "What is the total area under wheat cultivation?",
    ]
    
    for question in test_questions:
        result = agent.ask(question, model="openai/gpt-4o-mini")
        
        if not result['success']:
            print(f"❌ Failed: {result.get('error', 'Unknown error')}\n")
        
        print("\n" + "="*80 + "\n")