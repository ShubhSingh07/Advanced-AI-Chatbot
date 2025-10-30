"""
Enhanced LLM-Powered SQL Query Generator for Agricultural Data
Compatible with OpenRouter API (GPT-5 Codex and other models)
"""

import os
import sqlite3
import json
import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

@dataclass
class QueryResult:
    """Encapsulates query execution results"""
    success: bool
    data: Optional[pd.DataFrame]
    sql_query: str
    error_message: Optional[str] = None
    execution_time: float = 0.0
    row_count: int = 0


class DatabaseSchema:
    """Manages database schema information for LLM context"""
    
    SCHEMA_INFO = {
        "crop_production": {
            "columns": ["id", "state", "district", "crop", "season", "year", "area", "production", "data_source", "fetch_timestamp"],
            "description": "Agricultural crop production statistics by state, district, crop, season, and year",
            "sample_queries": [
                "SELECT state, crop, SUM(production) FROM crop_production WHERE year BETWEEN 2010 AND 2015 GROUP BY state, crop",
                "SELECT district, AVG(area) FROM crop_production WHERE state='Punjab' GROUP BY district"
            ]
        },
        "rainfall_district": {
            "columns": ["id", "SUBDIVISION", "YEAR", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "annual"],
            "description": "Monthly and annual rainfall data by subdivision/district",
            "sample_queries": [
                "SELECT SUBDIVISION, AVG(annual) FROM rainfall_district WHERE YEAR BETWEEN 2010 AND 2020 GROUP BY SUBDIVISION",
                "SELECT SUBDIVISION, jun, jul, aug FROM rainfall_district WHERE YEAR=2015"
            ]
        },
        "production_crop_specific": {
            "columns": ["id", "State", "District", "Wheat", "Maize", "Rice", "Barley", "Ragi", "Pulses", "common_millets", "Total", "Chillies", "Ginger", "Oil_seeds"],
            "description": "Crop-specific production data in metric tonnes by state and district",
            "sample_queries": [
                "SELECT State, District, Wheat, Rice FROM production_crop_specific WHERE Wheat > 1000",
                "SELECT State, SUM(Total) FROM production_crop_specific GROUP BY State ORDER BY SUM(Total) DESC"
            ]
        },
        "Agency_Rainfall": {
            "columns": ["id", "State", "District", "Date", "Year", "Month", "Avg_rainfall", "Agency_name"],
            "description": "Daily rainfall measurements by state, district, and reporting agency",
            "sample_queries": [
                "SELECT State, AVG(Avg_rainfall) FROM Agency_Rainfall WHERE Year=2020 GROUP BY State",
                "SELECT District, Month, AVG(Avg_rainfall) FROM Agency_Rainfall WHERE State='Maharashtra' GROUP BY District, Month"
            ]
        }
    }
    
    @classmethod
    def get_schema_description(cls) -> str:
        """Generate detailed schema description for LLM"""
        desc = "DATABASE SCHEMA:\n\n"
        for table, info in cls.SCHEMA_INFO.items():
            desc += f"Table: {table}\n"
            desc += f"Description: {info['description']}\n"
            desc += f"Columns: {', '.join(info['columns'])}\n"
            desc += "Sample Queries:\n"
            for sq in info['sample_queries']:
                desc += f"  - {sq}\n"
            desc += "\n"
        return desc
    
    @classmethod
    def get_table_info(cls, db_path: str) -> Dict[str, Any]:
        """Get actual table statistics from database"""
        conn = sqlite3.connect(db_path)
        stats = {}
        
        for table in cls.SCHEMA_INFO.keys():
            try:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # Get sample data
                cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                sample = cursor.fetchall()
                
                stats[table] = {
                    "row_count": count,
                    "sample_data": sample[:2] if sample else []
                }
            except Exception as e:
                stats[table] = {"error": str(e)}
        
        conn.close()
        return stats


class SQLQueryGenerator:
    """LLM-powered SQL query generator with validation (OpenRouter compatible)"""
    
    SQL_GENERATION_PROMPT = """You are an expert SQL query generator for an agricultural database. Generate a SAFE, VALID SQLite query based on the user's question.

{schema}

STRICT RULES:
1. ONLY use SELECT statements - NO INSERT, UPDATE, DELETE, DROP, ALTER
2. Use proper JOINs when combining tables
3. Use aggregation functions (SUM, AVG, COUNT, MAX, MIN) appropriately
4. Always include LIMIT clause (max 1000 rows)
5. Use WHERE clauses for filtering
6. Use GROUP BY with aggregations
7. Use ORDER BY for sorting
8. Handle NULL values appropriately
9. Use DISTINCT when needed to avoid duplicates
10. For time ranges, use BETWEEN operator

TABLE RELATIONSHIPS:
- crop_production.state can JOIN with rainfall_district.SUBDIVISION (approximate match)
- crop_production.state can JOIN with production_crop_specific.State
- crop_production.state can JOIN with Agency_Rainfall.State

IMPORTANT NOTES:
- rainfall_district uses 'SUBDIVISION' not 'state'
- production_crop_specific has crop columns (Wheat, Rice, etc.) as integers
- Agency_Rainfall has daily data, may need aggregation
- crop_production has year as INTEGER
- Always use CAST or proper type conversions when comparing columns

User Question: {question}

Database Statistics:
{db_stats}

Generate a JSON response with:
{{
    "sql_query": "the complete SQL query",
    "explanation": "brief explanation of what the query does",
    "tables_used": ["list", "of", "tables"],
    "complexity": "simple|moderate|complex",
    "estimated_rows": approximate number of result rows
}}

If the question is ambiguous or cannot be answered with available data, return:
{{
    "sql_query": "",
    "explanation": "Clear explanation of why query cannot be generated",
    "tables_used": [],
    "complexity": "none",
    "estimated_rows": 0
}}"""

    # OpenRouter model mappings
    OPENROUTER_MODELS = {
        "gpt-5-codex": "openai/gpt-5-codex",
        "gpt-4-turbo": "openai/gpt-4-turbo",
        "gpt-4": "openai/gpt-4",
        "gpt-3.5-turbo": "openai/gpt-3.5-turbo",
        "claude-3-opus": "anthropic/claude-3-opus",
        "claude-3-sonnet": "anthropic/claude-3-sonnet",
        # Add more models as needed
    }

    def __init__(
        self, 
        api_key: Optional[str] = None, 
        model: str = "gpt-5-codex",
        use_openrouter: bool = True,
        openrouter_api_key: Optional[str] = None
    ):
        """
        Initialize with OpenRouter or OpenAI API
        
        Args:
            api_key: OpenAI API key (for direct OpenAI usage)
            model: Model name (short name like 'gpt-5-codex' or full OpenRouter path)
            use_openrouter: Whether to use OpenRouter API
            openrouter_api_key: OpenRouter API key (if different from OPENAI_API_KEY)
        """
        self.use_openrouter = use_openrouter
        
        if use_openrouter:
            # OpenRouter configuration
            self.api_key = openrouter_api_key or os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")
            if not self.api_key:
                raise ValueError(
                    "OpenRouter API key required. Set OPENROUTER_API_KEY or OPENAI_API_KEY environment variable."
                )
            
            # Map short model name to OpenRouter path
            self.model = self.OPENROUTER_MODELS.get(model, model)
            
            # Initialize OpenAI client with OpenRouter base URL
            self.client = OpenAI(
                api_key=self.api_key,
                base_url="https://openrouter.ai/api/v1"
            )
            
            print(f"✅ Initialized with OpenRouter API")
            print(f"   Model: {self.model}")
        else:
            # Standard OpenAI configuration
            self.api_key = api_key or os.getenv("OPENAI_API_KEY")
            if not self.api_key:
                raise ValueError("OpenAI API key required. Set OPENAI_API_KEY environment variable.")
            
            self.model = model
            self.client = OpenAI(api_key=self.api_key)
            print(f"✅ Initialized with OpenAI API")
            print(f"   Model: {self.model}")
        
        self.schema = DatabaseSchema()
    
    def generate_query(self, question: str, db_path: str) -> Dict[str, Any]:
        """Generate SQL query from natural language question"""
        try:
            # Get database statistics
            db_stats = self.schema.get_table_info(db_path)
            
            # Prepare prompt
            prompt = self.SQL_GENERATION_PROMPT.format(
                schema=self.schema.get_schema_description(),
                question=question,
                db_stats=json.dumps(db_stats, indent=2)
            )
            
            # Prepare request parameters
            request_params = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "You are an expert SQL query generator. Always return valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.1,
                "max_tokens": 1000
            }
            
            # Add OpenRouter-specific headers if using OpenRouter
            if self.use_openrouter:
                # OpenRouter supports extra_headers for additional metadata
                request_params["extra_headers"] = {
                    "HTTP-Referer": "https://github.com/your-repo",  # Optional: your app URL
                    "X-Title": "Agricultural QA System"  # Optional: your app name
                }
            
            # Call API
            response = self.client.chat.completions.create(**request_params)
            
            # Parse response
            content = response.choices[0].message.content.strip()
            
            # Extract JSON from response (handle markdown code blocks)
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            else:
                json_match = re.search(r'(\{.*\})', content, re.DOTALL)
                if json_match:
                    content = json_match.group(1)
            
            result = json.loads(content)
            
            # Validate query
            if result.get("sql_query"):
                is_valid, error = self._validate_query(result["sql_query"])
                if not is_valid:
                    result["sql_query"] = ""
                    result["explanation"] = f"Generated query failed validation: {error}"
            
            return result
            
        except json.JSONDecodeError as e:
            return {
                "sql_query": "",
                "explanation": f"Failed to parse LLM response: {str(e)}",
                "tables_used": [],
                "complexity": "error",
                "estimated_rows": 0
            }
        except Exception as e:
            return {
                "sql_query": "",
                "explanation": f"Error generating query: {str(e)}",
                "tables_used": [],
                "complexity": "error",
                "estimated_rows": 0
            }
    
    def _validate_query(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Validate SQL query for safety"""
        sql_lower = sql.lower().strip()
        
        # Check for dangerous keywords
        dangerous = ["insert", "update", "delete", "drop", "alter", "create", "truncate", "exec", "execute"]
        for keyword in dangerous:
            if keyword in sql_lower:
                return False, f"Dangerous keyword detected: {keyword}"
        
        # Must start with SELECT or WITH
        if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
            return False, "Query must start with SELECT or WITH"
        
        # Check for semicolons (prevent query chaining)
        if sql.count(";") > 1:
            return False, "Multiple statements not allowed"
        
        # Check for LIMIT clause
        if "limit" not in sql_lower:
            return False, "LIMIT clause required for safety"
        
        return True, None


class QueryExecutor:
    """Executes SQL queries and manages results"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def execute_query(self, sql: str, max_rows: int = 1000) -> QueryResult:
        """Execute SQL query and return results"""
        start_time = datetime.now()
        
        try:
            # Ensure LIMIT is applied
            if "limit" not in sql.lower():
                sql = f"{sql.rstrip(';')} LIMIT {max_rows};"
            
            # Connect and execute
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(sql, conn)
            conn.close()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return QueryResult(
                success=True,
                data=df,
                sql_query=sql,
                execution_time=execution_time,
                row_count=len(df)
            )
            
        except Exception as e:
            return QueryResult(
                success=False,
                data=None,
                sql_query=sql,
                error_message=str(e),
                execution_time=(datetime.now() - start_time).total_seconds()
            )


class AnswerSynthesizer:
    """Generates natural language answers from query results (OpenRouter compatible)"""
    
    SYNTHESIS_PROMPT = """You are an agricultural data analyst. Generate a clear, conversational answer based on the query results.

Original Question: {question}

SQL Query Executed:
```sql
{sql_query}
```

Query Results ({row_count} rows):
{results_preview}

Tables Used: {tables_used}

Generate a comprehensive answer that:
1. Directly answers the user's question
2. Highlights key insights and patterns
3. Includes specific numbers and comparisons
4. Mentions data sources (tables used)
5. Notes any limitations or caveats
6. Uses a friendly, conversational tone

Keep the answer concise (3-5 paragraphs max) but informative."""

    def __init__(
        self, 
        api_key: Optional[str] = None, 
        model: str = "gpt-4-turbo",
        use_openrouter: bool = True,
        openrouter_api_key: Optional[str] = None
    ):
        """Initialize with OpenRouter or OpenAI API"""
        self.use_openrouter = use_openrouter
        
        if use_openrouter:
            self.api_key = openrouter_api_key or os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")
            # Map short model name to OpenRouter path
            model_mapping = {
                "gpt-4-turbo": "openai/gpt-4-turbo",
                "gpt-4": "openai/gpt-4",
                "gpt-3.5-turbo": "openai/gpt-3.5-turbo",
                "claude-3-sonnet": "anthropic/claude-3-sonnet"
            }
            self.model = model_mapping.get(model, model)
            self.client = OpenAI(
                api_key=self.api_key,
                base_url="https://openrouter.ai/api/v1"
            )
        else:
            self.api_key = api_key or os.getenv("OPENAI_API_KEY")
            self.model = model
            self.client = OpenAI(api_key=self.api_key)
    
    def synthesize_answer(
        self, 
        question: str, 
        query_result: QueryResult, 
        query_metadata: Dict[str, Any]
    ) -> str:
        """Generate natural language answer from query results"""
        
        if not query_result.success:
            return f"❌ **Error executing query:** {query_result.error_message}\n\nThe generated SQL query could not be executed. Please try rephrasing your question or ask about different data."
        
        if query_result.data is None or query_result.data.empty:
            return "📊 **No data found** matching your query criteria. This could mean:\n- The data doesn't exist for the specified filters\n- Try adjusting year ranges or location filters\n- Check if the crop/location names are spelled correctly"
        
        try:
            # Prepare results preview
            df = query_result.data
            preview_rows = min(20, len(df))
            
            # Format preview nicely
            results_preview = df.head(preview_rows).to_string(index=False)
            
            # If many rows, add summary statistics
            if len(df) > preview_rows:
                results_preview += f"\n\n... (showing first {preview_rows} of {len(df)} rows)"
                
                # Add summary for numeric columns
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    results_preview += "\n\nSummary Statistics:\n"
                    results_preview += df[numeric_cols].describe().to_string()
            
            # Generate answer using LLM
            prompt = self.SYNTHESIS_PROMPT.format(
                question=question,
                sql_query=query_result.sql_query,
                row_count=query_result.row_count,
                results_preview=results_preview,
                tables_used=", ".join(query_metadata.get("tables_used", []))
            )
            
            request_params = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "You are a helpful agricultural data analyst. Provide clear, accurate answers based on data."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": 800
            }
            
            if self.use_openrouter:
                request_params["extra_headers"] = {
                    "HTTP-Referer": "https://github.com/your-repo",
                    "X-Title": "Agricultural QA System"
                }
            
            response = self.client.chat.completions.create(**request_params)
            answer = response.choices[0].message.content.strip()
            
            # Add metadata footer
            footer = f"\n\n---\n**Query Details:**\n"
            footer += f"- Tables: {', '.join(query_metadata.get('tables_used', []))}\n"
            footer += f"- Rows returned: {query_result.row_count:,}\n"
            footer += f"- Execution time: {query_result.execution_time:.2f}s\n"
            footer += f"- Complexity: {query_metadata.get('complexity', 'unknown')}"
            
            return answer + footer
            
        except Exception as e:
            return f"❌ **Error generating answer:** {str(e)}\n\nRaw data preview:\n{query_result.data.head(10).to_string()}"


class AgriculturalQASystem:
    """Main QA system orchestrating all components (OpenRouter compatible)"""
    
    def __init__(
        self, 
        db_path: str, 
        api_key: Optional[str] = None,
        use_openrouter: bool = True,
        query_model: str = "gpt-5-codex",
        answer_model: str = "gpt-4-turbo"
    ):
        """
        Initialize QA System
        
        Args:
            db_path: Path to SQLite database
            api_key: API key (OpenRouter or OpenAI)
            use_openrouter: Whether to use OpenRouter API
            query_model: Model for SQL generation
            answer_model: Model for answer synthesis
        """
        self.db_path = db_path
        self.use_openrouter = use_openrouter
        
        # Initialize components
        self.query_generator = SQLQueryGenerator(
            api_key=api_key,
            model=query_model,
            use_openrouter=use_openrouter,
            openrouter_api_key=api_key
        )
        
        self.query_executor = QueryExecutor(db_path)
        
        self.answer_synthesizer = AnswerSynthesizer(
            api_key=api_key,
            model=answer_model,
            use_openrouter=use_openrouter,
            openrouter_api_key=api_key
        )
    
    def answer_question(self, question: str) -> Dict[str, Any]:
        """
        End-to-end question answering pipeline
        
        Returns:
            Dict containing answer, SQL query, data, and metadata
        """
        # Step 1: Generate SQL query
        query_metadata = self.query_generator.generate_query(question, self.db_path)
        
        if not query_metadata.get("sql_query"):
            return {
                "success": False,
                "question": question,
                "answer": f"❌ **Unable to generate query:** {query_metadata.get('explanation', 'Unknown error')}",
                "sql_query": None,
                "data": None,
                "metadata": query_metadata
            }
        
        # Step 2: Execute query
        query_result = self.query_executor.execute_query(query_metadata["sql_query"])
        
        # Step 3: Synthesize answer
        answer = self.answer_synthesizer.synthesize_answer(
            question, 
            query_result, 
            query_metadata
        )
        
        return {
            "success": query_result.success,
            "question": question,
            "answer": answer,
            "sql_query": query_result.sql_query,
            "data": query_result.data,
            "metadata": {
                **query_metadata,
                "execution_time": query_result.execution_time,
                "row_count": query_result.row_count
            }
        }


# CLI Testing Interface
if __name__ == "__main__":
    import sys
    
    DB_PATH = "data/agricultural_data.db"
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found: {DB_PATH}")
        sys.exit(1)
    
    print("="*80)
    print("Agricultural QA System - CLI Test (OpenRouter)")
    print("="*80)
    
    # Initialize with OpenRouter
    qa_system = AgriculturalQASystem(
        DB_PATH,
        use_openrouter=True,
        query_model="gpt-5-codex",  # or any OpenRouter model
        answer_model="gpt-4-turbo"
    )
    
    # Test questions
    test_questions = [
        "What was the average annual rainfall in Punjab between 2010 and 2015?",
        "Which state produced the most rice in 2014?",
        "Show me the top 5 districts by wheat production",
        "Compare rainfall between Maharashtra and Karnataka"
    ]
    
    print("\nRunning test questions...\n")
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n{'='*80}")
        print(f"Question {i}: {question}")
        print('='*80)
        
        result = qa_system.answer_question(question)
        
        print(f"\n📊 SQL Query:\n{result['sql_query']}\n")
        print(f"💬 Answer:\n{result['answer']}\n")
        
        if result['data'] is not None and not result['data'].empty:
            print(f"📈 Data Preview:\n{result['data'].head().to_string()}\n")
    
    print("\n" + "="*80)
    print("✅ Test complete!")
    print("="*80)