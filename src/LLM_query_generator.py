"""
Advanced LLM-Powered SQL Query Generator for Agricultural Data
Using Google Gemini API Directly
"""

import requests
import os
import sqlite3
import json
import re
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
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
    tables_used: List[str] = None


class DatabaseSchema:
    """Manages database schema information for LLM context"""
    
    SCHEMA_INFO = {
        "crop_production": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "state": "TEXT (State name)",
                "district": "TEXT (District name)",
                "crop": "TEXT (Crop name: Rice, Wheat, Maize, Arecanut, etc.)",
                "season": "TEXT (Kharif, Rabi, Whole Year)",
                "year": "INTEGER (Year: 1997-2014)",
                "area": "REAL (Area in hectares)",
                "production": "REAL (Production in tonnes)",
                "data_source": "TEXT",
                "fetch_timestamp": "TEXT"
            },
            "description": "Detailed crop production by state, district, crop, season and year (1997-2014)",
            "sample_queries": [
                "SELECT state, crop, SUM(production) FROM crop_production WHERE year=2014 GROUP BY state, crop",
                "SELECT district, AVG(area) FROM crop_production WHERE state='Punjab' AND year BETWEEN 2010 AND 2014 GROUP BY district"
            ],
            "row_count": "9,000 records",
            "indexes": ["state", "district", "crop", "year"]
        },
        
        "rainfall_district": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "SUBDIVISION": "TEXT (State/District name - joins with crop_production.state)",
                "YEAR": "REAL (Year: 1901-2015)",
                "jan": "REAL (January rainfall in mm)",
                "feb": "REAL (February rainfall in mm)",
                "mar": "REAL (March rainfall in mm)",
                "apr": "REAL (April rainfall in mm)",
                "may": "REAL (May rainfall in mm)",
                "jun": "REAL (June rainfall in mm)",
                "jul": "REAL (July rainfall in mm)",
                "aug": "REAL (August rainfall in mm)",
                "sep": "REAL (September rainfall in mm)",
                "oct": "REAL (October rainfall in mm)",
                "nov": "REAL (November rainfall in mm)",
                "dec": "REAL (December rainfall in mm)",
                "annual": "REAL (Total annual rainfall in mm)"
            },
            "description": "Monthly and annual rainfall data by subdivision/district (1901-2015)",
            "sample_queries": [
                "SELECT SUBDIVISION, AVG(annual) FROM rainfall_district WHERE YEAR BETWEEN 2010 AND 2014 GROUP BY SUBDIVISION",
                "SELECT SUBDIVISION, jun, jul, aug FROM rainfall_district WHERE YEAR=2014"
            ],
            "row_count": "9,000 records",
            "indexes": ["SUBDIVISION", "YEAR"]
        },
        
        "production_crop_specific": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "State": "TEXT (State name - currently only Himachal Pradesh)",
                "District": "TEXT (District name)",
                "Wheat": "REAL (Wheat production in metric tonnes)",
                "Maize": "REAL (Maize production in metric tonnes)",
                "Rice": "REAL (Rice production in metric tonnes)",
                "Barley": "REAL (Barley production in metric tonnes)",
                "Ragi": "REAL (Ragi production in metric tonnes)",
                "Pulses": "REAL (Pulses production in metric tonnes)",
                "common_millets": "REAL (Common millets production in metric tonnes)",
                "Total": "REAL (Total food grains production in metric tonnes)",
                "Chillies": "REAL (Chillies production in metric tonnes)",
                "Ginger": "REAL (Ginger production in metric tonnes)",
                "Oil_seeds": "REAL (Oil seeds production in metric tonnes)"
            },
            "description": "Crop-specific production aggregated by state and district (Himachal Pradesh only)",
            "sample_queries": [
                "SELECT District, Wheat, Rice FROM production_crop_specific WHERE Wheat > 50000",
                "SELECT District, Total FROM production_crop_specific ORDER BY Total DESC LIMIT 5"
            ],
            "row_count": "117 records",
            "indexes": ["State", "District"]
        },
        
        "Agency_Rainfall": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "State": "TEXT (State name)",
                "District": "TEXT (District name)",
                "Date": "REAL (Date of measurement)",
                "Year": "REAL (Year: 2018)",
                "Month": "REAL (Month number: 1-12)",
                "Avg_rainfall": "REAL (Average rainfall in mm)",
                "Agency_name": "REAL (Measuring agency)",
                "fetch_timestamp": "REAL"
            },
            "description": "Daily rainfall measurements by agency (mainly 2018 data from Assam)",
            "sample_queries": [
                "SELECT State, District, AVG(Avg_rainfall) FROM Agency_Rainfall WHERE Year=2018 GROUP BY State, District",
                "SELECT District, Month, AVG(Avg_rainfall) FROM Agency_Rainfall WHERE State='Assam' GROUP BY District, Month"
            ],
            "row_count": "1,000 records",
            "indexes": ["State", "District", "Year", "Month"]
        },
        
        "damages_floods": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "year": "INTEGER (Year)",
                "state_ut": "TEXT (State/UT name)",
                "flood_severity": "TEXT (Severity level)",
                "area_affected_hectare": "REAL (Area affected in hectares)",
                "area_affected_hectare_raw": "TEXT (Raw value)",
                "crop_damage_value_crore": "REAL (Crop damage value in crores)",
                "crop_damage_value_crore_raw": "TEXT (Raw value)",
                "source": "TEXT (data.gov.in)",
                "fetch_timestamp": "TEXT"
            },
            "description": "Flood damage data by state including area affected and crop damage value",
            "sample_queries": [
                "SELECT state_ut, SUM(area_affected_hectare) FROM damages_floods GROUP BY state_ut",
                "SELECT year, state_ut, crop_damage_value_crore FROM damages_floods WHERE year BETWEEN 2010 AND 2020"
            ],
            "row_count": "324 records",
            "indexes": ["year", "state_ut"]
        },
        
        "damages_rainfall": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "year": "INTEGER (Year: 2012-2021)",
                "state_ut": "TEXT (State/UT name)",
                "crop_damage_hectare": "REAL (Crop damage area in hectares)",
                "crop_damage_hectare_raw": "TEXT (Raw value)",
                "crop_damage_value_crore": "REAL (Crop damage value in crores)",
                "crop_damage_value_crore_raw": "TEXT (Raw value)",
                "rainfall_type": "TEXT (Type of rainfall event)",
                "source": "TEXT (data.gov.in)",
                "fetch_timestamp": "TEXT"
            },
            "description": "Rainfall-related crop damage data (2012-2021) - NOTE: Most fields are NULL",
            "row_count": "90 records",
            "indexes": ["year", "state_ut"],
            "note": "Data quality issue: Most records have NULL values except year"
        },
        
        "extreme_temperature": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "year": "INTEGER (Year)",
                "state_ut": "TEXT (State/UT name)",
                "extreme_event_type": "TEXT (Type of extreme event)",
                "event_days": "INTEGER (Number of event days)",
                "event_days_raw": "TEXT (Raw value)",
                "deaths": "INTEGER (Number of deaths)",
                "deaths_raw": "TEXT (Raw value)",
                "source": "TEXT (data.gov.in)",
                "fetch_timestamp": "TEXT"
            },
            "description": "Extreme temperature events and casualties - NOTE: Most fields are NULL",
            "row_count": "594 records",
            "indexes": ["year", "state_ut"],
            "note": "Data quality issue: Most records have NULL values"
        },
        
        "drought_cases": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "year": "INTEGER (Year)",
                "state_ut": "TEXT (State/UT name)",
                "drought_severity": "TEXT (Severity level)",
                "area_affected_hectare": "REAL (Area affected in hectares)",
                "area_affected_hectare_raw": "TEXT (Raw value)",
                "crop_loss_value_crore": "REAL (Crop loss value in crores)",
                "crop_loss_value_crore_raw": "TEXT (Raw value)",
                "source": "TEXT (data.gov.in)",
                "fetch_timestamp": "TEXT"
            },
            "description": "Drought cases and crop losses by state - NOTE: Most fields are NULL",
            "row_count": "108 records",
            "indexes": ["year", "state_ut"],
            "note": "Data quality issue: Most records have NULL values"
        },
        
        "climate_expenditure": {
            "columns": {
                "id": "INTEGER PRIMARY KEY",
                "Year": "REAL (Year)",
                "Schemes": "TEXT (Scheme/Program name)",
                "Actual_Expenditure": "REAL (Expenditure amount)"
            },
            "description": "Climate-related government expenditure by scheme and year - NOTE: Currently empty",
            "row_count": "0 records (empty table)",
            "indexes": ["Year"],
            "note": "Table exists but contains no data"
        }
    }
    
    TABLE_RELATIONSHIPS = [
        {
            "tables": ["crop_production", "rainfall_district"],
            "join_condition": "crop_production.state = rainfall_district.SUBDIVISION AND crop_production.year = CAST(rainfall_district.YEAR AS INTEGER)",
            "description": "Join crop production with rainfall by state and year"
        },
        {
            "tables": ["crop_production", "damages_floods"],
            "join_condition": "crop_production.state = damages_floods.state_ut AND crop_production.year = damages_floods.year",
            "description": "Join crop production with flood damage data"
        }
    ]
    
    DATA_QUALITY_NOTES = {
        "damages_rainfall": "WARNING: 90 records but most have NULL values",
        "extreme_temperature": "WARNING: 594 records but most have NULL values",
        "drought_cases": "WARNING: 108 records but most have NULL values",
        "climate_expenditure": "WARNING: Table is empty",
        "production_crop_specific": "NOTE: Only Himachal Pradesh data",
        "Agency_Rainfall": "NOTE: Primarily 2018 Assam data"
    }
    
    @classmethod
    def get_schema_description(cls) -> str:
        desc = "DATABASE SCHEMA - AGRICULTURAL DATA\n\n"
        desc += "DATA QUALITY NOTES:\n"
        for table, note in cls.DATA_QUALITY_NOTES.items():
            desc += f"- {table}: {note}\n"
        desc += "\n"
        
        for table, info in cls.SCHEMA_INFO.items():
            desc += f"TABLE: {table}\n"
            desc += f"Description: {info['description']}\n"
            desc += "Columns:\n"
            for col, col_desc in info['columns'].items():
                desc += f"  - {col}: {col_desc}\n"
            desc += "\n"
        
        return desc
    
    @classmethod
    def get_table_info(cls, db_path: str) -> Dict[str, Any]:
        conn = sqlite3.connect(db_path)
        stats = {}
        
        for table in cls.SCHEMA_INFO.keys():
            try:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats[table] = {"row_count": count}
            except Exception as e:
                stats[table] = {"error": str(e)}
        
        conn.close()
        return stats


class GeminiAPIClient:
    """Google Gemini API client"""
    
    def __init__(self, api_key: str, model: str = "gemini-2.0-flash-exp"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        print(f"✅ Gemini API Client initialized")
        print(f"   Model: {self.model}")
    
    def generate_content(self, prompt: str, temperature: float = 0.1, max_tokens: int = 1500, retries: int = 3) -> str:
        """Call Gemini API"""
        url = f"{self.base_url}/{self.model}:generateContent?key={self.api_key}"
        
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
                "topP": 0.95,
                "topK": 40
            }
        }
        
        for attempt in range(1, retries + 1):
            try:
                print(f"🔄 API call attempt {attempt}/{retries}...")
                
                response = requests.post(url, json=payload, timeout=60)
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    print("   ✅ Success!")
                    data = response.json()
                    
                    if "candidates" in data and len(data["candidates"]) > 0:
                        content = data["candidates"][0]["content"]["parts"][0]["text"]
                        return content
                    else:
                        raise RuntimeError("No content in response")
                        
                elif response.status_code == 429:
                    print(f"   ⚠️  Rate limited, waiting...")
                    if attempt < retries:
                        time.sleep(5 * attempt)
                        continue
                    raise RuntimeError("Rate limit exceeded")
                    
                else:
                    error_msg = response.text
                    print(f"   ❌ Error: {error_msg}")
                    raise RuntimeError(f"HTTP {response.status_code}: {error_msg}")
                    
            except requests.exceptions.Timeout:
                print(f"   ⏳ Timeout, retrying...")
                if attempt < retries:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError("Request timeout")
                
            except Exception as e:
                if attempt < retries:
                    print(f"   ⚠️  Error: {e}, retrying...")
                    time.sleep(2 ** attempt)
                    continue
                raise
        
        raise RuntimeError("All retries failed")


class SQLQueryGenerator:
    """SQL query generator using Gemini"""
    
    ADVANCED_SQL_PROMPT = """You are an expert SQL query generator for agricultural data.

{schema}

RULES:
1. Only SELECT statements. NO INSERT, UPDATE, DELETE, DROP, ALTER
2. Use proper JOINs for multi-table queries
3. Use aggregation functions (SUM, AVG, COUNT, MAX, MIN) with GROUP BY
4. Always include LIMIT (max 1000 rows)
5. Use WHERE for filtering, ORDER BY for sorting

USER QUESTION: {question}

DATABASE STATS: {db_stats}

Return ONLY valid JSON (no markdown):
{{
    "sql_query": "Complete SQL query",
    "explanation": "Brief explanation",
    "tables_used": ["list"],
    "complexity": "simple|moderate|complex",
    "estimated_rows": number
}}"""

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash-exp"):
        self.client = GeminiAPIClient(api_key, model)
        self.schema = DatabaseSchema()
    
    def generate_query(self, question: str, db_path: str) -> Dict[str, Any]:
        try:
            db_stats = self.schema.get_table_info(db_path)
            
            prompt = self.ADVANCED_SQL_PROMPT.format(
                schema=self.schema.get_schema_description(),
                question=question,
                db_stats=json.dumps(db_stats, indent=2)
            )
            
            content = self.client.generate_content(prompt, temperature=0.1, max_tokens=1500)
            
            # Extract JSON
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            else:
                json_match = re.search(r'(\{.*\})', content, re.DOTALL)
                if json_match:
                    content = json_match.group(1)
            
            result = json.loads(content)
            
            if result.get("sql_query"):
                is_valid, error = self._validate_query(result["sql_query"])
                if not is_valid:
                    result["sql_query"] = ""
                    result["explanation"] = f"Validation failed: {error}"
            
            return result
            
        except Exception as e:
            print(f"❌ Query generation error: {str(e)}")
            return {
                "sql_query": "",
                "explanation": f"Error: {str(e)}",
                "tables_used": [],
                "complexity": "error",
                "estimated_rows": 0
            }
    
    def _validate_query(self, sql: str) -> Tuple[bool, Optional[str]]:
        sql_lower = sql.lower().strip()
        
        dangerous = ["insert", "update", "delete", "drop", "alter", "create", "truncate"]
        for keyword in dangerous:
            if keyword in sql_lower:
                return False, f"Dangerous keyword: {keyword}"
        
        if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
            return False, "Must start with SELECT or WITH"
        
        if "limit" not in sql_lower:
            return False, "LIMIT clause required"
        
        return True, None


class QueryExecutor:
    """Executes SQL queries"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def execute_query(self, sql: str, max_rows: int = 1000) -> QueryResult:
        start_time = datetime.now()
        
        try:
            if "limit" not in sql.lower():
                sql = f"{sql.rstrip(';')} LIMIT {max_rows};"
            
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(sql, conn)
            conn.close()
            
            tables_used = []
            for table in DatabaseSchema.SCHEMA_INFO.keys():
                if table in sql.lower():
                    tables_used.append(table)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return QueryResult(
                success=True,
                data=df,
                sql_query=sql,
                execution_time=execution_time,
                row_count=len(df),
                tables_used=tables_used
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
    """Generates natural language answers using Gemini"""
    
    SYNTHESIS_PROMPT = """You are an agricultural data analyst.

QUESTION: {question}
SQL QUERY: {sql_query}
RESULTS ({row_count} rows):
{results_preview}

Provide a clear answer (1 paragraphs) with specific numbers and insights."""

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash-exp"):
        self.client = GeminiAPIClient(api_key, model)
    
    def synthesize_answer(self, question: str, query_result: QueryResult, query_metadata: Dict[str, Any]) -> str:
        if not query_result.success:
            return f"❌ Error: {query_result.error_message}"
        
        if query_result.data is None or query_result.data.empty:
            return "📊 No data found. Try adjusting your filters."
        
        try:
            df = query_result.data
            preview = df.head(15).to_string(index=False)
            
            if len(df) > 15:
                preview += f"\n\n... ({len(df)} total rows)"
            
            prompt = self.SYNTHESIS_PROMPT.format(
                question=question,
                sql_query=query_result.sql_query,
                row_count=query_result.row_count,
                results_preview=preview
            )
            
            answer = self.client.generate_content(prompt, temperature=0.3, max_tokens=800)
            
            footer = f"\n\n---\n**Details:**\n"
            footer += f"- Tables: {', '.join(query_result.tables_used or [])}\n"
            footer += f"- Rows: {query_result.row_count:,}\n"
            footer += f"- Time: {query_result.execution_time:.2f}s"
            
            return answer + footer
            
        except Exception as e:
            print(f"⚠️  LLM synthesis failed, using fallback")
            return self._generate_fallback(question, query_result)
    
    def _generate_fallback(self, question: str, query_result: QueryResult) -> str:
        df = query_result.data
        answer = f"📊 **Results for:** {question}\n\n"
        
        if len(df) <= 10:
            answer += "**Complete Results:**\n```\n" + df.to_string(index=False) + "\n```\n"
        else:
            answer += f"**Top 10 Results** (out of {len(df)}):\n```\n"
            answer += df.head(10).to_string(index=False) + "\n```\n"
        
        return answer


class AgriculturalQASystem:
    """Main QA system using Google Gemini"""
    
    def __init__(self, db_path: str, api_key: Optional[str] = None, model: str = "gemini-2.0-flash-exp"):
        self.db_path = db_path
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        
        if not self.api_key:
            raise ValueError("Google API key required. Set GOOGLE_API_KEY or GEMINI_API_KEY in .env")
        
        self.query_generator = SQLQueryGenerator(self.api_key, model)
        self.query_executor = QueryExecutor(db_path)
        self.answer_synthesizer = AnswerSynthesizer(self.api_key, model)
    
    def answer_question(self, question: str) -> Dict[str, Any]:
        query_metadata = self.query_generator.generate_query(question, self.db_path)
        
        if not query_metadata.get("sql_query"):
            return {
                "success": False,
                "question": question,
                "answer": f"❌ Cannot generate query: {query_metadata.get('explanation')}",
                "sql_query": None,
                "data": None,
                "metadata": query_metadata
            }
        
        query_result = self.query_executor.execute_query(query_metadata["sql_query"])
        answer = self.answer_synthesizer.synthesize_answer(question, query_result, query_metadata)
        
        return {
            "success": query_result.success,
            "question": question,
            "answer": answer,
            "sql_query": query_result.sql_query,
            "data": query_result.data,
            "metadata": {
                **query_metadata,
                "execution_time": query_result.execution_time,
                "row_count": query_result.row_count,
                "tables_used": query_result.tables_used
            }
        }


# CLI Testing
if __name__ == "__main__":
    import sys
    
    DB_PATH = "data/agricultural_data.db"
    
    print("\n" + "=" * 80)
    print("SYSTEM DIAGNOSTICS")
    print("=" * 80)
    print(f"✓ Python: {sys.version.split()[0]}")
    print(f"✓ Database: {DB_PATH}")
    print(f"✓ DB exists: {os.path.exists(DB_PATH)}")
    
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    print(f"✓ GOOGLE_API_KEY: {'✅ Set' if api_key else '❌ Missing'}")
    
    if not os.path.exists(DB_PATH):
        print(f"\n❌ Database not found: {DB_PATH}")
        sys.exit(1)
    
    if not api_key:
        print("\n❌ Google API Key not found!")
        print("Set GOOGLE_API_KEY or GEMINI_API_KEY in your .env file")
        print("Get your key from: https://aistudio.google.com/app/apikey")
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print("AGRICULTURAL QA SYSTEM - TEST (Google Gemini)")
    print("=" * 80)
    
    try:
        qa_system = AgriculturalQASystem(DB_PATH)
        
        questions = [
            "Which state produced the most rice in 2014?",
            "Show top 5 districts by wheat production",
        ]
        
        for i, q in enumerate(questions, 1):
            print(f"\n{'='*80}\nQuestion {i}: {q}\n{'='*80}")
            result = qa_system.answer_question(q)
            print(f"\n📊 SQL:\n{result['sql_query']}\n")
            print(f"💬 Answer:\n{result['answer']}\n")
        
        print("\n✅ Test complete!")
        
    except Exception as e:
        print(f"\n❌ System error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()