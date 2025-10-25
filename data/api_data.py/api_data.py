import requests
from requests.exceptions import RequestException
import json
import csv
import sqlite3
import os
from datetime import datetime

# API Configuration
API_KEY = '579b464db66ec23bdd000001cb707f48193e4ff96953ee8fc80258d6'
RESOURCE_ID = '35be999b-0208-4354-b557-f6ca9a5355de'

# File paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SQL_FOLDER = os.path.join(PROJECT_ROOT, 'sql')
CSV_FOLDER = os.path.join(PROJECT_ROOT, 'data')

# Create folders if they don't exist
os.makedirs(SQL_FOLDER, exist_ok=True)
os.makedirs(CSV_FOLDER, exist_ok=True)

CSV_FILE = os.path.join(CSV_FOLDER, 'data_output.csv')
DB_FILE = os.path.join(SQL_FOLDER, 'crop_production.db')
TABLE_NAME = 'api_data'

# Construct the API endpoint URL
url = f"https://api.data.gov.in/resource/{RESOURCE_ID}?api-key={API_KEY}&format=json&limit=100"

def fetch_api_data():
    """Fetch data from the API"""
    try:
        response = requests.get(url, timeout=10)
        if response.status_code >= 400:
            raise RequestException(f"HTTP {response.status_code}: {response.text[:200]}")
        
        data = response.json()
        print("✓ Data fetched successfully from API")
        return data
    
    except RequestException as e:
        print(f"✗ Error fetching data: {e}")
        return None

def display_table(data, limit=10):
    """Display data in table format"""
    if not data:
        print("✗ No data to display")
        return
    
    records = data.get('records', []) if isinstance(data, dict) else data
    
    if not records:
        print("✗ No records found in data")
        return
    
    # Display limited records in simple format
    display_records = records[:limit]
    
    if display_records:
        # Get column names
        headers = list(display_records[0].keys())
        
        print("\n" + "=" * 100)
        print(f"API DATA (Showing {len(display_records)} of {len(records)} records)")
        print("=" * 100)
        
        # Print headers
        header_line = " | ".join([f"{h:20}" for h in headers[:5]])  # Show first 5 columns
        print(header_line)
        print("-" * len(header_line))
        
        # Print rows
        for record in display_records:
            row_values = []
            for header in headers[:5]:  # Show first 5 columns
                value = record.get(header, '')
                # Truncate long values
                if isinstance(value, str) and len(value) > 20:
                    value = value[:17] + '...'
                elif isinstance(value, (dict, list)):
                    value = str(value)[:17] + '...'
                row_values.append(f"{str(value):20}")
            print(" | ".join(row_values))
        
        print("=" * 100 + "\n")
    
    return records

def save_to_csv(data, filename=CSV_FILE, append_mode=True):
    """Convert JSON data to CSV"""
    if not data:
        print("✗ No data to save to CSV")
        return False
    
    records = data.get('records', []) if isinstance(data, dict) else data
    
    if not records:
        print("✗ No records found in data")
        return False
    
    # Get all unique keys from all records
    fieldnames = set()
    for record in records:
        if isinstance(record, dict):
            fieldnames.update(record.keys())
    
    fieldnames = sorted(list(fieldnames))
    
    try:
        # Check if file exists to determine write mode
        file_exists = os.path.exists(filename)
        mode = 'a' if (append_mode and file_exists) else 'w'
        
        with open(filename, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header only if new file or overwrite mode
            if mode == 'w' or not file_exists:
                writer.writeheader()
            
            writer.writerows(records)
        
        action = "appended to" if (mode == 'a' and file_exists) else "saved to"
        print(f"✓ Data {action} CSV: {filename}")
        return True
    
    except Exception as e:
        print(f"✗ Error saving to CSV: {e}")
        return False

def save_to_sqlite(data, db_file=DB_FILE, table_name=TABLE_NAME, append_mode=True):
    """Save JSON data to SQLite database with append mode"""
    if not data:
        print("✗ No data to save to database")
        return False
    
    records = data.get('records', []) if isinstance(data, dict) else data
    
    if not records:
        print("✗ No records found in data")
        return False
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Get all unique keys from all records
        columns = set()
        for record in records:
            if isinstance(record, dict):
                columns.update(record.keys())
        
        columns = sorted(list(columns))
        
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if not append_mode or not table_exists:
            # Create/recreate table
            columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            cursor.execute(f'''CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fetch_timestamp TEXT,
                {columns_def}
            )''')
            print(f"✓ Table '{table_name}' created")
        else:
            print(f"✓ Appending to existing table '{table_name}'")
        
        # Add timestamp column for tracking
        fetch_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Insert records
        columns_with_timestamp = ['fetch_timestamp'] + columns
        placeholders = ', '.join(['?' for _ in columns_with_timestamp])
       
        # Build the quoted column list safely, then format the insert query
        quoted_cols = ", ".join([f'"{col}"' for col in columns_with_timestamp])
        insert_query = f'INSERT INTO {table_name} ({quoted_cols}) VALUES ({placeholders})'
        
        inserted_count = 0
        for record in records:
            if isinstance(record, dict):
                values = [fetch_time]
                for col in columns:
                    val = record.get(col, None)
                    if isinstance(val, (dict, list)):
                        val = json.dumps(val)
                    # Ensure we always append a string (use empty string for None)
                    values.append(str(val) if val is not None else "")
                
                cursor.execute(insert_query, values)
                inserted_count += 1
        
        conn.commit()
        
        # Get total count
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        total_count = cursor.fetchone()[0]
        
        print(f"✓ Data saved to SQLite: {db_file}")
        print(f"  Table: {table_name}")
        print(f"  Records inserted: {inserted_count}")
        print(f"  Total records in database: {total_count}")
        
        conn.close()
        return True
    
    except Exception as e:
        print(f"✗ Error saving to SQLite: {e}")
        if 'conn' in locals():
            conn.close() # pyright: ignore[reportPossiblyUnboundVariable]
        return False

def view_database_table(db_file=DB_FILE, table_name=TABLE_NAME, limit=10):
    """View data from SQLite database in table format"""
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            print(f"✗ Table '{table_name}' does not exist")
            conn.close()
            return
        
        # Get total count
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        total_count = cursor.fetchone()[0]
        
        # Fetch data
        cursor.execute(f'SELECT * FROM {table_name} ORDER BY id DESC LIMIT {limit}')
        rows = cursor.fetchall()
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        
        print("\n" + "=" * 100)
        print(f"DATABASE TABLE: {table_name}")
        print(f"Total Records: {total_count} | Showing: {len(rows)} (latest entries)")
        print("=" * 100)
        
        # Print headers (first 5 columns)
        header_line = " | ".join([f"{h:20}" for h in columns[:5]])
        print(header_line)
        print("-" * len(header_line))
        
        # Print rows
        for row in rows:
            row_values = []
            for value in row[:5]:  # Show first 5 columns
                if isinstance(value, str) and len(value) > 20:
                    value = value[:17] + '...'
                row_values.append(f"{str(value):20}")
            print(" | ".join(row_values))
        
        print("=" * 100 + "\n")
        
        conn.close()
    
    except Exception as e:
        print(f"✗ Error viewing database: {e}")

def main():
    """Main function to orchestrate the workflow"""
    print("\n" + "=" * 100)
    print("API Data Extraction, CSV Conversion & SQLite Storage (APPEND MODE)")
    print("=" * 100)
    print()
    
    # Configuration
    APPEND_MODE = True  # Set to False to overwrite data each time
    
    # Step 1: Fetch data from API
    print("Step 1: Fetching data from API...")
    data = fetch_api_data()
    
    if data:
        print()
        
        # Step 2: Display data in table format
        print("Step 2: Displaying API data in table format...")
        display_table(data, limit=10)
        
        # Step 3: Save to CSV (append mode)
        print("Step 3: Saving to CSV...")
        save_to_csv(data, append_mode=APPEND_MODE)
        print()
        
        # Step 4: Save to SQLite (append mode)
        print("Step 4: Saving to SQLite database...")
        save_to_sqlite(data, append_mode=APPEND_MODE)
        print()
        
        # Step 5: View database contents
        print("Step 5: Viewing database contents...")
        view_database_table(limit=10)
        
        print("=" * 100)
        print("✓ Process completed successfully!")
        print("=" * 100)
        print(f"\nNote: Running in {'APPEND' if APPEND_MODE else 'OVERWRITE'} mode")
        print("Each run will add new records to the database with timestamps.")
    else:
        print("\n✗ Process failed: Unable to fetch data from API")

if __name__ == "__main__":
    main()