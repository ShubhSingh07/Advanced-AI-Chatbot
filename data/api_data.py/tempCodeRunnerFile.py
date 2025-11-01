"""
Fixed Multi-Source Agricultural & Climate Data Fetcher
With correct data.gov.in resource IDs and error handling
"""

import requests
import sqlite3
import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class MultiSourceDataFetcher:
    """Fetches and stores data from multiple government data sources"""
    
    # CORRECTED Data source configurations from data.gov.in
    DATA_SOURCES = {
        'crop_production': {
            'resource_id': '35be999b-0208-4354-b557-f6ca9a5355de',
            'table_name': 'crop_production',
            'description': 'Agricultural Crop Production Statistics',
            'enabled': True
        },
        'rainfall_district': {
            'resource_id': '8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f',  # District rainfall normal
            'table_name': 'rainfall_district',
            'description': 'Daily District-wise Rainfall Data',
            'enabled': True
        },
        'production_crop_specific': {
            'resource_id': 'f20d7d45-e3d8-4603-bc79-15a3d0db1f9a', 
            'table_name': 'production_crop_specific',
            'description': 'Specific Crop Comparison',
            'enabled': True
        },
        'Agency_Rainfall': {
            'resource_id': '6c05cd1b-ed59-40c2-bc31-e314f39c6971',  # Climate action expenditure
            'table_name': 'Agency_Rainfall',
            'description': 'District-wise Agency Rainfall',
            'enabled': True
        }
    }
    
    def __init__(self, api_key: str, db_path: str = 'data/agricultural_data.db'):
        self.api_key = api_key
        self.db_path = db_path
        self.base_url = "https://api.data.gov.in/resource"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else "data", exist_ok=True)
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize database with all required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Crop Production table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crop_production (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT,
                district TEXT,
                crop TEXT,
                season TEXT,
                year INTEGER,
                area REAL,
                production REAL,
                data_source TEXT,
                fetch_timestamp TEXT
            )
        """)
        
        # Rainfall Data table (District Normal)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rainfall_district (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                SUBDIVISION TEXT,
                YEAR REAL,
                jan REAL,
                feb REAL,
                mar REAL,
                apr REAL,
                may REAL,
                jun REAL,
                jul REAL,
                aug REAL,
                sep REAL,
                oct REAL,
                nov REAL,
                dec REAL,
                annual REAL
            )
        """)
        
        # CROP PRODUCTION CROP-SPECIFIC
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS production_crop_specific (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                State TEXT,
                District TEXT,
                Wheat INT,
                Maize INT,
                Rice INT,
                Barley INT,
                Ragi INT,
                Pulses INT,
                common_millets INT,
                Total INT,
                Chillies INT,
                Ginger INT,
                Oil_seeds INT
            )
        """)
        
        # Expenditure Data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Agency_Rainfall (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                State TEXT,
                District TEXT,
                Date,
                Year INT,
                Month TEXT,
                Avg_rainfall INT,
                Agency_name TEXT
            )
        """)
        
        # Metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT UNIQUE,
                resource_id TEXT,
                last_updated TEXT,
                record_count INTEGER,
                description TEXT,
                fetch_status TEXT
            )
        """)
        
        conn.commit()
        conn.close()
        print("✅ Database initialized with all tables")
    
    def fetch_data(self, resource_id: str, limit: int = 1000, offset: int = 0, 
                   max_retries: int = 3) -> Optional[Dict]:
        """Fetch data from data.gov.in API with retry logic"""
        url = f"{self.base_url}/{resource_id}"
        params = {
            'api-key': self.api_key,
            'format': 'json',
            'limit': limit,
            'offset': offset
        }
        
        for attempt in range(max_retries):
            try:
                print(f"  Attempting fetch (try {attempt + 1}/{max_retries})...")
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # Check if we got valid data
                if 'records' in data and data['records']:
                    print(f"  ✓ Fetched {len(data['records'])} records")
                    return data
                else:
                    print(f"  ⚠️ No records found in response")
                    return None
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"  ❌ Resource not found (404): {resource_id}")
                    return None
                elif e.response.status_code == 429:
                    wait_time = 2 ** attempt
                    print(f"  ⚠️ Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  ❌ HTTP Error {e.response.status_code}")
                    if attempt == max_retries - 1:
                        return None
            except requests.exceptions.Timeout:
                print(f"  ⚠️ Timeout. Retrying...")
                if attempt < max_retries - 1:
                    time.sleep(2)
            except Exception as e:
                print(f"  ❌ Error: {str(e)}")
                if attempt == max_retries - 1:
                    return None
        
        return None
    
    def normalize_crop_data(self, records: List[Dict]) -> List[tuple]:
        """Normalize crop production data"""
        normalized = []
        timestamp = datetime.now().isoformat()
        
        for record in records:
            try:
                # Try different field name variations
                state = (record.get('state_name') or record.get('State') or 
                        record.get('state') or '').strip()
                district = (record.get('district_name') or record.get('District') or 
                           record.get('district') or '').strip()
                crop = (record.get('crop') or record.get('Crop') or '').strip()
                season = (record.get('season') or record.get('Season') or '').strip()
                
                year_val = record.get('year') or record.get('Year') or record.get('crop_year') or 0
                year = int(year_val) if year_val else 0
                
                # Handle area and production
                area_str = str(record.get('area') or record.get('Area') or '0')
                prod_str = str(record.get('production') or record.get('Production') or '0')
                
                area = float(area_str) if area_str not in ['NA', 'N/A', '', 'null'] else 0.0
                production = float(prod_str) if prod_str not in ['NA', 'N/A', '', 'null'] else 0.0
                
                if state and year > 0:  # Only add if we have minimum required data
                    normalized.append((
                        state, district, crop, season, year, area, production,
                        'data.gov.in_crop_production', timestamp
                    ))
            except Exception as e:
                continue
        
        return normalized
    
    def normalize_rainfall_district_data(self, records: List[Dict]) -> List[tuple]:
        """
        Normalize district rainfall data to match rainfall_district table schema:
        (SUBDIVISION, YEAR, jan, feb, ..., dec, annual)
        """
        normalized = []
        current_year = datetime.now().year

        for record in records:
            try:
                # Extract subdivision / district name
                subdivision = (
                    record.get('subdivision') or
                    record.get('SUBDIVISION') or
                    record.get('district_name') or
                    record.get('District') or
                    record.get('district') or
                    record.get('state_ut_name') or
                    record.get('State') or
                    record.get('state') or ''
                ).strip()

                # Extract year if available, else use current year
                year = record.get('year') or record.get('YEAR') or current_year
                try:
                    year = float(year)
                except:
                    year = float(current_year)

                # Monthly rainfall values
                months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                        'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
                rainfall_values = []

                for month in months:
                    val_str = str(record.get(month) or record.get(month.upper()) or '0')
                    try:
                        val = float(val_str) if val_str not in ['NA', '', 'null'] else 0.0
                    except:
                        val = 0.0
                    rainfall_values.append(val)

                # Annual total
                annual_str = str(record.get('annual') or record.get('ANNUAL') or '0')
                try:
                    annual = float(annual_str) if annual_str not in ['NA', '', 'null'] else sum(rainfall_values)
                except:
                    annual = sum(rainfall_values)

                # Append normalized row (matching DB schema)
                if subdivision:
                    normalized.append((
                        subdivision,
                        year,
                        *rainfall_values,
                        annual
                    ))

            except Exception as e:
                continue

        return normalized
    
    def normalize_production_crop_specific(self, records: List[Dict]) -> List[tuple]:
        """Normalize crop production data to match production_crop_specific table schema"""

        normalized = []
        skipped_no_state_district = 0
        skipped_errors = 0
        
        print(f"  🔍 DEBUG: Starting normalization of {len(records)} records")
        
        # Print first record to see structure
        if records:
            print(f"  📋 Sample record keys: {list(records[0].keys())}")
            print(f"  📋 Sample record: {records[0]}")
        
        for idx, record in enumerate(records):
            try:
                # Extract State and District - THE API USES LOWERCASE!
                state = (record.get('state') or record.get('State') or 
                        record.get('state_ut') or '').strip()
                district = (record.get('district') or record.get('District') or 
                            record.get('district_name') or '').strip()

                if not state or not district:
                    skipped_no_state_district += 1
                    if skipped_no_state_district <= 3:
                        print(f"  ⚠️  Record {idx}: Missing state/district")
                        print(f"      Available keys: {list(record.keys())}")
                    continue

                # Helper to safely parse int values
                def to_int(value):
                    try:
                        if value in [None, "", "NA", "null", "Null", "NULL"]:
                            return 0
                        return int(float(value))
                    except:
                        return 0

                # ✅ FIXED: Use the ACTUAL field names from the API
                wheat = to_int(record.get('wheat_in_metric_tonnes_'))
                maize = to_int(record.get('maize_in_metric_tonnes_'))
                rice = to_int(record.get('rice_in_metric_tonnes_'))
                barley = to_int(record.get('barley_in_metric_tonnes_'))
                ragi = to_int(record.get('ragi_in_metric_tonnes_'))
                pulses = to_int(record.get('pulses_in_metric_tonnes_'))
                common_millets = to_int(record.get('common_millets_in_metric_tonnes_'))
                total = to_int(record.get('total_food_grains_in_metric_tonnes_'))
                chillies = to_int(record.get('chillies_in_metric_tonnes_'))
                ginger = to_int(record.get('ginger_in_metric_tonnes_'))
                oil_seeds = to_int(record.get('oil_seeds_in_metric_tonnes_'))

                normalized.append((
                    state, district, wheat, maize, rice, barley, ragi, pulses,
                    common_millets, total, chillies, ginger, oil_seeds
                ))
                
                # Show first successful record
                if len(normalized) == 1:
                    print(f"  ✅ First normalized record: State={state}, District={district}, Wheat={wheat}, Rice={rice}")

            except Exception as e:
                skipped_errors += 1
                if skipped_errors <= 3:
                    print(f"  ❌ Record {idx} error: {str(e)}")
                    print(f"      Record: {record}")
                continue
        
        print(f"\n  📊 Normalization Summary:")
        print(f"     Total records processed: {len(records)}")
        print(f"     Successfully normalized: {len(normalized)}")
        print(f"     Skipped (no state/district): {skipped_no_state_district}")
        print(f"     Skipped (errors): {skipped_errors}")
        
        return normalized
    def normalize_Agency_Rainfall_data(self, records: List[Dict]) -> List[tuple]:
        """
        Normalize agency rainfall data to match Agency_Rainfall table schema:
        (State, District, Date, Year, Month, Avg_rainfall, Agency_name)
        """

        normalized = []
        skipped_no_state = 0
        skipped_errors = 0

        print(f"  🔍 DEBUG: Starting normalization of {len(records)} Agency Rainfall records")

        # Print first record structure
        if records:
            print(f"  📋 Sample record keys: {list(records[0].keys())}")
            print(f"  📋 Sample record: {records[0]}")

        for idx, record in enumerate(records):
            try:
                # Extract State & District
                state = (record.get('state') or record.get('State') or
                        record.get('state_ut') or record.get('State/UT') or '').strip()

                district = (record.get('district') or record.get('District') or
                            record.get('district_name') or '').strip()

                if not state:
                    skipped_no_state += 1
                    if skipped_no_state <= 3:
                        print(f"  ⚠️  Record {idx}: Missing state")
                        print(f"      Available keys: {list(record.keys())}")
                    continue

                # Extract Date (string format retained)
                date_val = record.get('date') or record.get('Date') or record.get('observation_date') or ''
                date_str = str(date_val).strip()

                # Extract year
                year_val = record.get('year') or record.get('Year')
                if not year_val and date_str:
                    try:
                        year_val = int(date_str[:4])  # Expected YYYY-MM-DD
                    except:
                        year_val = None
                year = int(year_val) if year_val else None

                # Extract month
                month = (record.get('month') or record.get('Month') or '').strip()
                if not month and date_str:
                    try:
                        # Extract MM from YYYY-MM-DD and convert to short name
                        month_num = int(date_str[5:7])
                        month = datetime.strptime(str(month_num), "%m").strftime("%b")  # e.g., "Jan"
                    except:
                        month = ''

                # Parse rainfall
                rf = record.get('avg_rainfall') or record.get('rainfall_mm') or record.get('rainfall') or 0
                try:
                    avg_rainfall = float(rf)
                except:
                    avg_rainfall = 0.0

                # Extract agency name
                agency = (record.get('agency') or record.get('Agency') or
                        record.get('source') or record.get('data_source') or '').strip()

                # Append normalized row (matching DB schema)
                if state and year:
                    normalized.append((
                        state, district, date_str, year, month, avg_rainfall, agency
                    ))

                    # Show first success
                    if len(normalized) == 1:
                        print(f"  ✅ First normalized record: State={state}, Year={year}, Rainfall={avg_rainfall}")

            except Exception as e:
                skipped_errors += 1
                if skipped_errors <= 3:
                    print(f"  ❌ Error in record {idx}: {str(e)}")
                    print(f"      Record: {record}")
                continue

        print(f"\n  📊 Normalization Summary (Agency Rainfall):")
        print(f"     Total records processed: {len(records)}")
        print(f"     Successfully normalized: {len(normalized)}")
        print(f"     Skipped (no state): {skipped_no_state}")
        print(f"     Skipped (errors): {skipped_errors}")

        return normalized

    
    def save_to_database(self, table_name: str, data: List[tuple], 
                        columns: List[str]) -> Tuple[int, str]:
        """Save normalized data to database"""
        if not data:
            return 0, "No data to save"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in columns])
        column_names = ','.join(columns)
        
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        try:
            cursor.executemany(query, data)
            conn.commit()
            inserted = len(data)
            
            # Update metadata
            cursor.execute("""
                INSERT OR REPLACE INTO data_metadata 
                (table_name, last_updated, record_count, fetch_status)
                VALUES (?, ?, (SELECT COUNT(*) FROM {table}), 'success')
            """.format(table=table_name), (table_name, datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            return inserted, "success"
        except Exception as e:
            conn.rollback()
            conn.close()
            return 0, f"Error: {str(e)}"
    
    def fetch_all_sources(self, verbose: bool = True):
        """Fetch data from all configured sources"""
        if verbose:
            print("\n" + "="*80)
            print("🚀 Multi-Source Data Fetching Started")
            print("="*80 + "\n")
        
        results = {}
        
        for source_key, source_config in self.DATA_SOURCES.items():
            if not source_config.get('enabled', True):
                continue
                
            if verbose:
                print(f"📊 Fetching {source_config['description']}...")
                print(f"   Resource ID: {source_config['resource_id']}")
            
            data = self.fetch_data(source_config['resource_id'], limit=1000)
            
            if not data or 'records' not in data or not data['records']:
                results[source_key] = {'count': 0, 'status': 'no_data'}
                if verbose:
                    print(f"   ⚠️ No data available\n")
                continue
            
            # Normalize based on source type
            if source_key == 'crop_production':
                normalized = self.normalize_crop_data(data['records'])
                columns = ['state', 'district', 'crop', 'season', 'year', 'area', 
                          'production', 'data_source', 'fetch_timestamp']
            elif source_key == 'rainfall_district':
                normalized = self.normalize_rainfall_district_data(data['records'])
                columns = ['SUBDIVISION', 'YEAR', 'jan', 'feb', 'mar', 'apr', 'may', 'jun',
                          'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'annual',
                          ]
            elif source_key == 'production_crop_specific':
                normalized = self.normalize_production_crop_specific(data['records'])
                columns = ['State', 'District', 'Wheat', 'Maize',
                            'Rice', 'Barley', 'Ragi', 'Pulses', 'common_millets',
                            'Total', 'Chillies', 'Ginger', 'Oil_seeds'
                          ]
            elif source_key == 'Agency_Rainfall':
                normalized = self.normalize_Agency_Rainfall_data(data['records'])
                columns = ['State', 'District', 'Date', 'Year',
                            'Month', 'Avg_rainfall', 'Agency_name'
                          ]
            else:
                continue
            
            count, status = self.save_to_database(
                source_config['table_name'],
                normalized,
                columns
            )
            
            results[source_key] = {'count': count, 'status': status}
            
            if verbose:
                if count > 0:
                    print(f"   ✅ Saved {count} records to {source_config['table_name']}\n")
                else:
                    print(f"   ⚠️ {status}\n")
        
        if verbose:
            print("="*80)
            print("✅ Data Fetching Complete")
            print(f"\nSummary:")
            for source, result in results.items():
                status_icon = "✓" if result['count'] > 0 else "⚠"
                print(f"  {status_icon} {source}: {result['count']} records ({result['status']})")
            print("="*80 + "\n")
        
        return results
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {}
        tables = ['crop_production', 'rainfall_district', 'production_crop_specific', 'Agency_Rainfall']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # Get date range if applicable
                date_range = None
                if table == 'crop_production':
                    cursor.execute(f"SELECT MIN(year), MAX(year) FROM {table} WHERE year > 0")
                    result = cursor.fetchone()
                    if result and result[0]:
                        date_range = f"{result[0]}-{result[1]}"
                elif table == 'production_crop_specific':
                    cursor.execute(f"SELECT MIN(state), MAX(state) FROM {table}")
                    result = cursor.fetchone()
                    if result and result[0]:
                        date_range = f"{result[0]}-{result[1]}"
                
                stats[table] = {
                    'count': count,
                    'date_range': date_range
                }
            except Exception as e:
                stats[table] = {'count': 0, 'error': str(e)}
        
        conn.close()
        return stats


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    API_KEY = os.getenv('YOUR_API_KEY', '579b464db66ec23bdd000001cb707f48193e4ff96953ee8fc80258d6')
    
    print("Initializing Multi-Source Data Fetcher...")
    fetcher = MultiSourceDataFetcher(API_KEY)
    
    print("\nFetching data from all sources...")
    results = fetcher.fetch_all_sources()
    
    print("\n📈 Final Database Statistics:")
    stats = fetcher.get_database_stats()
    for table, info in stats.items():
        if isinstance(info, dict) and 'count' in info:
            range_str = f" ({info['date_range']})" if info.get('date_range') else ""
            print(f"  {table}: {info['count']:,} records{range_str}")
        else:
            print(f"  {table}: Error - {info}")