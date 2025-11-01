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

# --- Helpers: numeric parsing and snake_case cleaning ---
import re
from typing import Any, Tuple

def parse_numeric_keep_raw(value: Any) -> Tuple[Optional[float], Optional[str]]:
    """
    Parse a numeric-ish string into a float (if possible) while returning
    the raw string for a `_raw` column.

    Returns: (numeric_value_or_None, raw_str_or_None)
    Examples:
      "3,45,000" -> (345000.0, "3,45,000")
      "12.5 Cr"  -> (12.5, "12.5 Cr")
      "-" or "NA" -> (None, "-")
    """
    if value is None:
        return None, None

    raw = str(value).strip()
    if raw == "" or raw.lower() in {"na", "n/a", "-", "null", "none"}:
        return None, raw

    # Remove commas, non-numeric unit words (but preserve decimals and minus)
    # We keep numbers with decimal points. If formatted as "12.5 Cr" -> keep 12.5
    # For values like "2,500 Ha" -> "2500" -> float
    # For values with lakhs/crore notation (e.g., "12.5 Cr"), we DO NOT expand units automatically.
    # We simply extract the first numeric token.
    numeric_token_match = re.search(r"-?\d+(?:[\.,]\d+)?", raw)
    if not numeric_token_match:
        return None, raw

    token = numeric_token_match.group(0)
    # unify commas to empty and dot to decimal
    token = token.replace(",", "")
    try:
        num = float(token)
        return num, raw
    except:
        return None, raw

def clean_column_name(col: str) -> str:
    """
    Convert arbitrary column name to snake_case and remove illegal chars.
    Example: 'State/UT' -> 'state_ut', 'S.No.' -> 's_no'
    """
    if not col:
        return col
    s = str(col).strip()
    s = re.sub(r"[^\w\s]", "_", s)           # non-word -> underscore
    s = re.sub(r"\s+", "_", s)               # spaces -> underscore
    s = re.sub(r"_+", "_", s)                # collapse multiples
    s = s.strip("_").lower()
    return s


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
        },
        'climate_expenditure': {
            'resource_id': '2dbfb6d3-b35f-4704-86e0-b22643889ddc',  # Climate action expenditure
            'table_name': 'climate_expenditure',
            'description': 'climate schema expenditure',
            'enabled': True
        },
        'damages_floods': {
            'resource_id': '081dccd1-bb4b-44af-b565-940b965f266b',  # Climate action expenditure
            'table_name': 'damages_floods',
            'description': 'Damages from floods',
            'enabled': True
        },
        'damages_rainfall': {
            'resource_id': '465b359b-d50e-4411-9df0-d29c208b817e',  # Climate action expenditure
            'table_name': 'damages_rainfall',
            'description': 'Damages from rainfall',
            'enabled': True
        },
        'extreme_temperature': {
            'resource_id': 'ce8ee775-9bd4-4852-9fe1-74ca4cfdbe73',  # Climate action expenditure
            'table_name': 'extreme_temperature',
            'description': 'State-wise Extreme Temperature',
            'enabled': True
        },
        'drought_cases': {
            'resource_id': '9c215261-d898-4244-941d-3497de31e746',  # Climate action expenditure
            'table_name': 'drought_cases',
            'description': 'State-wise Drought Cases',
            'enabled': True
        },
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
                Wheat REAL,
                Maize REAL,
                Rice REAL,
                Barley REAL,
                Ragi REAL,
                Pulses REAL,
                common_millets REAL,
                Total REAL,
                Chillies REAL,
                Ginger REAL,
                Oil_seeds REAL
            )
        """)
        
        # Expenditure Data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Agency_Rainfall (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                State TEXT, 
                District TEXT, 
                Date REAL, 
                Year REAL, 
                Month REAL, 
                Avg_rainfall REAL, 
                Agency_name REAL,
                fetch_timestamp REAL
            )
        """)
        # Climate Expenditure
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS climate_expenditure (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            scheme_name TEXT,
            category TEXT,
            expenditure_crore REAL,
            expenditure_crore_raw TEXT,
            source TEXT,
            fetch_timestamp TEXT
        )
        """)

        # Damages from floods
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS damages_floods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            s_no INTEGER,
            state_ut TEXT,
            area_affected_hectare REAL,
            area_affected_hectare_raw TEXT,
            damage_to_crop_hectare REAL,
            damage_to_crop_hectare_raw TEXT,
            livestock_loss_count INTEGER,
            livestock_loss_count_raw TEXT,
            human_lives_lost INTEGER,
            human_lives_lost_raw TEXT,
            source TEXT,
            fetch_timestamp TEXT
        )
        """)

        # Damages from rainfall
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS damages_rainfall (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            state_ut TEXT,
            crop_damage_hectare REAL,
            crop_damage_hectare_raw TEXT,
            crop_damage_value_crore REAL,
            crop_damage_value_crore_raw TEXT,
            rainfall_type TEXT,
            source TEXT,
            fetch_timestamp TEXT
        )
        """)

        # Extreme temperature events
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS extreme_temperature (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            state_ut TEXT,
            extreme_event_type TEXT,
            event_days INTEGER,
            event_days_raw TEXT,
            deaths INTEGER,
            deaths_raw TEXT,
            source TEXT,
            fetch_timestamp TEXT
        )
        """)

        # Drought cases
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS drought_cases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            state_ut TEXT,
            drought_severity TEXT,
            area_affected_hectare REAL,
            area_affected_hectare_raw TEXT,
            crop_loss_value_crore REAL,
            crop_loss_value_crore_raw TEXT,
            source TEXT,
            fetch_timestamp TEXT
        )
        """)

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
                    record.get('state_ut') or ''
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

    def normalize_climate_expenditure_data(self, records: List[Dict]) -> List[tuple]:
        """
        Expected canonical output tuple:
        (year, scheme_name, category, expenditure_crore, expenditure_crore_raw, source, fetch_timestamp)
            """
        normalized = []
        ts = datetime.now().isoformat()
        for rec in records:
            try:
                year_raw = rec.get('year') or rec.get('Year') or rec.get('financial_year') or None
                year = None
                try:
                    year = int(float(year_raw)) if year_raw not in (None, "") else None
                except:
                    year = None

                scheme = (rec.get('scheme') or rec.get('scheme_name') or rec.get('Schemes') or "").strip()
                category = (rec.get('category') or rec.get('type') or "").strip()
                exp_val, exp_raw = parse_numeric_keep_raw(rec.get('expenditure') or rec.get('Actual_Expenditure') or rec.get('Actual_Expenditure_INR') or rec.get('amount') or None)
                source = rec.get('source') or rec.get('data_source') or 'data.gov.in'

                normalized.append((year, scheme, category, exp_val, exp_raw, source, ts))
            except Exception:
                continue
        return normalized

    def normalize_damages_floods_data(self, records: List[Dict]) -> List[tuple]:
        """
        Output tuple order:
        (year, s_no, state_ut, area_affected_hectare, area_affected_hectare_raw, damage_to_crop_hectare, damage_to_crop_hectare_raw,
        livestock_loss_count, livestock_loss_count_raw, human_lives_lost, human_lives_lost_raw, source, fetch_timestamp)
        """
        normalized = []
        ts = datetime.now().isoformat()
        for rec in records:
            try:
                year = None
                yraw = rec.get('year') or rec.get('Year') or rec.get('Year_of_event')
                try:
                    year = int(float(yraw)) if yraw not in (None, "") else None
                except:
                    year = None

                s_no = None
                if rec.get('s_no') or rec.get('S.No') or rec.get('sl_no'):
                    try:
                        val = rec.get('s_no') or rec.get('S.No') or rec.get('sl_no')
                        s_no = int(float(val)) if val not in (None, "", "NA") else None

                    except:
                        s_no = None

                state_ut = (rec.get('state') or rec.get('state_ut') or rec.get('State/UT') or rec.get('State') or "").strip()

                area_val, area_raw = parse_numeric_keep_raw(rec.get('area_affected') or rec.get('area') or rec.get('area_affected_hectare'))
                damage_crop_val, damage_crop_raw = parse_numeric_keep_raw(rec.get('damage_to_crops_area') or rec.get('damage_to_crops') or rec.get('damage_to_crop_hectare'))
                livestock_val, livestock_raw = parse_numeric_keep_raw(rec.get('livestock_loss') or rec.get('livestock_loss_count'))
                human_lives_val, human_lives_raw = parse_numeric_keep_raw(rec.get('human_losses') or rec.get('human_lives_lost'))

                source = rec.get('source') or rec.get('data_source') or 'data.gov.in'

                normalized.append((
                    year, s_no, state_ut,
                    area_val, area_raw,
                    damage_crop_val, damage_crop_raw,
                    int(livestock_val) if livestock_val is not None else None, livestock_raw,
                    int(human_lives_val) if human_lives_val is not None else None, human_lives_raw,
                    source, ts
                ))
            except Exception:
                continue
        return normalized

    def normalize_damages_rainfall_data(self, records: List[Dict]) -> List[tuple]:
        """
        Tuple order:
        (year, state_ut, crop_damage_hectare, crop_damage_hectare_raw, crop_damage_value_crore, crop_damage_value_crore_raw,
        rainfall_type, source, fetch_timestamp)
        """
        normalized = []
        ts = datetime.now().isoformat()
        for rec in records:
            try:
                year = None
                yraw = rec.get('year') or rec.get('Year') or None
                try:
                    year = int(float(yraw)) if yraw not in (None, "") else None
                except:
                    year = None

                state_ut = (rec.get('state') or rec.get('state_ut') or rec.get('State') or "").strip()
                dmg_area, dmg_area_raw = parse_numeric_keep_raw(rec.get('area_affected') or rec.get('crop_damage_area') or rec.get('Damage_to_Crops_area'))
                dmg_value, dmg_value_raw = parse_numeric_keep_raw(rec.get('damage_value') or rec.get('Damage_to_Crops') or rec.get('damage_to_crops_value'))
                rainfall_type = (rec.get('rainfall_type') or rec.get('type') or "").strip()
                source = rec.get('source') or 'data.gov.in'

                normalized.append((year, state_ut, dmg_area, dmg_area_raw, dmg_value, dmg_value_raw, rainfall_type, source, ts))
            except Exception:
                continue
        return normalized

    def normalize_extreme_temperature_data(self, records: List[Dict]) -> List[tuple]:
        """
        Tuple order:
        (year, state_ut, extreme_event_type, event_days, event_days_raw, deaths, deaths_raw, source, fetch_timestamp)
        """
        normalized = []
        ts = datetime.now().isoformat()
        for rec in records:
            try:
                year = None
                yraw = rec.get('year') or rec.get('Year') or rec.get('date') or None
                try:
                    year = int(float(yraw)) if yraw not in (None, "") else None
                except:
                    # try to parse from date
                    try:
                        d = str(rec.get('date') or rec.get('Date') or "")
                        if d:
                            year = int(d[:4])
                    except:
                        year = None

                state_ut = (rec.get('state') or rec.get('State') or rec.get('state_ut') or "").strip()
                event_type = (rec.get('event') or rec.get('extreme_event_type') or rec.get('type') or "").strip()
                days, days_raw = parse_numeric_keep_raw(rec.get('days') or rec.get('no_of_days') or rec.get('event_days'))
                deaths, deaths_raw = parse_numeric_keep_raw(rec.get('deaths') or rec.get('no_of_deaths') or rec.get('fatalities'))
                source = rec.get('source') or 'data.gov.in'

                normalized.append((year, state_ut, event_type, int(days) if days is not None else None, days_raw, int(deaths) if deaths is not None else None, deaths_raw, source, ts))
            except Exception:
                continue
        return normalized

    def normalize_drought_cases_data(self, records: List[Dict]) -> List[tuple]:
        """
        Tuple order:
        (year, state_ut, drought_severity, area_affected_hectare, area_affected_hectare_raw, crop_loss_value_crore, crop_loss_value_crore_raw, source, fetch_timestamp)
        """
        normalized = []
        ts = datetime.now().isoformat()
        for rec in records:
            try:
                year = None
                yraw = rec.get('year') or rec.get('Year') or None
                try:
                    year = int(float(yraw)) if yraw not in (None, "") else None
                except:
                    year = None

                state_ut = (rec.get('state') or rec.get('State') or rec.get('state_ut') or "").strip()
                severity = (rec.get('severity') or rec.get('drought_severity') or '').strip()
                area, area_raw = parse_numeric_keep_raw(rec.get('area_affected') or rec.get('area') or rec.get('area_affected_hectare'))
                crop_loss, crop_loss_raw = parse_numeric_keep_raw(rec.get('crop_loss_value') or rec.get('crop_loss_value_crore') or rec.get('loss_value'))
                source = rec.get('source') or 'data.gov.in'

                normalized.append((year, state_ut, severity, area, area_raw, crop_loss, crop_loss_raw, source, ts))
            except Exception:
                continue
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
                columns = ['State', 'District', 'Date', 'Year', 'Month', 'Avg_rainfall', 'Agency_name'
                          ]
            elif source_key == 'damages_floods':
                normalized = self.normalize_damages_floods_data(data['records'])
                columns = [ 'year','s_no', 'state_ut', 'area_affected_hectare', 'area_affected_hectare_raw', 
                            'damage_to_crop_hectare','damage_to_crop_hectare_raw', 'livestock_loss_count', 'livestock_loss_count_raw', 
                            'human_lives_lost', 'human_lives_lost_raw', 'source', 'fetch_timestamp'
                          ]
            elif source_key == 'damages_rainfall':
                normalized = self.normalize_damages_rainfall_data(data['records'])
                columns = ['year', 'state_ut', 'crop_damage_hectare', 'crop_damage_hectare_raw',
                           'crop_damage_value_crore', 'crop_damage_value_crore_raw','rainfall_type',
                           'source','fetch_timestamp'
                          ]
            elif source_key == 'extreme_temperature':
                normalized = self.normalize_extreme_temperature_data(data['records'])
                columns = ['year','state_ut','extreme_event_type','event_days',
                           'event_days_raw','deaths','deaths_raw','source', 'fetch_timestamp'
                          ]
            elif source_key == 'drought_cases':
                normalized = self.normalize_drought_cases_data(data['records'])
                columns = ['year','state_ut','drought_severity','area_affected_hectare', 'area_affected_hectare_raw',
                            'crop_loss_value_crore','crop_loss_value_crore_raw','source','fetch_timestamp'
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
        tables = ['crop_production', 'rainfall_district', 'production_crop_specific', 'Agency_Rainfall', 
                  'damages_floods', 'damages_rainfall', 'extreme_temperature', 'drought_cases']
        
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
