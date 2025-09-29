#!/usr/bin/env python3
"""
CDC Consistency Check Script
Phase 3: Main consistency validation system for Moneyball PostgreSQL to Snowflake CDC pipeline
"""

import os
import sys
import yaml
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Optional

import psycopg2
import snowflake.connector
from dotenv import load_dotenv
import click
import pytz
from colorama import init, Fore, Style

# Initialize colorama for colored output
init()

# Global logger instance
_file_logger = None

def format_lag_time(lag_minutes: float) -> str:
    """Format lag time in a human-readable format with days, hours, and minutes"""
    if lag_minutes == float('inf'):
        return "∞ (infinite lag)"
    
    if lag_minutes < 1:
        return f"{lag_minutes:.1f} minutes"
    
    total_minutes = int(lag_minutes)
    days = total_minutes // (24 * 60)
    hours = (total_minutes % (24 * 60)) // 60
    minutes = total_minutes % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0 or not parts:  # Always show minutes if no days/hours
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    
    human_readable = " and ".join(parts)
    return f"{lag_minutes:.1f} minutes ({human_readable})"

def format_timestamp_in_timezone(timestamp, config: dict) -> str:
    """Format timestamp in the configured timezone"""
    if not timestamp:
        return "None"
        
    # Get timezone from config, default to US/Pacific
    tz_name = config.get('display', {}).get('timezone', 'US/Pacific')
    datetime_format = config.get('display', {}).get('datetime_format', '%b %d, %Y %H:%M')
    
    try:
        target_tz = pytz.timezone(tz_name)
        
        # Convert to target timezone
        if timestamp.tzinfo is None:
            # Assume UTC if no timezone info
            timestamp = pytz.utc.localize(timestamp)
        
        localized_timestamp = timestamp.astimezone(target_tz)
        
        # Format with timezone abbreviation
        tz_abbrev = localized_timestamp.strftime('%Z')
        return f"{localized_timestamp.strftime(datetime_format)} {tz_abbrev}"
        
    except Exception as e:
        # Fallback to UTC if timezone conversion fails
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=pytz.utc)
        else:
            timestamp = timestamp.astimezone(pytz.utc)
        return f"{timestamp.strftime(datetime_format)} UTC"

def get_status_emoji(status: str) -> str:
    """Get emoji for status"""
    status_emojis = {
        'pass': '✅',
        'warning': '⚠️',
        'critical': '❌',
        'error': '💥'
    }
    return status_emojis.get(status.lower(), '❓')

def setup_logging(log_file_path: str):
    """Setup logging to both console and file"""
    global _file_logger
    
    # Create file logger
    _file_logger = logging.getLogger('cdc_consistency')
    _file_logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    _file_logger.handlers.clear()
    
    # Create file handler
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add handler to logger
    _file_logger.addHandler(file_handler)
    
    log_info(f"Logging initialized. Console and file output: {log_file_path}")

def _log_to_file(level: str, message: str):
    """Log message to file if logger is set up"""
    if _file_logger:
        if level == "INFO":
            _file_logger.info(message)
        elif level == "SUCCESS":
            _file_logger.info(f"SUCCESS - {message}")
        elif level == "ERROR":
            _file_logger.error(message)
        elif level == "WARNING":
            _file_logger.warning(message)

def log_info(message):
    """Print info message with color and log to file"""
    print(f"{Fore.BLUE}[INFO]{Style.RESET_ALL} {message}")
    _log_to_file("INFO", message)

def log_success(message):
    """Print success message with color and log to file"""
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} {message}")
    _log_to_file("SUCCESS", message)

def log_error(message):
    """Print error message with color and log to file"""
    print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} {message}")
    _log_to_file("ERROR", message)

def log_warning(message):
    """Print warning message with color and log to file"""
    print(f"{Fore.YELLOW}[WARNING]{Style.RESET_ALL} {message}")
    _log_to_file("WARNING", message)

@dataclass
class ValidationResult:
    """Container for validation results"""
    check_name: str
    status: str  # 'pass', 'warning', 'critical'
    details: Dict[str, Any]
    timestamp: datetime

@dataclass
class TableCounts:
    """Container for table count results"""
    table_name: str
    postgres_count: int
    snowflake_count: int
    variance_pct: float
    status: str

class DatabaseConnector:
    """Handles database connections"""
    
    def __init__(self, postgres_url: str, snowflake_url: str):
        self.postgres_url = postgres_url
        self.snowflake_url = snowflake_url
        self._pg_conn = None
        self._sf_conn = None
    
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        if not self._pg_conn:
            parsed = urlparse(self.postgres_url)
            self._pg_conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                database=parsed.path.lstrip('/'),
                user=parsed.username,
                password=parsed.password
            )
        return self._pg_conn
    
    def get_snowflake_connection(self):
        """Get Snowflake connection"""
        if not self._sf_conn:
            parsed = urlparse(self.snowflake_url)
            query_params = parse_qs(parsed.query)
            
            self._sf_conn = snowflake.connector.connect(
                account=parsed.hostname,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.split('/')[1] if len(parsed.path.split('/')) > 1 else None,
                schema=parsed.path.split('/')[2] if len(parsed.path.split('/')) > 2 else None,
                warehouse=query_params.get('warehouse', [None])[0],
                role=query_params.get('role', [None])[0]
            )
        return self._sf_conn
    
    def close_connections(self):
        """Close all connections"""
        if self._pg_conn:
            self._pg_conn.close()
        if self._sf_conn:
            self._sf_conn.close()

class ConsistencyChecker:
    """Main consistency checking logic"""
    
    def __init__(self, config: Dict, db_connector: DatabaseConnector):
        self.config = config
        self.db = db_connector
        self.results = []
    
    def run_all_checks(self) -> List[ValidationResult]:
        """Run all consistency checks"""
        log_info("Starting CDC consistency validation...")
        
        # 1. Table count validation
        self.check_table_counts()
        
        # 1.5. Detailed note mismatch analysis if counts don't match
        self.analyze_note_mismatches()
        
        # 2. Company-centric lag analysis
        self.check_company_note_lag()
        
        # 3. Conditional transaction lag analysis (skip if perfect count match)
        self.check_transaction_lag_conditional()
        
        # 4. User synchronization check
        self.check_user_synchronization()
        
        # 5. Random spot checks
        self.run_spot_checks()
        
        log_success(f"Completed {len(self.results)} validation checks")
        return self.results
    
    def run_company_analysis_only(self) -> List[ValidationResult]:
        """Run only company note analysis (default mode)"""
        log_info("Starting company note analysis...")
        
        # 0. Table synchronization overview
        self.analyze_table_synchronization()
        
        # 1. Global note analysis (total counts and latest timestamps)
        self.analyze_global_note_statistics()
        
        # 2. Company note discrepancy analysis (find companies missing notes)
        self.analyze_company_note_discrepancies()
        
        # 3. Company-specific note analysis
        self.check_company_note_lag()
        
        log_success(f"Completed company note analysis")
        return self.results
    
    def analyze_global_note_statistics(self):
        """Analyze global note statistics across all companies"""
        log_info("Analyzing global note statistics...")
        
        try:
            pg_conn = self.db.get_postgres_connection()
            sf_conn = self.db.get_snowflake_connection()
            
            with pg_conn.cursor() as pg_cursor, sf_conn.cursor() as sf_cursor:
                # Get total note count and latest note from PostgreSQL
                log_info("📊 Getting total note statistics from PostgreSQL...")
                pg_cursor.execute("""
                    SELECT 
                        COUNT(*) as total_notes,
                        MAX(created_at) as latest_note_time,
                        COUNT(DISTINCT company_id) as companies_with_notes
                    FROM notes
                """)
                
                pg_result = pg_cursor.fetchone()
                pg_total_notes = pg_result[0] if pg_result[0] else 0
                pg_latest_note = pg_result[1] if pg_result[1] else None
                pg_companies_with_notes = pg_result[2] if pg_result[2] else 0
                
                log_info(f"  PostgreSQL - Total notes: {pg_total_notes:,}, Companies with notes: {pg_companies_with_notes}")
                log_info(f"  PostgreSQL - Latest note: {format_timestamp_in_timezone(pg_latest_note, self.config)}")
                
                # Get total note count and latest note from Snowflake
                log_info("📊 Getting total note statistics from Snowflake...")
                sf_cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_notes,
                        MAX("created_at") as latest_note_time,
                        COUNT(DISTINCT "company_id") as companies_with_notes
                    FROM MONEYBALL."public"."notes"
                    WHERE "_SNOWFLAKE_DELETED" = FALSE
                """)
                
                sf_result = sf_cursor.fetchone()
                sf_total_notes = sf_result[0] if sf_result[0] else 0
                sf_latest_note = sf_result[1] if sf_result[1] else None
                sf_companies_with_notes = sf_result[2] if sf_result[2] else 0
                
                log_info(f"  Snowflake - Total notes: {sf_total_notes:,}, Companies with notes: {sf_companies_with_notes}")
                log_info(f"  Snowflake - Latest note: {format_timestamp_in_timezone(sf_latest_note, self.config)}")
                
                # Calculate differences
                note_count_diff = pg_total_notes - sf_total_notes
                note_count_pct = (note_count_diff / pg_total_notes * 100) if pg_total_notes > 0 else 0
                company_diff = pg_companies_with_notes - sf_companies_with_notes
                
                log_info(f"📈 Global note count difference: {note_count_diff:,} ({note_count_pct:.1f}% missing from Snowflake)")
                log_info(f"📈 Companies with notes difference: {company_diff}")
                
                # Calculate timestamp lag
                if pg_latest_note and sf_latest_note:
                    from datetime import timezone
                    
                    # Convert both to UTC for comparison
                    if pg_latest_note.tzinfo is None:
                        pg_latest_utc = pg_latest_note.replace(tzinfo=timezone.utc)
                    else:
                        pg_latest_utc = pg_latest_note.astimezone(timezone.utc)
                    
                    if sf_latest_note.tzinfo is not None:
                        sf_latest_utc = sf_latest_note.astimezone(timezone.utc)
                    else:
                        sf_latest_utc = sf_latest_note.replace(tzinfo=timezone.utc)
                    
                    lag_seconds = (pg_latest_utc - sf_latest_utc).total_seconds()
                    lag_minutes = lag_seconds / 60
                    lag_formatted = format_lag_time(lag_minutes)
                    
                    log_info(f"🕐 Global note timestamp lag: {lag_formatted}")
                    
                    # Determine status based on lag and count difference
                    if note_count_pct <= 1.0 and lag_minutes <= 10:
                        status = 'pass'
                    elif note_count_pct <= 5.0 and lag_minutes <= 60:
                        status = 'warning'
                    else:
                        status = 'critical'
                else:
                    lag_minutes = float('inf')
                    lag_formatted = "∞ (missing timestamps)"
                    status = 'critical'
                    log_warning("❌ Missing global note timestamps - CRITICAL")
                
                log_info(f"📊 Global note analysis status: {status.upper()}")
                
                # Store the result
                self.results.append(ValidationResult(
                    check_name="global_note_analysis",
                    status=status,
                    details={
                        "pg_total_notes": pg_total_notes,
                        "sf_total_notes": sf_total_notes,
                        "note_count_diff": note_count_diff,
                        "note_count_pct": note_count_pct,
                        "pg_companies_with_notes": pg_companies_with_notes,
                        "sf_companies_with_notes": sf_companies_with_notes,
                        "company_diff": company_diff,
                        "pg_latest_note": pg_latest_note.isoformat() if pg_latest_note else None,
                        "sf_latest_note": sf_latest_note.isoformat() if sf_latest_note else None,
                        "lag_minutes": lag_minutes,
                        "lag_formatted": lag_formatted
                    },
                    timestamp=datetime.now()
                ))
                
        except Exception as e:
            log_error(f"Failed global note analysis: {e}")
            self.results.append(ValidationResult(
                check_name="global_note_analysis",
                status="error",
                details={"error": str(e)},
                timestamp=datetime.now()
            ))
    
    def analyze_company_note_discrepancies(self):
        """Analyze which specific companies are missing notes in Snowflake"""
        log_info("Analyzing company-level note discrepancies...")
        
        try:
            pg_conn = self.db.get_postgres_connection()
            sf_conn = self.db.get_snowflake_connection()
            
            with pg_conn.cursor() as pg_cursor, sf_conn.cursor() as sf_cursor:
                # Get note counts and latest timestamps for ALL companies from PostgreSQL (including 0 note companies)
                log_info("📊 Getting note statistics per company from PostgreSQL...")
                pg_cursor.execute("""
                    SELECT 
                        c.id as company_id,
                        c.name as company_name,
                        COALESCE(COUNT(n.id), 0) as note_count,
                        MAX(n.created_at) as latest_note_time
                    FROM companies c
                    LEFT JOIN notes n ON c.id = n.company_id
                    GROUP BY c.id, c.name
                    ORDER BY note_count DESC
                """)
                
                pg_results = pg_cursor.fetchall()
                log_info(f"  Found {len(pg_results)} total companies in PostgreSQL")
                
                # Get note counts and latest timestamps for ALL companies from Snowflake (including 0 note companies)
                log_info("📊 Getting note statistics per company from Snowflake...")
                sf_cursor.execute(f"""
                    SELECT 
                        c."id" as company_id,
                        c."name" as company_name,
                        COALESCE(COUNT(n."id"), 0) as note_count,
                        MAX(n."created_at") as latest_note_time
                    FROM MONEYBALL."public"."companies" c
                    LEFT JOIN MONEYBALL."public"."notes" n ON c."id" = n."company_id" 
                        AND n."_SNOWFLAKE_DELETED" = FALSE
                    WHERE c."_SNOWFLAKE_DELETED" = FALSE
                    GROUP BY c."id", c."name"
                    ORDER BY note_count DESC
                """)
                
                sf_results = sf_cursor.fetchall()
                log_info(f"  Found {len(sf_results)} total companies in Snowflake")
                
                # Also check which companies exist in Snowflake at all
                sf_cursor.execute(f"""
                    SELECT "id", "name"
                    FROM MONEYBALL."public"."companies"
                    WHERE "_SNOWFLAKE_DELETED" = FALSE
                """)
                sf_all_companies = {str(row[0]): row[1] for row in sf_cursor.fetchall()}
                log_info(f"  Found {len(sf_all_companies)} total companies in Snowflake")
                
                # Create dictionaries for easy lookup
                pg_companies = {str(row[0]): {
                    'name': row[1], 
                    'note_count': row[2], 
                    'latest_note': row[3]
                } for row in pg_results}
                
                sf_companies = {str(row[0]): {
                    'name': row[1], 
                    'note_count': row[2], 
                    'latest_note': row[3]
                } for row in sf_results}
                
                # Find companies with note discrepancies
                discrepant_companies = []
                companies_missing_from_sf = []
                
                for company_id, pg_data in pg_companies.items():
                    # Check if company exists in Snowflake at all
                    company_exists_in_sf = company_id in sf_all_companies
                    sf_data = sf_companies.get(company_id)
                    
                    pg_note_count = pg_data['note_count']
                    sf_note_count = sf_data['note_count'] if sf_data else 0
                    
                    # If PG has 0 notes, skip this company regardless of SF status - no notes to be missing
                    if pg_note_count == 0:
                        continue
                    
                    if not company_exists_in_sf:
                        # Company completely missing from Snowflake - we already filtered out 0-note companies above
                        companies_missing_from_sf.append({
                            'company_id': company_id,
                            'company_name': pg_data['name'],
                            'pg_note_count': pg_note_count,
                            'sf_note_count': 0,
                            'missing_count': pg_note_count,
                            'pg_latest_note': pg_data['latest_note'],
                            'sf_latest_note': None,
                            'company_in_snowflake': False,
                        })
                    elif sf_note_count != pg_note_count:
                        # Company has different note counts
                        missing_count = pg_note_count - sf_note_count
                        if missing_count > 0:  # Only include if Snowflake has fewer notes
                            discrepant_companies.append({
                                'company_id': company_id,
                                'company_name': pg_data['name'],
                                'pg_note_count': pg_note_count,
                                'sf_note_count': sf_note_count,
                                'missing_count': missing_count,
                                'pg_latest_note': pg_data['latest_note'],
                                'sf_latest_note': sf_data['latest_note'] if sf_data else None,
                                'company_in_snowflake': True,
                            })
                
                # Combine and sort by missing count (descending)
                all_discrepancies = companies_missing_from_sf + discrepant_companies
                all_discrepancies.sort(key=lambda x: x['missing_count'], reverse=True)
                
                total_companies_checked = len(pg_companies)
                companies_with_discrepancies = len(all_discrepancies)
                companies_perfect_match = total_companies_checked - companies_with_discrepancies
                
                log_info(f"📈 Company note discrepancy analysis:")
                log_info(f"  Total companies with notes in PostgreSQL: {total_companies_checked}")
                log_info(f"  Companies with perfect note counts: {companies_perfect_match}")
                log_info(f"  Companies with note discrepancies: {companies_with_discrepancies}")
                log_info(f"  Companies completely missing from Snowflake: {len(companies_missing_from_sf)}")
                
                if all_discrepancies:
                    log_info(f"📋 Top companies missing notes in Snowflake:")
                    for i, company in enumerate(all_discrepancies[:15], 1):  # Show top 15
                        pg_time_str = format_timestamp_in_timezone(company['pg_latest_note'], self.config)
                        sf_time_str = format_timestamp_in_timezone(company['sf_latest_note'], self.config)
                        log_info(f"  {i}. {company['company_name']}: missing {company['missing_count']} notes")
                        log_info(f"     PG: {company['pg_note_count']} notes, latest: {pg_time_str}")
                        log_info(f"     SF: {company['sf_note_count']} notes, latest: {sf_time_str}")
                        # Determine status based on presence
                        if not company['company_in_snowflake']:
                            status_desc = "Company missing from Snowflake"
                        elif company['sf_note_count'] == 0:
                            status_desc = "All notes missing in Snowflake"
                        else:
                            status_desc = "Partial notes missing in Snowflake"
                        log_info(f"     Status: {status_desc}")
                
                # Determine overall status
                discrepancy_pct = (companies_with_discrepancies / total_companies_checked * 100) if total_companies_checked > 0 else 0
                if companies_with_discrepancies == 0:
                    status = 'pass'
                elif discrepancy_pct <= 5.0:  # 5% tolerance
                    status = 'warning'
                else:
                    status = 'critical'
                
                log_info(f"📊 Company note discrepancy analysis status: {status.upper()}")
                log_info(f"   Discrepancy rate: {discrepancy_pct:.1f}% of companies")
                
                # Store the result
                self.results.append(ValidationResult(
                    check_name="company_note_discrepancies",
                    status=status,
                    details={
                        "total_companies_checked": total_companies_checked,
                        "companies_perfect_match": companies_perfect_match,
                        "companies_with_discrepancies": companies_with_discrepancies,
                        "companies_missing_from_sf": len(companies_missing_from_sf),
                        "discrepant_companies": all_discrepancies,
                        "discrepancy_percentage": discrepancy_pct
                    },
                    timestamp=datetime.now()
                ))
                
        except Exception as e:
            log_error(f"Failed company note discrepancy analysis: {e}")
            self.results.append(ValidationResult(
                check_name="company_note_discrepancies",
                status="error",
                details={"error": str(e)},
                timestamp=datetime.now()
            ))
    
    def check_table_counts(self):
        """Check row counts for all tables"""
        log_info("Checking table counts...")
        
        tables = self.config['tables']['core_tables']
        count_results = []
        
        pg_conn = self.db.get_postgres_connection()
        sf_conn = self.db.get_snowflake_connection()
        
        pg_cursor = pg_conn.cursor()
        sf_cursor = sf_conn.cursor()
        
        # Debug: Check Snowflake context and set schema if needed
        sf_cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        context = sf_cursor.fetchone()
        log_info(f"Snowflake context - DB: {context[0]}, Schema: {context[1]}, Warehouse: {context[2]}, Role: {context[3]}")
        
        # Set schema context if needed (using quotes for lowercase)
        if context[1] is None:
            log_info("Setting schema context to 'public'")
            sf_cursor.execute('USE SCHEMA "public"')
            sf_cursor.execute("SELECT CURRENT_SCHEMA()")
            new_schema = sf_cursor.fetchone()[0]
            log_info(f"Schema context now: {new_schema}")
        else:
            log_info(f"Schema already set: {context[1]}")
        
        # Debug: Show available tables
        sf_cursor.execute("SHOW TABLES")
        available_tables = sf_cursor.fetchall()
        log_info(f"Available tables in Snowflake: {len(available_tables)} tables found")
        log_info(f"Sample tables: {[t[1] for t in available_tables[:10]]}")  # Show first 10 table names
        
        for table in tables:
            try:
                # Get PostgreSQL count
                pg_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                pg_count = pg_cursor.fetchone()[0]
                
                # Get Snowflake count (excluding soft-deleted records)
                # Note: Lowercase identifiers need quotes in Snowflake
                sf_cursor.execute(f'SELECT COUNT(*) FROM "{table.lower()}" WHERE "_SNOWFLAKE_DELETED" = FALSE')
                sf_count = sf_cursor.fetchone()[0]
                
                # Calculate variance
                if pg_count > 0:
                    variance_pct = abs(sf_count - pg_count) / pg_count * 100
                else:
                    variance_pct = 0 if sf_count == 0 else 100
                
                # Determine status
                threshold = self.config['validation_rules']['count_variance_threshold'] * 100
                if variance_pct <= threshold:
                    status = 'pass'
                elif variance_pct <= threshold * 2:
                    status = 'warning'
                else:
                    status = 'critical'
                
                count_result = TableCounts(
                    table_name=table,
                    postgres_count=pg_count,
                    snowflake_count=sf_count,
                    variance_pct=variance_pct,
                    status=status
                )
                count_results.append(count_result)
                
                log_info(f"{table}: PG={pg_count}, SF={sf_count}, variance={variance_pct:.2f}% ({status})")
                
            except Exception as e:
                log_error(f"Failed to check counts for {table}: {e}")
                count_results.append(TableCounts(
                    table_name=table,
                    postgres_count=-1,
                    snowflake_count=-1,
                    variance_pct=100,
                    status='critical'
                ))
        
        # Determine overall status
        critical_tables = [r for r in count_results if r.status == 'critical']
        warning_tables = [r for r in count_results if r.status == 'warning']
        
        overall_status = 'critical' if critical_tables else ('warning' if warning_tables else 'pass')
        
        self.results.append(ValidationResult(
            check_name="table_counts",
            status=overall_status,
            details={
                "count_results": count_results,
                "critical_tables": len(critical_tables),
                "warning_tables": len(warning_tables),
                "passing_tables": len([r for r in count_results if r.status == 'pass'])
            },
            timestamp=datetime.now()
            ))
    
    def analyze_note_mismatches(self):
        """Analyze note count mismatches between PostgreSQL and Snowflake"""
        log_info("Analyzing note count mismatches...")
        
        try:
            pg_conn = self.db.get_postgres_connection()
            sf_conn = self.db.get_snowflake_connection()
            
            pg_cursor = pg_conn.cursor()
            sf_cursor = sf_conn.cursor()
            
            # Set schema for Snowflake queries
            sf_cursor.execute('USE SCHEMA "public"')
            
            # Get note counts per company from PostgreSQL
            # Since notes table doesn't have company_id, we'll use a different approach
            log_info("Getting note counts per company from PostgreSQL...")
            pg_cursor.execute("""
                SELECT 
                    'total' as company_id,
                    'All Companies' as company_name,
                    COUNT(*) as pg_note_count,
                    MAX(created_at) as latest_pg_note
                FROM notes
                WHERE created_at >= NOW() - INTERVAL '30 days'
            """)
            
            pg_total = pg_cursor.fetchone()
            
            # Get note counts from Snowflake
            log_info("Getting note counts from Snowflake...")
            sf_cursor.execute("""
                SELECT 
                    COUNT(*) as sf_note_count,
                    MAX("created_at") as latest_sf_note
                FROM "notes"
                WHERE "created_at" >= DATEADD(day, -30, CURRENT_TIMESTAMP())
                  AND "_SNOWFLAKE_DELETED" = FALSE
            """)
            
            sf_total = sf_cursor.fetchone()
            
            if pg_total and sf_total:
                pg_count, pg_latest = pg_total[2], pg_total[3]
                sf_count, sf_latest = sf_total[0], sf_total[1]
                missing_count = pg_count - sf_count
                
                if missing_count > 0:
                    log_warning(f"Found {missing_count} missing notes in Snowflake")
                    
                    # Get more details about recent notes
                    pg_cursor.execute("""
                        SELECT created_at, updated_at, id, type
                        FROM notes 
                        WHERE created_at >= NOW() - INTERVAL '7 days'
                        ORDER BY created_at DESC
                        LIMIT 10
                    """)
                    recent_pg_notes = pg_cursor.fetchall()
                    
                    sf_cursor.execute("""
                        SELECT "created_at", "updated_at", "id", "type"
                        FROM "notes"
                        WHERE "created_at" >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                          AND "_SNOWFLAKE_DELETED" = FALSE
                        ORDER BY "created_at" DESC
                        LIMIT 10
                    """)
                    recent_sf_notes = sf_cursor.fetchall()
                    
                    mismatch_details = {
                        "total_missing_notes": missing_count,
                        "pg_recent_count": pg_count,
                        "sf_recent_count": sf_count,
                        "latest_pg_note": pg_latest.isoformat() if pg_latest else None,
                        "latest_sf_note": sf_latest.isoformat() if sf_latest else None,
                        "recent_pg_notes": len(recent_pg_notes),
                        "recent_sf_notes": len(recent_sf_notes),
                        "analysis_period": "30 days"
                    }
                    
                    self.results.append(ValidationResult(
                        check_name="note_mismatch_analysis",
                        status="warning" if missing_count < pg_count * 0.05 else "critical",
                        details=mismatch_details,
                        timestamp=datetime.now()
                    ))
                else:
                    log_info("No significant note count mismatches found")
                    
        except Exception as e:
            log_error(f"Error in note mismatch analysis: {e}")
            self.results.append(ValidationResult(
                check_name="note_mismatch_analysis",
                status="error",
                details={"error": str(e)},
                timestamp=datetime.now()
            ))
    
    def analyze_table_synchronization(self):
        """Analyze synchronization status of all tables in public schema"""
        log_info("Analyzing table synchronization across all public schema tables...")
        
        try:
            pg_conn = self.db.get_postgres_connection()
            sf_conn = self.db.get_snowflake_connection()
            
            pg_cursor = pg_conn.cursor()
            sf_cursor = sf_conn.cursor()
            
            # Get all tables in public schema from PostgreSQL
            log_info("📋 Discovering tables in PostgreSQL public schema...")
            pg_cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            pg_tables = {row[0] for row in pg_cursor.fetchall()}
            log_info(f"  Found {len(pg_tables)} tables in PostgreSQL public schema")
            
            # Get all tables in public schema from Snowflake
            log_info("📋 Discovering tables in Snowflake public schema...")
            sf_cursor.execute("""
                SELECT table_name 
                FROM MONEYBALL.INFORMATION_SCHEMA.TABLES 
                WHERE table_schema = 'public' 
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            sf_tables = {row[0].lower() for row in sf_cursor.fetchall()}  # Snowflake returns uppercase
            log_info(f"  Found {len(sf_tables)} tables in Snowflake public schema")
            
            # Find common tables and differences
            common_tables = pg_tables.intersection(sf_tables)
            pg_only_tables = pg_tables - sf_tables
            
            # Filter out CDC journal tables from Snowflake-only tables (they're internal CDC metadata)
            sf_only_tables_filtered = {table for table in (sf_tables - pg_tables) 
                                     if not (table.endswith('_journal_1757441435_1') or 
                                           table.endswith('_journal_1757441436_1') or
                                           table.endswith('_journal_1758702347_1') or
                                           table.endswith('_journal_1758702348_1') or
                                           table.endswith('_journal_1758702349_1') or
                                           table.endswith('_journal_1758702350_1') or
                                           table.endswith('_journal_1758702351_1') or
                                           table.endswith('_journal_1758702351_2') or
                                           '_journal_' in table)}
            sf_only_tables = sf_only_tables_filtered
            
            log_info(f"📊 Table comparison summary:")
            log_info(f"  Common tables: {len(common_tables)}")
            log_info(f"  PostgreSQL only: {len(pg_only_tables)}")
            log_info(f"  Snowflake only: {len(sf_only_tables)}")
            
            # Analyze each common table
            table_sync_results = []
            
            for table_name in sorted(common_tables):
                log_info(f"🔍 Analyzing table: {table_name}")
                
                # Get PostgreSQL table info
                pg_cursor.execute(f"""
                    SELECT 
                        COUNT(*) as record_count,
                        CASE 
                            WHEN COUNT(*) = 0 THEN NULL
                            ELSE MAX(GREATEST(
                                COALESCE(created_at, '1900-01-01'::timestamp),
                                COALESCE(updated_at, '1900-01-01'::timestamp)
                            ))
                        END as latest_timestamp
                    FROM "{table_name}"
                """)
                pg_result = pg_cursor.fetchone()
                pg_count = pg_result[0] if pg_result else 0
                pg_latest = pg_result[1] if pg_result else None
                
                # Get Snowflake table info
                sf_cursor.execute(f"""
                    SELECT 
                        COUNT(*) as record_count,
                        CASE 
                            WHEN COUNT(*) = 0 THEN NULL
                            ELSE MAX(GREATEST(
                                COALESCE("created_at", '1900-01-01'::timestamp),
                                COALESCE("updated_at", '1900-01-01'::timestamp)
                            ))
                        END as latest_timestamp
                    FROM MONEYBALL."public"."{table_name}"
                    WHERE "_SNOWFLAKE_DELETED" = FALSE
                """)
                sf_result = sf_cursor.fetchone()
                sf_count = sf_result[0] if sf_result else 0
                sf_latest = sf_result[1] if sf_result else None
                
                # Calculate differences
                count_diff = pg_count - sf_count
                count_diff_pct = (count_diff / pg_count * 100) if pg_count > 0 else 0
                
                # Calculate lag
                lag_minutes = 0
                if pg_latest and sf_latest:
                    # Ensure both timestamps are timezone-aware
                    if pg_latest.tzinfo is None:
                        pg_latest = pytz.utc.localize(pg_latest)
                    if sf_latest.tzinfo is None:
                        sf_latest = pytz.utc.localize(sf_latest)
                    
                    lag_seconds = (pg_latest - sf_latest).total_seconds()
                    lag_minutes = max(0, lag_seconds / 60)  # Don't show negative lag
                elif pg_latest and not sf_latest:
                    lag_minutes = float('inf')
                
                # Determine sync status
                if pg_count == 0 and sf_count == 0:
                    sync_status = 'empty'
                elif count_diff == 0 and lag_minutes <= 5:
                    sync_status = 'perfect'
                elif abs(count_diff_pct) <= 1 and lag_minutes <= 30:
                    sync_status = 'good'
                elif abs(count_diff_pct) <= 5 and lag_minutes <= 60:
                    sync_status = 'warning'
                else:
                    sync_status = 'critical'
                
                table_result = {
                    'table_name': table_name,
                    'pg_count': pg_count,
                    'sf_count': sf_count,
                    'count_diff': count_diff,
                    'count_diff_pct': count_diff_pct,
                    'pg_latest': pg_latest,
                    'sf_latest': sf_latest,
                    'lag_minutes': lag_minutes,
                    'sync_status': sync_status
                }
                table_sync_results.append(table_result)
                
                # Log individual table status
                lag_str = format_lag_time(lag_minutes) if lag_minutes != float('inf') else "No SF data"
                log_info(f"  📊 {table_name}: PG={pg_count:,}, SF={sf_count:,}, diff={count_diff:+,} ({count_diff_pct:+.1f}%), lag={lag_str}")
            
            # Sort by sync status (worst first) then by count difference
            status_priority = {'critical': 0, 'warning': 1, 'good': 2, 'perfect': 3, 'empty': 4}
            table_sync_results.sort(key=lambda x: (status_priority.get(x['sync_status'], 99), -abs(x['count_diff'])))
            
            # Determine overall sync status
            critical_tables = len([t for t in table_sync_results if t['sync_status'] == 'critical'])
            warning_tables = len([t for t in table_sync_results if t['sync_status'] == 'warning'])
            total_tables = len(table_sync_results)
            
            if critical_tables > 0:
                overall_status = 'critical'
            elif warning_tables > 0:
                overall_status = 'warning'
            else:
                overall_status = 'pass'
            
            log_info(f"📈 Table synchronization analysis complete:")
            log_info(f"  Critical: {critical_tables}, Warning: {warning_tables}, Good/Perfect: {total_tables - critical_tables - warning_tables}")
            log_info(f"  Tables only in PostgreSQL: {list(pg_only_tables) if pg_only_tables else 'None'}")
            log_info(f"  Tables only in Snowflake: {list(sf_only_tables) if sf_only_tables else 'None'}")
            log_info(f"📊 Overall table sync status: {overall_status.upper()}")
            
            # Store the result
            self.results.append(ValidationResult(
                check_name="table_synchronization",
                status=overall_status,
                details={
                    "common_tables_count": len(common_tables),
                    "pg_only_tables": list(pg_only_tables),
                    "sf_only_tables": list(sf_only_tables),
                    "table_results": table_sync_results,
                    "critical_tables": critical_tables,
                    "warning_tables": warning_tables,
                    "total_analyzed": total_tables
                },
                timestamp=datetime.now()
            ))
            
        except Exception as e:
            log_error(f"Error in table synchronization analysis: {e}")
            self.results.append(ValidationResult(
                check_name="table_synchronization",
                status="error",
                details={"error": str(e)},
                timestamp=datetime.now()
            ))
    
    def check_company_note_lag(self):
        """Check note data lag for companies with most recent activity and watchlist companies"""
        log_info("Checking company note lag...")
        log_info("ℹ️  Note: This compares note counts and latest note timestamps for selected companies")
        log_info("ℹ️  This measures how far behind Snowflake note data is from PostgreSQL")
        
        from datetime import timezone
        
        try:
            top_companies_count = self.config['validation_rules']['top_companies_with_notes']
            
            pg_conn = self.db.get_postgres_connection()
            sf_conn = self.db.get_snowflake_connection()
            
            pg_cursor = pg_conn.cursor()
            sf_cursor = sf_conn.cursor()
            
            # Set schema for Snowflake queries
            sf_cursor.execute('USE SCHEMA "public"')
            
            log_info(f"Getting top {top_companies_count} companies with recent notes...")
            
            # Get companies with most recent activity from PostgreSQL
            # We'll focus on companies that have been updated recently, then analyze their notes
            pg_cursor.execute(f"""
                SELECT c.id, c.sr_name, c.updated_at as latest_company_activity
                FROM companies c
                WHERE c.updated_at >= NOW() - INTERVAL '30 days'
                  AND c.sr_name IS NOT NULL
                ORDER BY c.updated_at DESC
                LIMIT {top_companies_count}
            """)
            
            pg_companies = pg_cursor.fetchall()
            log_info(f"Found {len(pg_companies)} companies with recent notes")
            
            # Get watchlist companies by name patterns
            watchlist_companies = []
            if 'company_watchlist' in self.config and 'name_patterns' in self.config['company_watchlist']:
                log_info("Getting watchlist companies by name patterns...")
                patterns = self.config['company_watchlist']['name_patterns']
                
                for pattern in patterns:
                    log_info(f"Searching for companies matching pattern: {pattern}")
                    pg_cursor.execute(f"""
                        SELECT c.id, c.sr_name, c.updated_at as latest_company_activity
                        FROM companies c
                        WHERE LOWER(c.sr_name) LIKE LOWER('%{pattern}%')
                          AND c.updated_at >= NOW() - INTERVAL '90 days'
                          AND c.sr_name IS NOT NULL
                        ORDER BY c.updated_at DESC
                    """)
                    
                    pattern_companies = pg_cursor.fetchall()
                    log_info(f"Found {len(pattern_companies)} companies matching '{pattern}'")
                    for company in pattern_companies:
                        log_info(f"  - {company[1]} (ID: {company[0]})")
                    
                    watchlist_companies.extend(pattern_companies)
            
            # Combine top companies and watchlist companies (remove duplicates)
            all_companies_dict = {}
            for company in pg_companies + watchlist_companies:
                all_companies_dict[company[0]] = company  # Use company ID as key to deduplicate
            
            all_companies = list(all_companies_dict.values())
            log_info(f"Total unique companies to check: {len(all_companies)} (top: {len(pg_companies)}, watchlist: {len(watchlist_companies)})")
            
            if not all_companies:
                log_warning("No companies found for lag analysis")
                self.results.append(ValidationResult(
                    check_name="company_note_lag",
                    status="warning",
                    details={"message": "No companies found for lag analysis"},
                    timestamp=datetime.now()
                ))
                return
            
            company_lag_results = []
            max_lag_minutes = self.config['lag_thresholds']['company_notes']
            
            for i, (company_id, company_name, latest_company_activity) in enumerate(all_companies, 1):
                log_info(f"[{i}/{len(all_companies)}] Analyzing notes for company: {company_name} (ID: {company_id})")
                log_info(f"  Company last updated: {latest_company_activity}")
                
                # Get ALL note counts and latest note timestamp for THIS SPECIFIC COMPANY from PostgreSQL
                log_info(f"  📊 Getting ALL note count for company {company_id} from PostgreSQL...")
                pg_cursor.execute("""
                    SELECT COUNT(*) as note_count, MAX(created_at) as latest_note_time
                    FROM notes 
                    WHERE company_id = %s 
                """, (company_id,))
                
                pg_result = pg_cursor.fetchone()
                pg_note_count, latest_pg_note_time = pg_result[0] if pg_result else 0, pg_result[1] if pg_result else None
                
                # Format timestamp for display in configured timezone
                pg_time_str = format_timestamp_in_timezone(latest_pg_note_time, self.config)
                log_info(f"  PostgreSQL - Notes for {company_name} (all time): {pg_note_count}, Latest: {pg_time_str}")
                
                # Get ALL note counts and latest note timestamp for THIS SPECIFIC COMPANY from Snowflake
                log_info(f"  📊 Getting ALL note count for company {company_id} from Snowflake...")
                sf_cursor.execute(f"""
                    SELECT COUNT(*) as note_count, MAX("created_at") as latest_note_time
                    FROM "notes"
                    WHERE "company_id" = '{company_id}'
                      AND "_SNOWFLAKE_DELETED" = FALSE
                """)
                
                sf_result = sf_cursor.fetchone()
                sf_note_count, latest_sf_note_time = sf_result[0] if sf_result else 0, sf_result[1] if sf_result else None
                
                # Format timestamp for display in configured timezone
                sf_time_str = format_timestamp_in_timezone(latest_sf_note_time, self.config)
                log_info(f"  Snowflake - Notes for {company_name} (all time): {sf_note_count}, Latest: {sf_time_str}")
                
                # Calculate note count difference
                note_count_diff = pg_note_count - sf_note_count
                note_count_pct = (note_count_diff / pg_note_count * 100) if pg_note_count > 0 else 0
                log_info(f"  📈 Note count difference: {note_count_diff} ({note_count_pct:.1f}% missing from Snowflake)")
                
                # Calculate note timestamp lag
                if latest_pg_note_time and latest_sf_note_time:
                    # Convert both timestamps to UTC for proper comparison
                    from datetime import timezone
                    
                    # Ensure PG time is timezone-aware (should already be UTC)
                    if latest_pg_note_time.tzinfo is None:
                        latest_pg_note_time = latest_pg_note_time.replace(tzinfo=timezone.utc)
                    else:
                        latest_pg_note_time = latest_pg_note_time.astimezone(timezone.utc)
                    
                    # Convert SF time to UTC if it has timezone info
                    if latest_sf_note_time.tzinfo is not None:
                        latest_sf_note_time = latest_sf_note_time.astimezone(timezone.utc)
                    else:
                        latest_sf_note_time = latest_sf_note_time.replace(tzinfo=timezone.utc)
                    
                    lag_seconds = (latest_pg_note_time - latest_sf_note_time).total_seconds()
                    lag_minutes = lag_seconds / 60
                    
                    # Format the lag time with days/hours/minutes
                    lag_formatted = format_lag_time(lag_minutes)
                    
                    log_info(f"  🕐 Note timestamp lag analysis:")
                    log_info(f"    PG latest note: {format_timestamp_in_timezone(latest_pg_note_time, self.config)}")
                    log_info(f"    SF latest note: {format_timestamp_in_timezone(latest_sf_note_time, self.config)}")
                    log_info(f"    Note lag: {lag_formatted}")
                    
                    if lag_minutes <= max_lag_minutes:
                        status = 'pass'
                    elif lag_minutes <= max_lag_minutes * 2:
                        status = 'warning'
                    else:
                        status = 'critical'
                    
                    log_info(f"  📊 Overall status for {company_name}: {status.upper()}")
                elif pg_note_count == 0 and sf_note_count == 0:
                    # Both databases have 0 notes - this is OK
                    lag_minutes = 0
                    status = 'pass'
                    log_info(f"  📊 No notes in either database - OK")
                    log_info(f"  📊 Overall status for {company_name}: {status.upper()}")
                else:
                    lag_minutes = float('inf')
                    status = 'critical'
                    log_warning(f"  ❌ Missing note timestamps - CRITICAL")
                
                company_lag_results.append({
                    'company_id': str(company_id),
                    'company_name': company_name,
                    'pg_note_count': pg_note_count,
                    'sf_note_count': sf_note_count,
                    'note_count_diff': note_count_diff,
                    'note_count_pct': note_count_pct,
                    'latest_pg_note_time': latest_pg_note_time.isoformat() if latest_pg_note_time else None,
                    'latest_sf_note_time': latest_sf_note_time.isoformat() if latest_sf_note_time else None,
                    'lag_minutes': lag_minutes,
                    'status': status
                })
                
                status_emoji = {"pass": "✅", "warning": "⚠️", "critical": "❌"}[status]
                log_info(f"  Result: {company_name} = {status_emoji} {status.upper()}")
                
                if i < len(all_companies):  # Don't add separator after last item
                    log_info("  " + "-" * 50)
            
            # Determine overall status
            critical_companies = [r for r in company_lag_results if r['status'] == 'critical']
            warning_companies = [r for r in company_lag_results if r['status'] == 'warning']
            
            overall_status = 'critical' if critical_companies else ('warning' if warning_companies else 'pass')
            
            self.results.append(ValidationResult(
                check_name="company_note_lag",
                status=overall_status,
                details={
                    "company_results": company_lag_results,
                    "max_lag_threshold_minutes": max_lag_minutes,
                    "companies_checked": len(company_lag_results),
                    "critical_companies": len(critical_companies),
                    "warning_companies": len(warning_companies)
                },
                timestamp=datetime.now()
            ))
            
        except Exception as e:
            log_error(f"Failed to check company note lag: {e}")
            self.results.append(ValidationResult(
                check_name="company_note_lag",
                status="critical",
                details={"error": str(e)},
                timestamp=datetime.now()
            ))
    
    def check_transaction_lag_conditional(self):
        """Check transaction lag for funds - skip if counts are perfect"""
        
        # Check if we should skip detailed transaction checking
        skip_if_perfect = self.config['validation_rules'].get('skip_detailed_checks_if_perfect_count', False)
        
        if skip_if_perfect:
            # Check if transaction and fund counts are perfect (0% variance)
            table_count_result = None
            for result in self.results:
                if result.check_name == "table_counts":
                    table_count_result = result
                    break
            
            if table_count_result:
                transaction_perfect = False
                fund_perfect = False
                
                for count_result in table_count_result.details.get("count_results", []):
                    if count_result.table_name == "transactions" and count_result.variance_pct == 0.0:
                        transaction_perfect = True
                    elif count_result.table_name == "funds" and count_result.variance_pct == 0.0:
                        fund_perfect = True
                
                if transaction_perfect and fund_perfect:
                    log_info("⏭️  Skipping transaction lag check - perfect count match (0% variance)")
                    self.results.append(ValidationResult(
                        check_name="transaction_lag",
                        status="pass",
                        details={
                            "message": "Skipped - perfect count match",
                            "transaction_variance": 0.0,
                            "fund_variance": 0.0,
                            "skipped": True
                        },
                        timestamp=datetime.now()
                    ))
                    return
        
        # If not skipping, run the full transaction lag check
        self.check_transaction_lag()
    
    def check_transaction_lag(self):
        """Check transaction lag for funds"""
        log_info("Checking transaction lag...")
        
        try:
            pg_conn = self.db.get_postgres_connection()
            sf_conn = self.db.get_snowflake_connection()
            
            pg_cursor = pg_conn.cursor()
            sf_cursor = sf_conn.cursor()
            
            # Get latest transactions per fund from PostgreSQL
            pg_cursor.execute("""
                SELECT f.id as fund_id, f.name as fund_name, 
                       MAX(t.created_at) as latest_transaction_time,
                       COUNT(t.id) as transaction_count
                FROM funds f
                JOIN companies c ON c.id IS NOT NULL  -- Business logic placeholder for fund linkage
                JOIN transactions t ON t.company_id = c.id
                WHERE t.created_at >= NOW() - INTERVAL '30 days'
                GROUP BY f.id, f.name
                ORDER BY latest_transaction_time DESC
            """)
            
            pg_funds = pg_cursor.fetchall()
            
            if not pg_funds:
                log_warning("No funds with recent transactions found")
                self.results.append(ValidationResult(
                    check_name="transaction_lag",
                    status="warning",
                    details={"message": "No funds with recent transactions found"},
                    timestamp=datetime.now()
                ))
                return
            
            fund_lag_results = []
            max_lag_minutes = self.config['lag_thresholds']['fund_transactions']
            
            for fund_id, fund_name, latest_pg_time, transaction_count in pg_funds:
                # Get latest transaction time from Snowflake for this fund
                sf_cursor.execute(f"""
                    SELECT MAX(t."created_at") as latest_sf_time
                    FROM "funds" f
                    JOIN "companies" c ON c."id" IS NOT NULL  -- Business logic placeholder
                    JOIN "transactions" t ON t."company_id" = c."id"
                    WHERE f."id" = '{fund_id}' AND f."_SNOWFLAKE_DELETED" = FALSE AND c."_SNOWFLAKE_DELETED" = FALSE AND t."_SNOWFLAKE_DELETED" = FALSE
                """)
                
                sf_result = sf_cursor.fetchone()
                latest_sf_time = sf_result[0] if sf_result and sf_result[0] else None
                
                if latest_sf_time:
                    lag_seconds = (latest_pg_time - latest_sf_time).total_seconds()
                    lag_minutes = lag_seconds / 60
                    
                    if lag_minutes <= max_lag_minutes:
                        status = 'pass'
                    elif lag_minutes <= max_lag_minutes * 2:
                        status = 'warning'
                    else:
                        status = 'critical'
                else:
                    lag_minutes = float('inf')
                    status = 'critical'
                
                fund_lag_results.append({
                    'fund_id': str(fund_id),
                    'fund_name': fund_name,
                    'latest_pg_time': latest_pg_time.isoformat(),
                    'latest_sf_time': latest_sf_time.isoformat() if latest_sf_time else None,
                    'lag_minutes': lag_minutes,
                    'transaction_count': transaction_count,
                    'status': status
                })
                
                log_info(f"Fund {fund_name}: lag={lag_minutes:.1f}min ({status})")
            
            # Determine overall status
            critical_funds = [r for r in fund_lag_results if r['status'] == 'critical']
            warning_funds = [r for r in fund_lag_results if r['status'] == 'warning']
            
            overall_status = 'critical' if critical_funds else ('warning' if warning_funds else 'pass')
            
            self.results.append(ValidationResult(
                check_name="transaction_lag",
                status=overall_status,
                details={
                    "fund_results": fund_lag_results,
                    "max_lag_threshold_minutes": max_lag_minutes,
                    "funds_checked": len(fund_lag_results),
                    "critical_funds": len(critical_funds),
                    "warning_funds": len(warning_funds)
                },
                timestamp=datetime.now()
            ))
            
        except Exception as e:
            log_error(f"Failed to check transaction lag: {e}")
            self.results.append(ValidationResult(
                check_name="transaction_lag",
                status="critical",
                details={"error": str(e)},
                timestamp=datetime.now()
            ))
    
    def check_user_synchronization(self):
        """Check if all users from PostgreSQL exist in Snowflake"""
        log_info("Checking user synchronization...")
        
        try:
            pg_conn = self.db.get_postgres_connection()
            sf_conn = self.db.get_snowflake_connection()
            
            pg_cursor = pg_conn.cursor()
            sf_cursor = sf_conn.cursor()
            
            # Set schema for Snowflake queries
            sf_cursor.execute('USE SCHEMA "public"')
            
            # Get all users from PostgreSQL (not just recent ones)
            log_info("Getting all users from PostgreSQL...")
            pg_cursor.execute("""
                SELECT id, email, created_at, updated_at
                FROM users 
                ORDER BY created_at DESC
            """)
            
            pg_users = pg_cursor.fetchall()
            
            if not pg_users:
                log_warning("No users found in PostgreSQL")
                self.results.append(ValidationResult(
                    check_name="user_synchronization",
                    status="warning",
                    details={"message": "No users found"},
                    timestamp=datetime.now()
                ))
                return
            
            missing_users = []
            present_users = []
            
            for user_id, email, created_at, updated_at in pg_users:
                # Check if user exists in Snowflake
                sf_cursor.execute(f"""
                    SELECT "id", "email", "created_at" 
                    FROM "users" 
                    WHERE "id" = '{user_id}' AND "_SNOWFLAKE_DELETED" = FALSE
                """)
                
                sf_user = sf_cursor.fetchone()
                
                if sf_user:
                    present_users.append({
                        'user_id': str(user_id),
                        'email': email,
                        'pg_created_at': created_at.isoformat(),
                        'sf_created_at': sf_user[2].isoformat()
                    })
                else:
                    missing_users.append({
                        'user_id': str(user_id),
                        'email': email,
                        'created_at': created_at.isoformat(),
                        'updated_at': updated_at.isoformat()
                    })
            
            # Determine status
            missing_count = len(missing_users)
            total_count = len(pg_users)
            missing_pct = (missing_count / total_count * 100) if total_count > 0 else 0
            
            if missing_count == 0:
                status = 'pass'
            elif missing_pct <= 5:  # Less than 5% missing
                status = 'warning'
            else:
                status = 'critical'
            
            log_info(f"User sync: {total_count - missing_count}/{total_count} present, {missing_count} missing ({status})")
            log_info(f"Total PostgreSQL users: {total_count}")
            log_info(f"Users present in Snowflake: {total_count - missing_count}")
            log_info(f"Users missing from Snowflake: {missing_count}")
            
            self.results.append(ValidationResult(
                check_name="user_synchronization",
                status=status,
                details={
                    "total_pg_users": total_count,
                    "present_users": len(present_users),
                    "missing_users": missing_count,
                    "missing_percentage": missing_pct,
                    "missing_user_details": missing_users[:10],  # Limit to first 10 for report
                    "sample_present_users": present_users[:5]   # Sample for verification
                },
                timestamp=datetime.now()
            ))
            
        except Exception as e:
            log_error(f"Failed to check user synchronization: {e}")
            self.results.append(ValidationResult(
                check_name="user_synchronization",
                status="critical",
                details={"error": str(e)},
                timestamp=datetime.now()
            ))
    
    def run_spot_checks(self):
        """Run random spot checks on sample records (skip if perfect count match)"""
        log_info("Running random spot checks...")
        
        sample_size = self.config['validation_rules']['sample_size']
        tables = self.config['tables']['core_tables']
        
        # Check which tables have perfect count matches (0% variance)
        perfect_tables = set()
        for result in self.results:
            if result.check_name == "table_counts":
                for count_result in result.details.get("count_results", []):
                    if count_result.variance_pct == 0.0:
                        perfect_tables.add(count_result.table_name)
                        log_info(f"⏭️  Skipping spot check for {count_result.table_name} - perfect count match (0% variance)")
        
        spot_check_results = []
        
        for table in tables:
            if table in perfect_tables:
                # Skip spot check but add a result showing it was skipped
                spot_check_results.append({
                    'table': table,
                    'status': 'pass',
                    'skipped': True,
                    'message': 'Skipped - perfect count match',
                    'matches': 0,
                    'total': 0,
                    'accuracy': 100.0,
                    'mismatches': []
                })
                continue
                
            try:
                log_info(f"🔍 Spot checking {table}...")
                result = self.spot_check_table(table, sample_size)
                spot_check_results.append(result)
                
            except Exception as e:
                log_error(f"Failed spot check for {table}: {e}")
                spot_check_results.append({
                    'table': table,
                    'status': 'critical',
                    'error': str(e),
                    'matches': 0,
                    'total': 0,
                    'accuracy': 0,
                    'mismatches': []
                })
        
        # Calculate overall accuracy
        total_matches = sum(r.get('matches', 0) for r in spot_check_results)
        total_checks = sum(r.get('total', 0) for r in spot_check_results)
        overall_accuracy = (total_matches / total_checks * 100) if total_checks > 0 else 0
        
        accuracy_threshold = self.config['validation_rules']['spot_check_accuracy'] * 100
        
        if overall_accuracy >= accuracy_threshold:
            status = 'pass'
        elif overall_accuracy >= accuracy_threshold * 0.95:  # 95% of threshold
            status = 'warning'
        else:
            status = 'critical'
        
        log_info(f"Spot check accuracy: {overall_accuracy:.1f}% ({status})")
        
        # Summary of spot check results already logged during individual checks
        
        self.results.append(ValidationResult(
            check_name="spot_checks",
            status=status,
            details={
                "overall_accuracy": overall_accuracy,
                "accuracy_threshold": accuracy_threshold,
                "total_matches": total_matches,
                "total_checks": total_checks,
                "table_results": spot_check_results,
                "warning_explanation": f"Accuracy {overall_accuracy:.1f}% is below threshold {accuracy_threshold}%" if status != 'pass' else None
            },
            timestamp=datetime.now()
        ))
    
    def spot_check_table(self, table: str, sample_size: int) -> Dict:
        """Perform spot check on a specific table"""
        
        pg_conn = self.db.get_postgres_connection()
        sf_conn = self.db.get_snowflake_connection()
        
        pg_cursor = pg_conn.cursor()
        sf_cursor = sf_conn.cursor()
        
        # Get random sample from PostgreSQL
        log_info(f"  📊 Getting random sample of {sample_size} records from {table}...")
        pg_cursor.execute(f"""
            SELECT id FROM {table} 
            ORDER BY RANDOM() 
            LIMIT {sample_size}
        """)
        
        sample_ids = [row[0] for row in pg_cursor.fetchall()]
        
        if not sample_ids:
            log_warning(f"  ⚠️  No records found in {table} for sampling")
            return {
                'table': table,
                'status': 'warning',
                'message': 'No records found for sampling',
                'matches': 0,
                'total': 0,
                'accuracy': 0
            }
        
        log_info(f"  🔍 Checking {len(sample_ids)} records from {table}...")
        matches = 0
        total = len(sample_ids)
        mismatches = []
        
        # Check each sampled record with progress
        for i, record_id in enumerate(sample_ids, 1):
            if i % 20 == 0 or i == total:  # Log progress every 20 records
                log_info(f"    Progress: {i}/{total} records checked...")
            # Get record from PostgreSQL
            pg_cursor.execute(f"SELECT * FROM {table} WHERE id = %s", (record_id,))
            pg_record = pg_cursor.fetchone()
            
            # Get record from Snowflake
            sf_cursor.execute(f"""
                SELECT * FROM "{table.lower()}" 
                WHERE "id" = '{record_id}' AND "_SNOWFLAKE_DELETED" = FALSE
            """)
            sf_record = sf_cursor.fetchone()
            
            if sf_record:
                match_result = self.records_match_detailed(pg_record, sf_record, table)
                if match_result['matches']:
                    matches += 1
                else:
                    mismatches.append({
                        'id': str(record_id),
                        'details': match_result['differences'],
                        'pg_exists': True,
                        'sf_exists': True
                    })
            else:
                mismatches.append({
                    'id': str(record_id),
                    'details': 'Record not found in Snowflake',
                    'pg_exists': True,
                    'sf_exists': False
                })
        
        accuracy = (matches / total * 100) if total > 0 else 0
        
        if accuracy >= 99:
            status = 'pass'
        elif accuracy >= 95:
            status = 'warning'
        else:
            status = 'critical'
        
        # Log summary for this table
        status_emoji = {"pass": "✅", "warning": "⚠️", "critical": "❌"}[status]
        log_info(f"  {status_emoji} {table}: {matches}/{total} matches ({accuracy:.1f}%)")
        
        if mismatches:
            log_warning(f"    {len(mismatches)} mismatched records found:")
            for mismatch in mismatches[:3]:  # Show first 3
                log_warning(f"      • {mismatch.get('id', 'unknown')}: {mismatch.get('details', 'mismatch')}")
            if len(mismatches) > 3:
                log_warning(f"      • ... and {len(mismatches) - 3} more")
        
        return {
            'table': table,
            'status': status,
            'matches': matches,
            'total': total,
            'accuracy': accuracy,
            'mismatches': mismatches,
            'sample_ids': [str(id) for id in sample_ids[:5]]  # Sample for debugging
        }
    
    def records_match(self, pg_record: Tuple, sf_record: Tuple, table: str) -> bool:
        """Compare two records for substantial equality"""
        # For now, simplified comparison - just check if both exist
        # In full implementation, would compare field by field excluding CDC columns
        return pg_record is not None and sf_record is not None
    
    def records_match_detailed(self, pg_record: Tuple, sf_record: Tuple, table: str) -> Dict:
        """Compare two records with detailed difference reporting"""
        if pg_record is None:
            return {'matches': False, 'differences': 'PostgreSQL record not found'}
        if sf_record is None:
            return {'matches': False, 'differences': 'Snowflake record not found'}
        
        # Simple comparison - in production, would compare field by field
        # For now, just flag if lengths differ significantly
        pg_len = len([f for f in pg_record if f is not None])
        sf_len = len([f for f in sf_record if f is not None]) 
        
        # Account for CDC columns in Snowflake (subtract 3 for _SNOWFLAKE_* columns)
        sf_len_adjusted = sf_len - 3 if sf_len >= 3 else sf_len
        
        if abs(pg_len - sf_len_adjusted) > 1:  # Allow 1 field difference
            return {
                'matches': False, 
                'differences': f'Field count mismatch: PG={pg_len}, SF={sf_len_adjusted}'
            }
        
        return {'matches': True, 'differences': None}

def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def generate_markdown_report(results: List[ValidationResult], output_path: str, config: dict = None):
    """Generate markdown consistency report"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Calculate overall status
    critical_checks = [r for r in results if r.status == 'critical']
    warning_checks = [r for r in results if r.status == 'warning']
    
    if critical_checks:
        overall_status = "🔴 CRITICAL ISSUES DETECTED"
    elif warning_checks:
        overall_status = "🟡 WARNINGS DETECTED"
    else:
        overall_status = "🟢 ALL CHECKS PASSED"
    
    with open(output_path, 'w') as f:
        f.write(f"# CDC Consistency Check Report\n\n")
        f.write(f"**Generated**: {timestamp}\n")
        f.write(f"**Overall Status**: {overall_status}\n\n")
        
        f.write("## Executive Summary\n\n")
        f.write(f"- **Total Checks**: {len(results)}\n")
        f.write(f"- **Passing**: {len([r for r in results if r.status == 'pass'])}\n")
        f.write(f"- **Warnings**: {len(warning_checks)}\n")
        f.write(f"- **Critical**: {len(critical_checks)}\n\n")
        
        # Detailed results
        f.write("## Detailed Results\n\n")
        
        for result in results:
            status_emoji = {"pass": "✅", "warning": "⚠️", "critical": "❌"}[result.status]
            f.write(f"### {status_emoji} {result.check_name.replace('_', ' ').title()}\n\n")
            f.write(f"**Status**: {result.status.upper()}\n")
            f.write(f"**Timestamp**: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Add check-specific details
            if result.check_name == "table_counts":
                f.write("#### Table Count Results\n\n")
                f.write("| Table | PostgreSQL | Snowflake | Variance % | Status |\n")
                f.write("|-------|------------|-----------|------------|--------|\n")
                
                for count_result in result.details.get("count_results", []):
                    status_emoji = {"pass": "✅", "warning": "⚠️", "critical": "❌"}[count_result.status]
                    f.write(f"| {count_result.table_name} | {count_result.postgres_count:,} | {count_result.snowflake_count:,} | {count_result.variance_pct:.2f}% | {status_emoji} |\n")
                
            elif result.check_name == "table_synchronization":
                f.write("#### 🔄 Table Synchronization Overview\n\n")
                
                details = result.details
                common_tables = details.get('common_tables_count', 0)
                pg_only = details.get('pg_only_tables', [])
                sf_only = details.get('sf_only_tables', [])
                table_results = details.get('table_results', [])
                critical_tables = details.get('critical_tables', 0)
                warning_tables = details.get('warning_tables', 0)
                
                # Summary stats
                f.write("**Summary:**\n")
                f.write(f"- **Common Tables**: {common_tables}\n")
                f.write(f"- **PostgreSQL Only**: {len(pg_only)}\n")
                f.write(f"- **Snowflake Only**: {len(sf_only)}\n")
                f.write(f"- **Critical Sync Issues**: {critical_tables}\n")
                f.write(f"- **Warning Sync Issues**: {warning_tables}\n\n")
                
                if pg_only:
                    f.write(f"**⚠️ Tables Only in PostgreSQL**: {', '.join(pg_only)}\n\n")
                    
                if sf_only:
                    f.write(f"**⚠️ Tables Only in Snowflake**: {', '.join(sf_only)}\n\n")
                
                # Detailed table comparison
                if table_results:
                    f.write("**📊 Table Synchronization Status:**\n\n")
                    f.write("| Table | PG Records | SF Records | Difference | Difference % | Latest PG | Latest SF | Lag | Status |\n")
                    f.write("|-------|------------|------------|------------|--------------|-----------|-----------|-----|--------|\n")
                    
                    for table in table_results:
                        table_name = table['table_name']
                        pg_count = table['pg_count']
                        sf_count = table['sf_count']
                        count_diff = table['count_diff']
                        count_diff_pct = table['count_diff_pct']
                        pg_latest = table['pg_latest']
                        sf_latest = table['sf_latest']
                        lag_minutes = table['lag_minutes']
                        sync_status = table['sync_status']
                        
                        # Format timestamps
                        pg_time_str = format_timestamp_in_timezone(pg_latest, config or {}) if pg_latest else "None"
                        sf_time_str = format_timestamp_in_timezone(sf_latest, config or {}) if sf_latest else "None"
                        
                        # Format lag
                        if lag_minutes == float('inf'):
                            lag_str = "No SF data"
                        elif lag_minutes == 0:
                            lag_str = "0 min"
                        else:
                            lag_str = format_lag_time(lag_minutes)
                        
                        # Status emoji
                        status_emojis = {
                            'perfect': '🟢',
                            'good': '🟡', 
                            'warning': '🟠',
                            'critical': '🔴',
                            'empty': '⚪'
                        }
                        status_emoji = status_emojis.get(sync_status, '❓')
                        
                        # Format difference with sign
                        diff_str = f"{count_diff:+,}" if count_diff != 0 else "0"
                        diff_pct_str = f"{count_diff_pct:+.1f}%" if count_diff_pct != 0 else "0.0%"
                        
                        f.write(f"| {table_name} | {pg_count:,} | {sf_count:,} | {diff_str} | {diff_pct_str} | {pg_time_str} | {sf_time_str} | {lag_str} | {status_emoji} |\n")
                    
                    # Add legend
                    f.write(f"\n**Status Legend:**\n")
                    f.write(f"- 🟢 **Perfect**: Exact match, lag ≤ 5min\n")
                    f.write(f"- 🟡 **Good**: ≤1% difference, lag ≤ 30min\n")
                    f.write(f"- 🟠 **Warning**: ≤5% difference, lag ≤ 60min\n")
                    f.write(f"- 🔴 **Critical**: >5% difference or lag >60min\n")
                    f.write(f"- ⚪ **Empty**: No records in either database\n")
                    
                f.write(f"\n**Status**: {get_status_emoji(result.status)} {result.status.upper()}\n\n")
                
            elif result.check_name == "note_mismatch_analysis":
                f.write("#### Note Mismatch Analysis\n\n")
                f.write(f"**Analysis Period**: {result.details.get('analysis_period', 'N/A')}\n\n")
                
                missing_notes = result.details.get('total_missing_notes', 0)
                pg_count = result.details.get('pg_recent_count', 0)
                sf_count = result.details.get('sf_recent_count', 0)
                
                f.write(f"- **PostgreSQL Recent Notes**: {pg_count:,}\n")
                f.write(f"- **Snowflake Recent Notes**: {sf_count:,}\n")
                f.write(f"- **Missing in Snowflake**: {missing_notes:,}\n")
                f.write(f"- **Missing Percentage**: {(missing_notes/pg_count*100) if pg_count > 0 else 0:.2f}%\n\n")
                
                latest_pg = result.details.get('latest_pg_note', '')
                latest_sf = result.details.get('latest_sf_note', '')
                if latest_pg and latest_sf:
                    f.write(f"- **Latest PostgreSQL Note**: {latest_pg[:19]}\n")
                    f.write(f"- **Latest Snowflake Note**: {latest_sf[:19]}\n")
                
            elif result.check_name == "global_note_analysis":
                f.write("#### 📊 Global Note Statistics\n\n")
                
                details = result.details
                pg_total = details.get('pg_total_notes', 0)
                sf_total = details.get('sf_total_notes', 0)
                note_diff = details.get('note_count_diff', 0)
                note_pct = details.get('note_count_pct', 0)
                lag_formatted = details.get('lag_formatted', 'N/A')
                
                # Summary table
                f.write("| Metric | PostgreSQL | Snowflake | Difference |\n")
                f.write("|--------|------------|-----------|------------|\n")
                f.write(f"| **Total Notes** | {pg_total:,} | {sf_total:,} | {note_diff:,} ({note_pct:.1f}%) |\n")
                f.write(f"| **Companies with Notes** | {details.get('pg_companies_with_notes', 0)} | {details.get('sf_companies_with_notes', 0)} | {details.get('company_diff', 0)} |\n")
                f.write("\n")
                
                # Timestamp information
                if details.get('pg_latest_note') and details.get('sf_latest_note'):
                    try:
                        from datetime import datetime as dt
                        pg_latest = dt.fromisoformat(details['pg_latest_note'].replace('Z', '+00:00'))
                        sf_latest = dt.fromisoformat(details['sf_latest_note'].replace('Z', '+00:00'))
                        
                        f.write("**Latest Note Timestamps:**\n")
                        f.write(f"- **PostgreSQL**: {format_timestamp_in_timezone(pg_latest, config or {})}\n")
                        f.write(f"- **Snowflake**: {format_timestamp_in_timezone(sf_latest, config or {})}\n")
                        f.write(f"- **Global Lag**: {lag_formatted}\n")
                    except:
                        f.write(f"**Global Lag**: {lag_formatted}\n")
                else:
                    f.write(f"**Global Lag**: {lag_formatted}\n")
                
                f.write(f"\n**Status**: {get_status_emoji(result.status)} {result.status.upper()}\n\n")
                
            elif result.check_name == "company_note_discrepancies":
                f.write("#### 🔍 Company Note Discrepancies\n\n")
                
                details = result.details
                total_companies = details.get('total_companies_checked', 0)
                perfect_match = details.get('companies_perfect_match', 0)
                with_discrepancies = details.get('companies_with_discrepancies', 0)
                missing_from_sf = details.get('companies_missing_from_sf', 0)
                discrepancy_pct = details.get('discrepancy_percentage', 0)
                
                # Summary stats
                f.write("**Summary:**\n")
                f.write(f"- **Total Companies with Notes**: {total_companies:,}\n")
                f.write(f"- **Perfect Match**: {perfect_match:,} ({(perfect_match/total_companies*100):.1f}%)\n")
                f.write(f"- **With Discrepancies**: {with_discrepancies:,} ({discrepancy_pct:.1f}%)\n")
                f.write(f"- **Missing from Snowflake**: {missing_from_sf:,}\n\n")
                
                # Detailed breakdown table
                discrepant_companies = details.get('discrepant_companies', [])
                if discrepant_companies:
                    f.write("**❌ Companies Missing Notes in Snowflake:**\n\n")
                    f.write("| Company | Missing Notes | PG Notes | SF Notes | Latest PG Note | Latest SF Note | Company in Snowflake |\n")
                    f.write("|---------|---------------|----------|----------|----------------|----------------|-----------------------|\n")
                    
                    # Show ALL companies with discrepancies
                    for company in discrepant_companies:
                        company_name = (company.get('company_name') or 'Unknown')[:30]  # Truncate long names
                        missing_count = company.get('missing_count', 0)
                        pg_notes = company.get('pg_note_count', 0)
                        sf_notes = company.get('sf_note_count', 0)
                        in_snowflake = "Yes" if company.get('company_in_snowflake', False) else "No"
                        
                        # Format timestamps
                        pg_latest = company.get('pg_latest_note')
                        sf_latest = company.get('sf_latest_note')
                        
                        try:
                            if pg_latest and pg_latest != "None":
                                from datetime import datetime as dt
                                if isinstance(pg_latest, str) and pg_latest != "None":
                                    pg_latest = dt.fromisoformat(pg_latest.replace('Z', '+00:00'))
                                pg_time_str = format_timestamp_in_timezone(pg_latest, config or {})[:16] if pg_latest else "None"
                            else:
                                pg_time_str = "None"
                        except:
                            pg_time_str = "None"
                            
                        try:
                            if sf_latest and sf_latest != "None":
                                from datetime import datetime as dt
                                if isinstance(sf_latest, str) and sf_latest != "None":
                                    sf_latest = dt.fromisoformat(sf_latest.replace('Z', '+00:00'))
                                sf_time_str = format_timestamp_in_timezone(sf_latest, config or {})[:16] if sf_latest else "None"
                            else:
                                sf_time_str = "None"
                        except:
                            sf_time_str = "None"
                        
                        f.write(f"| {company_name} | {missing_count:,} | {pg_notes:,} | {sf_notes:,} | {pg_time_str} | {sf_time_str} | {in_snowflake} |\n")
                    
                    f.write(f"\n*Total: {len(discrepant_companies)} companies with discrepancies*\n")
                
                f.write(f"\n**Status**: {get_status_emoji(result.status)} {result.status.upper()}\n\n")
                
            elif result.check_name == "company_note_lag":
                if "company_results" in result.details:
                    f.write("#### Company Lag Analysis\n\n")
                    f.write(f"**Threshold**: {result.details.get('max_lag_threshold_minutes', 'N/A')} minutes\n\n")
                    
                    # Summary stats
                    companies = result.details["company_results"]
                    f.write(f"**Companies Checked**: {len(companies)}\n")
                    f.write(f"**Critical Issues**: {result.details.get('critical_companies', 0)}\n")
                    f.write(f"**Warnings**: {result.details.get('warning_companies', 0)}\n\n")
                    
                    # Table format for better readability
                    f.write("| Company | PG Notes | SF Notes | Missing | Latest PG Note | Latest SF Note | Lag (min) | Status |\n")
                    f.write("|---------|----------|----------|---------|----------------|----------------|-----------|--------|\n")
                    
                    for company in companies:
                        lag = company.get('lag_minutes', 0)
                        if lag == float('inf'):
                            lag_str = "Missing"
                        else:
                            lag_str = format_lag_time(lag)
                        
                        status_emoji = {"pass": "✅", "warning": "⚠️", "critical": "❌"}.get(company.get('status', 'unknown'), "❓")
                        
                        pg_notes = company.get('pg_note_count', 0)
                        sf_notes = company.get('sf_note_count', 0)
                        missing_notes = company.get('note_count_diff', 0)
                        
                        pg_time = company.get('latest_pg_note_time', '')
                        sf_time = company.get('latest_sf_note_time', '')
                        
                        # Format timestamps nicely
                        try:
                            if pg_time and pg_time != 'N/A':
                                from datetime import datetime as dt
                                if isinstance(pg_time, str):
                                    pg_dt = dt.fromisoformat(pg_time.replace('Z', '+00:00'))
                                else:
                                    pg_dt = pg_time
                                pg_time_short = format_timestamp_in_timezone(pg_dt, config or {})
                            else:
                                pg_time_short = 'N/A'
                        except:
                            pg_time_short = pg_time[:16] if pg_time else 'N/A'
                            
                        try:
                            if sf_time and sf_time != 'N/A':
                                from datetime import datetime as dt
                                if isinstance(sf_time, str):
                                    sf_dt = dt.fromisoformat(sf_time.replace('Z', '+00:00'))
                                else:
                                    sf_dt = sf_time
                                sf_time_short = format_timestamp_in_timezone(sf_dt, config or {})
                            else:
                                sf_time_short = 'N/A'
                        except:
                            sf_time_short = sf_time[:16] if sf_time else 'N/A'
                        
                        f.write(f"| {company.get('company_name', 'Unknown')} | {pg_notes:,} | {sf_notes:,} | {missing_notes} | {pg_time_short} | {sf_time_short} | {lag_str} | {status_emoji} |\n")
                    
                    # Show watchlist companies separately if present
                    watchlist_companies = [c for c in companies if any(pattern.lower() in c.get('company_name', '').lower() 
                                                                     for pattern in ['morpho', 'stripe', 'openai', 'monzo'])]
                    if watchlist_companies:
                        f.write(f"\n**Watchlist Companies ({len(watchlist_companies)}):**\n\n")
                        for company in watchlist_companies:
                            lag = company.get('lag_minutes', 0)
                            lag_str = f"{lag:.1f} minutes" if lag != float('inf') else "Missing in Snowflake"
                            status_emoji = {"pass": "✅", "warning": "⚠️", "critical": "❌"}.get(company.get('status', 'unknown'), "❓")
                            f.write(f"- **{company.get('company_name')}**: {lag_str} {status_emoji}\n")
                
            elif result.check_name == "user_synchronization":
                f.write("#### User Synchronization Results\n\n")
                f.write(f"- **Total PostgreSQL Users**: {result.details.get('total_pg_users', 0)}\n")
                f.write(f"- **Present in Snowflake**: {result.details.get('present_users', 0)}\n")
                f.write(f"- **Missing from Snowflake**: {result.details.get('missing_users', 0)}\n")
                f.write(f"- **Missing Percentage**: {result.details.get('missing_percentage', 0):.1f}%\n")
                
            elif result.check_name == "spot_checks":
                f.write("#### Spot Check Results\n\n")
                f.write(f"- **Overall Accuracy**: {result.details.get('overall_accuracy', 0):.1f}%\n")
                f.write(f"- **Threshold**: {result.details.get('accuracy_threshold', 0):.1f}%\n")
                f.write(f"- **Total Matches**: {result.details.get('total_matches', 0)}\n")
                f.write(f"- **Total Checks**: {result.details.get('total_checks', 0)}\n")
            
            if "error" in result.details:
                f.write(f"**Error**: {result.details['error']}\n")
            
            f.write("\n")
        
        f.write("---\n")
        f.write(f"*Report generated by CDC Consistency Check v1.0*\n")

@click.command()
@click.option('--config', default='config.yaml', help='Path to configuration file')
@click.option('--output-dir', default='./tmp', help='Output directory for reports')
@click.option('--env-file', default='.env', help='Path to .env file')
@click.option('--full', is_flag=True, help='Run full analysis (all checks). Default: company note analysis only')
def main(config, output_dir, env_file, full):
    """Run CDC consistency checks between Moneyball PostgreSQL and Snowflake"""
    
    # Load environment variables
    if os.path.exists(env_file):
        load_dotenv(env_file)
        log_success(f"Loaded environment from {env_file}")
    else:
        log_error(f"Environment file {env_file} not found")
        sys.exit(1)
    
    # Load configuration
    if not os.path.exists(config):
        log_error(f"Configuration file {config} not found")
        sys.exit(1)
    
    try:
        config_data = load_config(config)
        log_success(f"Loaded configuration from {config}")
    except Exception as e:
        log_error(f"Failed to load configuration: {e}")
        sys.exit(1)
    
    # Get database URLs
    moneyball_url = os.getenv('MONEYBALL_DATABASE_URL')
    snowflake_url = os.getenv('SNOWFLAKE_DATABASE_URL')
    
    if not moneyball_url:
        log_error("MONEYBALL_DATABASE_URL not set in environment")
        sys.exit(1)
    
    if not snowflake_url:
        log_error("SNOWFLAKE_DATABASE_URL not set in environment")
        sys.exit(1)
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Setup logging with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = output_path / f"consistency_report_{timestamp}.log"
    setup_logging(str(log_file_path))
    
    try:
        # Initialize database connector
        db_connector = DatabaseConnector(moneyball_url, snowflake_url)
        
        # Initialize consistency checker
        checker = ConsistencyChecker(config_data, db_connector)
        
        # Run checks based on mode
        if full:
            log_info("🔍 Running FULL analysis (all checks)...")
            results = checker.run_all_checks()
        else:
            log_info("🎯 Running company note analysis only (use --full for complete analysis)...")
            results = checker.run_company_analysis_only()
        
        # Generate report
        report_path = output_path / f"consistency_report_{timestamp}.md"
        
        generate_markdown_report(results, str(report_path), config_data)
        
        log_success(f"Consistency check completed!")
        log_info(f"Report generated: {report_path}")
        log_info(f"Log file: {log_file_path}")
        
        # Print summary
        critical_count = len([r for r in results if r.status == 'critical'])
        warning_count = len([r for r in results if r.status == 'warning'])
        pass_count = len([r for r in results if r.status == 'pass'])
        
        print(f"\n📊 Summary:")
        print(f"   ✅ Passed: {pass_count}")
        print(f"   ⚠️  Warnings: {warning_count}")
        print(f"   ❌ Critical: {critical_count}")
        
        if critical_count > 0:
            print(f"\n🚨 Critical issues detected! Review the report for details.")
            sys.exit(1)
        elif warning_count > 0:
            print(f"\n⚠️  Warnings detected. Review the report for details.")
        else:
            print(f"\n🎉 All checks passed!")
        
    except Exception as e:
        log_error(f"Consistency check failed: {e}")
        sys.exit(1)
    
    finally:
        if 'db_connector' in locals():
            db_connector.close_connections()

if __name__ == '__main__':
    main()
