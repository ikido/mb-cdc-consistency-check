#!/usr/bin/env python3
"""
Schema Dump Script for CDC Consistency Check
Phase 1: Extract schemas from Moneyball PostgreSQL and Snowflake databases
"""

import os
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import psycopg2
import snowflake.connector
from dotenv import load_dotenv
import click
from colorama import init, Fore, Style

# Initialize colorama for colored output
init()

def log_info(message):
    """Print info message with color"""
    print(f"{Fore.BLUE}[INFO]{Style.RESET_ALL} {message}")

def log_success(message):
    """Print success message with color"""
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} {message}")

def log_error(message):
    """Print error message with color"""
    print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} {message}")

def log_warning(message):
    """Print warning message with color"""
    print(f"{Fore.YELLOW}[WARNING]{Style.RESET_ALL} {message}")

def parse_postgres_url(url):
    """Parse PostgreSQL URL into connection parameters"""
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/'),
        'user': parsed.username,
        'password': parsed.password
    }

def parse_snowflake_url(url):
    """Parse Snowflake URL into connection parameters"""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    
    return {
        'account': parsed.hostname,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path.split('/')[1] if len(parsed.path.split('/')) > 1 else None,
        'schema': parsed.path.split('/')[2] if len(parsed.path.split('/')) > 2 else None,
        'warehouse': query_params.get('warehouse', [None])[0],
        'role': query_params.get('role', [None])[0]
    }

def get_postgres_schema(conn_params, tables):
    """Extract schema from PostgreSQL database"""
    log_info("Connecting to Moneyball PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        schema_info = {}
        
        for table in tables:
            log_info(f"Extracting schema for table: {table}")
            
            # Get table schema
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table,))
            
            columns = cursor.fetchall()
            
            if not columns:
                log_warning(f"Table '{table}' not found in PostgreSQL database")
                continue
            
            # Get primary keys
            cursor.execute("""
                SELECT column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                ORDER BY kcu.ordinal_position
            """, (table,))
            
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Get foreign keys
            cursor.execute("""
                SELECT 
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.table_name = %s AND tc.constraint_type = 'FOREIGN KEY'
            """, (table,))
            
            foreign_keys = cursor.fetchall()
            
            # Get indexes
            cursor.execute("""
                SELECT DISTINCT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s
            """, (table,))
            
            indexes = cursor.fetchall()
            
            schema_info[table] = {
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'indexes': indexes
            }
        
        cursor.close()
        conn.close()
        log_success("PostgreSQL schema extraction completed")
        return schema_info
        
    except Exception as e:
        log_error(f"Failed to connect to PostgreSQL: {e}")
        return None

def get_snowflake_schema(conn_params, tables):
    """Extract schema from Snowflake database"""
    log_info("Connecting to Snowflake...")
    
    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        
        # Debug: Show current context
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        context = cursor.fetchone()
        log_info(f"Snowflake context - DB: {context[0]}, Schema: {context[1]}, Warehouse: {context[2]}, Role: {context[3]}")
        
        # Debug: Show all tables
        cursor.execute("SHOW TABLES")
        all_tables = cursor.fetchall()
        log_info(f"Found {len(all_tables)} tables in current schema:")
        for table in all_tables:
            log_info(f"  - {table[1]}")
        
        schema_info = {}
        
        for table in tables:
            # Try both lowercase and uppercase table names
            log_info(f"Extracting schema for table: {table}")
            
            # Get table schema - try lowercase first, then uppercase
            table_found = False
            for table_variant in [table.lower(), table.upper()]:
                cursor.execute(f"""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale
                    FROM information_schema.columns 
                    WHERE table_name = '{table_variant}'
                    ORDER BY ordinal_position
                """)
                columns = cursor.fetchall()
                if columns:
                    table_found = True
                    log_info(f"Found table as: {table_variant}")
                    break
            
            if not table_found:
                log_warning(f"Table '{table}' not found in Snowflake database")
                continue
            
            # Try to get primary keys and foreign keys (may not have permissions)
            primary_keys = []
            foreign_keys = []
            
            try:
                cursor.execute(f"""
                    SELECT column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_name = '{table_variant}' AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                """)
                primary_keys = [row[0] for row in cursor.fetchall()]
            except Exception as e:
                log_warning(f"Could not fetch primary keys for {table}: {e}")
            
            try:
                cursor.execute(f"""
                    SELECT 
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage ccu 
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.table_name = '{table_variant}' AND tc.constraint_type = 'FOREIGN KEY'
                """)
                foreign_keys = cursor.fetchall()
            except Exception as e:
                log_warning(f"Could not fetch foreign keys for {table}: {e}")
            
            schema_info[table] = {
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'indexes': []  # Snowflake doesn't expose indexes via information_schema
            }
        
        cursor.close()
        conn.close()
        log_success("Snowflake schema extraction completed")
        return schema_info
        
    except Exception as e:
        log_error(f"Failed to connect to Snowflake: {e}")
        return None

def generate_schema_report(pg_schema, sf_schema, output_dir):
    """Generate markdown schema reports"""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Generate PostgreSQL schema report
    pg_file = output_path / f"postgresql_schema_{timestamp}.md"
    generate_database_schema_md(pg_schema, pg_file, "Moneyball PostgreSQL")
    
    # Generate Snowflake schema report
    sf_file = output_path / f"snowflake_schema_{timestamp}.md"
    generate_database_schema_md(sf_schema, sf_file, "Snowflake")
    
    # Generate comparison report
    comparison_file = output_path / f"schema_comparison_{timestamp}.md"
    generate_comparison_md(pg_schema, sf_schema, comparison_file)
    
    log_success(f"Schema reports generated in {output_dir}")
    log_info(f"PostgreSQL schema: {pg_file}")
    log_info(f"Snowflake schema: {sf_file}")
    log_info(f"Comparison report: {comparison_file}")

def generate_database_schema_md(schema_info, output_file, db_name):
    """Generate markdown schema report for a single database"""
    with open(output_file, 'w') as f:
        f.write(f"# {db_name} Schema Report\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        for table_name, table_info in schema_info.items():
            f.write(f"## Table: {table_name}\n\n")
            
            # Columns
            f.write("### Columns\n\n")
            f.write("| Column Name | Data Type | Nullable | Default | Max Length | Precision | Scale |\n")
            f.write("|-------------|-----------|----------|---------|------------|-----------|-------|\n")
            
            for col in table_info['columns']:
                col_name = col[0]
                data_type = col[1]
                is_nullable = col[2]
                default_val = col[3] or ""
                max_length = col[4] or ""
                precision = col[5] or ""
                scale = col[6] or ""
                
                f.write(f"| {col_name} | {data_type} | {is_nullable} | {default_val} | {max_length} | {precision} | {scale} |\n")
            
            # Primary Keys
            if table_info['primary_keys']:
                f.write(f"\n### Primary Keys\n")
                f.write(f"- {', '.join(table_info['primary_keys'])}\n")
            
            # Foreign Keys
            if table_info['foreign_keys']:
                f.write(f"\n### Foreign Keys\n")
                for fk in table_info['foreign_keys']:
                    f.write(f"- {fk[0]} → {fk[1]}.{fk[2]}\n")
            
            # Indexes (PostgreSQL only)
            if table_info['indexes']:
                f.write(f"\n### Indexes\n")
                for idx in table_info['indexes']:
                    f.write(f"- **{idx[0]}**: `{idx[1]}`\n")
            
            f.write("\n---\n\n")

def generate_comparison_md(pg_schema, sf_schema, output_file):
    """Generate schema comparison report"""
    with open(output_file, 'w') as f:
        f.write("# Schema Comparison Report\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Table Presence\n\n")
        f.write("| Table | PostgreSQL | Snowflake |\n")
        f.write("|-------|------------|----------|\n")
        
        all_tables = set(pg_schema.keys()) | set(sf_schema.keys())
        for table in sorted(all_tables):
            pg_present = "✅" if table in pg_schema else "❌"
            sf_present = "✅" if table in sf_schema else "❌"
            f.write(f"| {table} | {pg_present} | {sf_present} |\n")
        
        f.write("\n## Column Comparison\n\n")
        
        for table in sorted(set(pg_schema.keys()) & set(sf_schema.keys())):
            f.write(f"### Table: {table}\n\n")
            
            pg_columns = {col[0]: col[1] for col in pg_schema[table]['columns']}
            sf_columns = {col[0]: col[1] for col in sf_schema[table]['columns']}
            
            all_columns = set(pg_columns.keys()) | set(sf_columns.keys())
            
            f.write("| Column | PostgreSQL Type | Snowflake Type | Match |\n")
            f.write("|--------|-----------------|----------------|-------|\n")
            
            for col in sorted(all_columns):
                pg_type = pg_columns.get(col, "MISSING")
                sf_type = sf_columns.get(col, "MISSING")
                match = "✅" if pg_type == sf_type and pg_type != "MISSING" else "❌"
                
                f.write(f"| {col} | {pg_type} | {sf_type} | {match} |\n")
            
            f.write("\n")

@click.command()
@click.option('--env-file', default='.env', help='Path to .env file')
@click.option('--output-dir', default='./tmp', help='Output directory for schema files')
def main(env_file, output_dir):
    """Dump schemas from Moneyball PostgreSQL and Snowflake databases"""
    
    # Load environment variables
    if os.path.exists(env_file):
        load_dotenv(env_file)
        log_success(f"Loaded environment from {env_file}")
    else:
        log_error(f"Environment file {env_file} not found")
        log_info("Please copy env.template to .env and configure your database URLs")
        sys.exit(1)
    
    # Get environment variables
    moneyball_url = os.getenv('MONEYBALL_DATABASE_URL')
    snowflake_url = os.getenv('SNOWFLAKE_DATABASE_URL')
    target_tables = os.getenv('TARGET_TABLES', 'companies,transactions,funds,notes,users').split(',')
    
    if not moneyball_url:
        log_error("MONEYBALL_DATABASE_URL not set in environment")
        sys.exit(1)
    
    if not snowflake_url:
        log_error("SNOWFLAKE_DATABASE_URL not set in environment")
        sys.exit(1)
    
    # Strip whitespace from table names
    target_tables = [table.strip() for table in target_tables]
    
    log_info(f"Target tables: {', '.join(target_tables)}")
    log_info(f"Output directory: {output_dir}")
    
    # Parse connection URLs
    try:
        pg_params = parse_postgres_url(moneyball_url)
        sf_params = parse_snowflake_url(snowflake_url)
    except Exception as e:
        log_error(f"Failed to parse database URLs: {e}")
        sys.exit(1)
    
    # Extract schemas
    pg_schema = get_postgres_schema(pg_params, target_tables)
    if pg_schema is None:
        log_error("Failed to extract PostgreSQL schema")
        sys.exit(1)
    
    sf_schema = get_snowflake_schema(sf_params, target_tables)
    if sf_schema is None:
        log_error("Failed to extract Snowflake schema - generating PostgreSQL-only report")
        sf_schema = {}
    
    # Generate reports
    generate_schema_report(pg_schema, sf_schema, output_dir)
    
    log_success("Schema dump completed successfully!")

if __name__ == '__main__':
    main()
