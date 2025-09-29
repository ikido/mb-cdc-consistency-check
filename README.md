# CDC Consistency Check

A comprehensive data consistency validation system to ensure Change Data Capture (CDC) pipeline from Moneyball PostgreSQL database to Snowflake is functioning correctly.

## Overview

This system validates CDC pipeline health by comparing data between PostgreSQL (source of truth) and Snowflake (CDC replica) across critical tables: `companies`, `transactions`, `funds`, `notes`, and `users`.

**Key Features:**
- 🔍 **Global Statistics**: Total note counts and timestamp lag analysis
- 📊 **Company-Level Analysis**: Detailed note discrepancy tracking for all companies  
- ⏱️ **Lag Analysis**: Timezone-aware timestamp comparison for selected companies
- 📈 **Watchlist Monitoring**: Special tracking for critical companies (Morpho, Stripe, Monzo)
- 📝 **Comprehensive Reports**: Markdown reports with actionable insights
- 🎯 **Smart Filtering**: Only reports actual discrepancies (ignores 0-note companies)

## Prerequisites

- Python 3.8 or higher
- `uv` package manager ([install here](https://docs.astral.sh/uv/getting-started/installation/))
- Access to Moneyball PostgreSQL database
- Access to Snowflake account with appropriate permissions

## Quick Setup

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Setup project with virtual environment and dependencies
./setup.sh
```

## Manual Installation

1. **Clone or setup the project directory**
   ```bash
   cd /path/to/mb-cdc-consistency-check
   ```

2. **Create a virtual environment with uv**
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies with uv**
   ```bash
   uv pip install -r requirements.txt
   ```

## Database Setup

### PostgreSQL (Moneyball)
If you already have a connection string, you can skip this section.

### Snowflake Setup

You'll need to create a readonly user in Snowflake and get the connection details.

#### Step 1: Create a Readonly Role

Connect to Snowflake as an admin and run the following SQL commands:

```sql
-- Create a readonly role for CDC validation
CREATE ROLE IF NOT EXISTS mb_cdc_validator_role;

-- Grant usage on database and schema
GRANT USAGE ON DATABASE MONEYBALL TO ROLE mb_cdc_validator_role;
GRANT USAGE ON SCHEMA MONEYBALL."public" TO ROLE mb_cdc_validator_role;

-- Grant select permissions on all tables in the schema
GRANT SELECT ON ALL TABLES IN SCHEMA MONEYBALL."public" TO ROLE mb_cdc_validator_role;

-- Grant select permissions on future tables (optional, for new tables)
GRANT SELECT ON FUTURE TABLES IN SCHEMA MONEYBALL."public" TO ROLE mb_cdc_validator_role;

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE OPENFLOW_WAREHOUSE TO ROLE mb_cdc_validator_role;
```

#### Step 2: Create a User and Assign the Role

```sql
-- Create user (replace with your desired username and password)
CREATE USER IF NOT EXISTS mb_cdc_validator
    PASSWORD = 'heather-castiel-frannie1!'
    DEFAULT_WAREHOUSE = 'OPENFLOW_WAREHOUSE'
    MUST_CHANGE_PASSWORD = FALSE;

-- Assign the readonly role to the user
GRANT ROLE mb_cdc_validator_role TO USER mb_cdc_validator;

-- Set the role as default for the user
ALTER USER mb_cdc_validator SET DEFAULT_ROLE = mb_cdc_validator_role;
```

#### Step 3: Get Your Snowflake Connection Details

You'll need these values for your connection string:

- **Account**: Your Snowflake account identifier: `RIB63479.us-east-1`
  - From `SELECT CURRENT_ACCOUNT(), CURRENT_REGION()` in Snowflake: `RIB63479` + `AWS_US_EAST_1`
- **User**: The username you created (`mb_cdc_validator`)
- **Password**: The password you set
- **Database**: `MONEYBALL`
- **Schema**: `public` (case sensitive, will be quoted in URL)
- **Warehouse**: `OPENFLOW_WAREHOUSE`
- **Role**: `mb_cdc_validator_role`

## Configuration

1. **The setup script creates a `.env` file for you, or copy the template manually**
   ```bash
   # If not created by setup.sh:
   cp env.template .env  # if you have the template
   ```

2. **Edit the `.env` file with your database connections**
   ```bash
   # Database Connection URLs
   MONEYBALL_DATABASE_URL=postgresql://username:password@host:port/database
   SNOWFLAKE_DATABASE_URL=snowflake://mb_cdc_validator:USER_PASSWORD@RIB63479.us-east-1/MONEYBALL/public?warehouse=OPENFLOW_WAREHOUSE&role=mb_cdc_validator_role
   
   # Target Tables (comma-separated)
   TARGET_TABLES=companies,transactions,funds,notes,users
   
   # Output Configuration
   OUTPUT_DIR=./tmp
   ```

3. **Configure validation settings in `config.yaml`**
   
   The system uses `config.yaml` for validation thresholds and display settings:
   ```yaml
   validation_rules:
     skip_detailed_checks_if_perfect_count: true
     
   lag_thresholds:
     general_tables: 5        # minutes
     company_notes: 2         # minutes for top companies
     fund_transactions: 3     # minutes
     user_updates: 5          # minutes
     
   display:
     timezone: "US/Pacific"   # Timezone for timestamp display
     datetime_format: "%b %d, %Y %H:%M"  # Format: Sep 17, 2025 06:52
   ```

### Snowflake URL Format

The Snowflake URL format is:
```
snowflake://username:password@account/database/schema?warehouse=warehouse_name&role=role_name
```

**Your specific connection:**
```
snowflake://mb_cdc_validator:USER_PASSWORD@RIB63479.us-east-1/MONEYBALL/public?warehouse=OPENFLOW_WAREHOUSE&role=mb_cdc_validator_role
```

## Usage

### 🚀 Quick Start: Run CDC Consistency Check

**Make sure your virtual environment is activated first:**
```bash
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

#### Default Mode: Company Note Analysis (Recommended)
```bash
python consistency_check.py
```

This runs the most important checks focusing on company note discrepancies:
- ✅ Global note statistics (total counts, lag)
- ✅ Company-level note discrepancies (all companies with missing notes)  
- ✅ Company lag analysis (top 20 companies + watchlist)

#### Full Analysis Mode
```bash
python consistency_check.py --full
```

This runs all available checks including table counts, user synchronization, and spot checks.

#### Custom Configuration
```bash
python consistency_check.py --config custom_config.yaml --output-dir ./reports --env-file prod.env
```

### 📊 Schema Discovery (Optional)

To understand database structures:

```bash
python schema_dump.py
```

**Options:**
- `--env-file`: Path to .env file (default: `.env`)
- `--output-dir`: Output directory for schema files (default: `./tmp`)

## 📋 Report Locations

All reports are generated in the `./tmp` directory with timestamps:

### 🎯 CDC Consistency Reports

**Main Reports:**
```
./tmp/consistency_report_YYYY-MM-DD_HH-MM-SS.md  # Main consistency report
./tmp/consistency_report_YYYY-MM-DD_HH-MM-SS.log # Complete execution log
```

**Report Sections:**
1. **📊 Global Note Analysis**
   - Total note counts (PostgreSQL vs Snowflake)
   - Global timestamp lag analysis
   - Overall health status

2. **❌ Companies Missing Notes in Snowflake**
   - Complete list of companies with note discrepancies
   - Shows: missing count, latest timestamps, company presence in Snowflake
   - Only includes companies that actually have notes in PostgreSQL

3. **⏱️ Company Note Lag**
   - Detailed analysis of top 20 companies + watchlist
   - Timezone-aware timestamp comparison
   - Human-readable lag format (e.g., "96021.0 minutes (66 days and 16 hours)")
   - Status indicators for each company

### 📋 Schema Reports (Optional)

```
./tmp/postgresql_schema_YYYY-MM-DD_HH-MM-SS.md   # PostgreSQL schema
./tmp/snowflake_schema_YYYY-MM-DD_HH-MM-SS.md    # Snowflake schema  
./tmp/schema_comparison_YYYY-MM-DD_HH-MM-SS.md   # Side-by-side comparison
```

### 📈 Understanding Report Status

**Status Levels:**
- ✅ **PASS**: All checks passed, data is consistent
- ⚠️ **WARNING**: Minor discrepancies within acceptable thresholds
- ❌ **CRITICAL**: Significant discrepancies requiring attention

**Key Metrics to Monitor:**
- **Global note count difference**: Should be < 1%
- **Global timestamp lag**: Should be < 30 minutes  
- **Company discrepancy rate**: Should be < 5%
- **Watchlist company lag**: Should be < 2 minutes

## 🔧 Configuration Options

### Command Line Options

```bash
python consistency_check.py [OPTIONS]

Options:
  --config PATH      Path to configuration file (default: config.yaml)
  --output-dir PATH  Output directory for reports (default: ./tmp)
  --env-file PATH    Path to .env file (default: .env)
  --full             Run full analysis (default: company note analysis only)
  --help             Show help message
```

### Configuration File (`config.yaml`)

```yaml
# Tables to validate
target_tables:
  - companies
  - transactions
  - funds
  - notes
  - users

# Validation rules
validation_rules:
  skip_detailed_checks_if_perfect_count: true
  max_companies_to_check: 20
  
# Lag thresholds (in minutes)
lag_thresholds:
  general_tables: 5
  company_notes: 2
  fund_transactions: 3
  user_updates: 5

# Company watchlist (monitored separately)
company_watchlist:
  name_patterns:
    - morpho
    - stripe
    - openai
    - monzo

# Display settings
display:
  timezone: "US/Pacific"  # US/Pacific, UTC, US/Eastern
  datetime_format: "%b %d, %Y %H:%M"  # Sep 17, 2025 06:52
```

## 🔍 Monitoring Best Practices

### Daily Health Checks
```bash
# Quick health check (default mode)
python consistency_check.py

# Review the generated report in ./tmp/
# Focus on: Global lag, company discrepancy count, watchlist status
```

### Weekly Deep Dive
```bash
# Full analysis
python consistency_check.py --full

# Review: All table counts, user synchronization, detailed spot checks
```

### Alert Thresholds

Monitor these metrics for CDC pipeline health:

**🚨 Critical Alerts:**
- Global note lag > 60 minutes
- Company discrepancy rate > 10%
- Any watchlist company lag > 5 minutes
- User synchronization failure

**⚠️ Warning Alerts:**
- Global note lag > 30 minutes
- Company discrepancy rate > 5%
- Note count difference > 1%

## Troubleshooting

### Common Issues

#### PostgreSQL Connection Issues
```
Failed to connect to PostgreSQL: FATAL: password authentication failed
```
**Solution:** Verify your PostgreSQL credentials and connection string format.

#### Snowflake Connection Issues
```
Failed to connect to Snowflake: 250001 (08001): Failed to connect to DB
```
**Solutions:**
1. Verify your Snowflake account identifier is correct
2. Check username/password
3. Ensure the user has the correct role assigned
4. Verify database, schema, and warehouse names
5. Check if your IP is whitelisted (if network policies are in place)

#### Missing Tables
```
Table 'companies' not found in PostgreSQL database
```
**Solutions:**
1. Verify table names are correct (case-sensitive in some databases)
2. Check if you're connected to the right database/schema
3. Ensure you have permission to access the tables

#### Zero Companies in Report
If the "Companies Missing Notes in Snowflake" section shows 0 companies, this is likely correct behavior - the system only reports companies that have notes in PostgreSQL but are missing some/all in Snowflake.

### Getting Help

#### Check Snowflake Connection Details
Run this in Snowflake to verify your current context:
```sql
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE();
```

#### Test Database Connections
You can test connections individually:

**PostgreSQL:**
```bash
psql "postgresql://username:password@host:port/database" -c "SELECT current_database();"
```

**Snowflake:**
```python
import snowflake.connector
conn = snowflake.connector.connect(
    account='your_account',
    user='your_user',
    password='your_password',
    database='your_database',
    schema='your_schema',
    warehouse='your_warehouse',
    role='your_role'
)
print("Connected successfully!")
```

#### Debug Mode
For detailed logging, check the generated `.log` files in `./tmp/` - they contain complete execution details.

## Security Notes

- Store your `.env` file securely and never commit it to version control
- Use readonly credentials with minimal required permissions
- Consider using Snowflake key-pair authentication for production use
- Regularly rotate credentials
- The `./tmp` directory is in `.gitignore` but accessible for IDE indexing

## Architecture

```
CDC Consistency Check System
├── consistency_check.py    # Main consistency validation engine
├── schema_dump.py          # Schema discovery and comparison
├── config.yaml            # Validation rules and thresholds
├── .env                   # Database connection strings (not in git)
├── requirements.txt       # Python dependencies
└── tmp/                   # Generated reports and logs
    ├── consistency_report_*.md   # Main CDC health reports
    ├── consistency_report_*.log  # Execution logs
    └── schema_*.md              # Schema documentation
```

## Support

For issues with:
- **PostgreSQL connections**: Check with your database administrator
- **Snowflake setup**: Refer to [Snowflake documentation](https://docs.snowflake.com/) or contact your Snowflake administrator
- **Script issues**: Check the error messages and troubleshooting section above
- **CDC Pipeline Issues**: Use the generated reports to identify specific discrepancies and lag issues

---

**🎯 Quick Reference:**
- **Run check**: `python consistency_check.py`
- **View reports**: `./tmp/consistency_report_*.md`
- **View logs**: `./tmp/consistency_report_*.log`
- **Configure**: Edit `config.yaml` and `.env`