# Product Requirements Document: CDC Consistency Check System

## Overview
A comprehensive data consistency validation system to ensure Change Data Capture (CDC) pipeline from Moneyball PostgreSQL database to Snowflake is functioning correctly.

## Background
- **Source Database**: Moneyball PostgreSQL (source of truth)
- **Target Database**: Snowflake (CDC replica via Openflow - should be correct)
- **Goal**: Validate that CDC pipeline maintains data integrity and timeliness
- **Key Assumption**: Snowflake data should match Moneyball data with minimal lag

## Core Requirements

### 1. Schema Documentation
- **Requirement**: Dump database schemas from both databases
- **Scope**: All tables in scope (companies, transactions, funds, notes, users)
- **Output**: Separate schema dumps for Postgres and Snowflake

### 2. Data Consistency Checks

#### 2.1 Core Metrics
**Primary Focus**: Count differences and lag times

For tables: `companies`, `transactions`, `funds`, `notes`, `users`
- **Row Count Variance**: How off is the count between databases
- **Data Lag**: How far behind is Snowflake table (time-based analysis)
- **Random Spot Check**: Sample random records for field-by-field comparison

#### 2.2 Business Logic Validation

##### Company-Centric Lag Analysis
- **Configurable Sample Size**: Select X companies with most recent notes in Moneyball (default: 20)
- **Note Lag Analysis**: For each selected company:
  - **Key Timestamp Columns**: `notes.created_at`, `notes.updated_at`, `notes.actual_created_at`, `notes.actual_updated_at`
  - Compare most recent note timestamp in Moneyball vs Snowflake
  - Measure lag time for note replication using `created_at` as primary field
  - Flag companies with significant delays
- **Company Identification**: Use `companies.id` (UUID) and `notes.company_id` for linkage

##### Fund-Transaction Lag Analysis
- **Transaction Recency Check**: For each fund:
  - **Key Timestamp Columns**: `transactions.created_at`, `transactions.date`
  - **Linkage**: `transactions.company_id` → `companies.id` → fund relationships
  - Compare most recent transaction timestamp in Moneyball vs Snowflake
  - Measure lag time for transaction replication
  - Identify funds with delayed transaction updates

##### User Data Synchronization
- **Requirement**: Ensure all active users in Moneyball exist in Snowflake
- **Primary Key**: `users.id` (UUID)
- **Check**: Verify that all active users from Moneyball are present in Snowflake users table
- **Key Fields**: `users.created_at`, `users.updated_at` for activity tracking

## Technical Specifications

### 3. Deliverables

#### 3.1 Schema Dump Script (`schema_dump.py`)
- Connect to both databases
- Extract DDL for all tables
- Save separate schema files for each database
- Output in readable SQL format

#### 3.2 Consistency Check Script (`consistency_check.py`)
- Modular design with separate validation functions
- Configurable sample sizes and thresholds
- Detailed logging and reporting
- Markdown report generation

#### 3.3 Configuration Management
- Database connection URLs via dotenv (`.env` file)
- Table lists and validation rules in YAML config
- **Company sample size**: Configurable number of companies to check (default: 20)
- Lag thresholds and alert parameters
- Environment-specific settings

## Schema-Specific Implementation Details

### 4. Database Schema Knowledge

#### 4.1 Table Structures and Key Fields

##### Companies Table
- **Primary Key**: `id` (UUID)
- **Key Timestamp Fields**: `created_at`, `updated_at`, `actual_created_at`, `actual_updated_at`
- **Business Fields**: `sr_name`, `status`, `stage`, `responsible`
- **Snowflake Additions**: `_SNOWFLAKE_DELETED`, `_SNOWFLAKE_INSERTED_AT`, `_SNOWFLAKE_UPDATED_AT`

##### Notes Table  
- **Primary Key**: `id` (UUID)
- **Foreign Keys**: `user_id`, `company_id` (implicit via business logic)
- **Key Timestamp Fields**: `created_at`, `updated_at`, `actual_created_at`, `actual_updated_at`, `meeting_date`
- **Business Fields**: `description`, `score`, `type`, `plus_minus`

##### Transactions Table
- **Primary Key**: `id` (UUID) 
- **Foreign Keys**: `company_id` (links to companies)
- **Key Timestamp Fields**: `created_at`, `date`
- **Business Fields**: `type`, `vehicle`, `investment_round`, `transaction_amount`, `shares`

##### Funds Table
- **Primary Key**: `id` (UUID)
- **Key Timestamp Fields**: `created_at`, `updated_at`
- **Business Fields**: `name`, `status`, `vintage_year`

##### Users Table
- **Primary Key**: `id` (UUID)
- **Key Timestamp Fields**: `created_at`, `updated_at`
- **Business Fields**: `email`, `name`, `active` status

#### 4.2 Data Type Mappings (PostgreSQL → Snowflake)
- `uuid` → `TEXT`
- `timestamp with time zone` → `TIMESTAMP_LTZ`
- `character varying` → `TEXT`
- `jsonb` → `VARIANT`
- `boolean` → `BOOLEAN`
- `bigint` → `NUMBER`
- `numeric` → `NUMBER`

#### 4.3 CDC-Specific Columns in Snowflake
All tables include these Snowflake-managed fields:
- `_SNOWFLAKE_DELETED` (BOOLEAN): Soft delete flag
- `_SNOWFLAKE_INSERTED_AT` (TIMESTAMP_NTZ): CDC insertion timestamp
- `_SNOWFLAKE_UPDATED_AT` (TIMESTAMP_NTZ): CDC update timestamp

#### 4.4 Specific Validation Queries

##### Count Validation Queries
```sql
-- PostgreSQL
SELECT 'companies' as table_name, COUNT(*) as count FROM companies
UNION ALL
SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'funds', COUNT(*) FROM funds
UNION ALL  
SELECT 'notes', COUNT(*) FROM notes
UNION ALL
SELECT 'users', COUNT(*) FROM users;

-- Snowflake (same query structure)
```

##### Company-Centric Lag Analysis Queries
```sql
-- PostgreSQL: Get top 20 companies with most recent notes
SELECT c.id, c.sr_name, MAX(n.created_at) as latest_note_time
FROM companies c
JOIN notes n ON n.company_id = c.id  -- Note: may need business logic to link
ORDER BY latest_note_time DESC
LIMIT 20;

-- Snowflake: Check same companies for lag
SELECT c.id, c.sr_name, MAX(n.created_at) as latest_note_time_sf
FROM companies c
JOIN notes n ON n.company_id = c.id
WHERE c.id IN (list_of_company_ids_from_pg)
GROUP BY c.id, c.sr_name;
```

##### Transaction Lag Analysis
```sql
-- PostgreSQL: Latest transactions per fund
SELECT f.id as fund_id, f.name, MAX(t.created_at) as latest_transaction
FROM funds f
JOIN companies c ON c.fund_id = f.id  -- Business logic linkage
JOIN transactions t ON t.company_id = c.id
GROUP BY f.id, f.name;
```

##### User Synchronization Check
```sql
-- Find users in PostgreSQL but missing in Snowflake
SELECT pg.id, pg.email, pg.created_at
FROM postgresql_users pg
LEFT JOIN snowflake_users sf ON pg.id = sf.id
WHERE sf.id IS NULL;
```

### 5. Validation Criteria

#### 5.1 Pass/Fail Thresholds (Configurable)
All thresholds should be configurable via YAML config file:
- **Row Count Variance**: ±1% tolerance (primary metric)
- **Data Lag Thresholds**: 
  - General tables: Maximum 5 minutes
  - Company notes: Maximum 2 minutes for top companies
  - Fund transactions: Maximum 3 minutes
- **Spot Check Accuracy**: 99.9% field match rate
- **Schema Documentation**: Complete DDL export for both databases

#### 5.2 Critical vs Warning Issues
- **Critical**: Missing recent data, significant count discrepancies
- **Warning**: Minor timestamp differences, non-critical field mismatches

### 6. Reporting Requirements

#### 6.1 Executive Summary
- Overall system health status
- Key metrics and trends
- Critical issues requiring attention

#### 6.2 Detailed Reports
- Table-by-table analysis
- Sample record comparisons
- Schema documentation files
- Recommendations for resolution

## Implementation Phases

### Phase 1: Schema Discovery
**Goal**: Understand the database structures before building validation logic

**Deliverables**:
- Schema dump script (`schema_dump.py`)
- Connect to both Moneyball PostgreSQL and Snowflake
- Export DDL for target tables (companies, transactions, funds, notes, users)
- Generate readable schema documentation files

**Success Criteria**:
- Successfully connect to both databases
- Extract complete schema definitions
- Identify column names, types, and constraints for validation planning

### Phase 2: Schema Analysis & PRD Refinement
**Goal**: Update PRD with actual schema knowledge to inform validation implementation

**Activities**:
- Analyze exported schemas
- Identify column mappings between Postgres and Snowflake
- Refine validation rules based on actual table structures
- Update configuration templates with real column names
- Define specific timestamp columns for lag analysis

**Deliverables**:
- Updated PRD with schema-specific validation rules
- Refined configuration templates
- Column mapping documentation

### Phase 3: Validation Implementation
**Goal**: Build comprehensive consistency checking system

**Deliverables**:
- Main consistency check script (`consistency_check.py`)
- Configuration management system
- Markdown report generation
- All validation logic from Phase 2 requirements

**Success Criteria**:
- Complete end-to-end validation pipeline
- Accurate lag and count measurements
- Entity-specific analysis (companies, funds)
- Comprehensive reporting

### 7. Operational Requirements

#### 7.1 Execution Environment
- Python 3.8+ with required dependencies
- Database connection via environment variables using dotenv
- Required environment variables:
  - `MONEYBALL_DATABASE_URL`: PostgreSQL connection URL
  - `SNOWFLAKE_DATABASE_URL`: Snowflake connection URL
- Configuration stored in `.env` file

