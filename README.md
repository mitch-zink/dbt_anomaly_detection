# dbt Anomaly Detection

A dbt package for intelligent data quality monitoring using statistical anomaly detection with change-based algorithms.

> **🔷 Snowflake Only**: This package uses Snowflake-specific features (`information_schema`, `generator()`, date functions)

> **Design Philosophy**: Built to prevent alert fatigue. Default sensitivity (`very_low`) uses wide statistical bounds combined with materiality filters. Volume detection uses 20σ/16σ for change/count-based detection, freshness uses 10σ. Detects anomalies in **growth patterns**, not just absolute values.

## Features

### ✅ Volume Anomaly Detection (Dual Detection)
- **Change-based detection** - Monitors growth rate (row_count_change):
  - 📈 **Row Count Change - Spike** - Growth rate exceeded upper threshold (change > mean + σ × stddev) AND passes materiality filter
  - 📉 **Row Count Change - Dip** - Growth rate fell below lower threshold (change < mean - σ × stddev) AND passes materiality filter
- **Absolute row count detection** - Monitors total table size:
  - **Row Count - Dip** - Absolute row count below expected range (count < mean - σ × stddev) AND passes materiality filter
  - **Row Count - Spike** - Absolute row count above expected range (count > mean + σ × stddev) AND passes materiality filter
- **Enterprise-grade materiality filters** - Prevents statistically significant but meaningless alerts:
  - **Change-based**: Requires >1,000 rows deviation AND >200% relative change
  - **Count-based**: Requires >100 rows deviation AND >5% relative change
- **Fully dynamic & frequency-agnostic** - Works for daily, hourly, weekly, or any load frequency
- **Configurable sensitivity levels** (very_low/low/medium/high/very_high) using sigma multipliers
- **Dual-mode operation**: Metadata snapshots OR custom timestamp column (instant 30-day backfill)
- **Granular detection flags** - 4 boolean flags + 3 reason columns (row_count_change_anomaly_reason, row_count_anomaly_reason, anomaly_reason)

### ✅ Freshness Anomaly Detection
- **Data staleness monitoring** with statistical analysis
- **Dual-mode operation**: LAST_ALTERED OR custom timestamp column (instant backfill)
- **Rolling window statistics** (28-day baseline by default)
- **Intelligent alert filtering** - Prevents false positives with minimum thresholds:
  - Requires minimum staleness (6 hours by default) to filter out normal refresh lag
  - Requires minimum variation (1 hour stddev) to detect meaningful changes
  - Ignores tables with perfectly consistent updates (stddev = 0)
- **Configurable sensitivity** using sigma multipliers
- **Frequency-agnostic** - Reads all historical snapshot records regardless of run frequency
- **Table lifecycle tracking** - Monitors table_created_at to detect CREATE OR REPLACE operations

---

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/YOUR_ORG/dbt_anomaly_detection.git"
    revision: 0.2.0  # Replace with latest release tag
```

Or for local development:
```yaml
packages:
  - local: ../dbt_anomaly_detection
```

Then run:
```bash
dbt deps
```

---

## Quick Start

### Step 1: Install the Package

Add to your `packages.yml` and run `dbt deps`:

```yaml
packages:
  - local: ../dbt_anomaly_detection  # Or use git URL
```

### Step 2: Build Package Models

Build the entire package with a single command:

```bash
dbt build --select dbt_anomaly_detection

# Full refresh metrics during development/tuning (snapshot remains protected)
dbt build --select dbt_anomaly_detection --full-refresh
```

**🔒 Snapshot Protection:**
The snapshot (`snap_monitored_table_metadata`) ignores `--full-refresh` by default to preserve Type 2 SCD history. Metrics models (`volume_metrics_history`, `freshness_metrics_history`) support full-refresh for iterative development and sensitivity tuning.

To force snapshot refresh (rare, loses all history):
```bash
dbt snapshot --select snap_monitored_table_metadata --vars '{allow_full_refresh_anomaly_detection: true}'
```

**What happens:**
1. `stg_monitored_tables` scans your project for **models AND sources** with `volume_anomaly` or `freshness_anomaly` tests (supports cross-database monitoring)
2. `snap_monitored_table_metadata` captures metadata from `INFORMATION_SCHEMA.TABLES` across all databases for enrolled tables only
3. Metrics models calculate rolling statistics and detect anomalies

### Step 3: Enroll Tables for Monitoring

The package provides generic tests `volume_anomaly` and `freshness_anomaly` that you can apply to **both models AND sources** (cross-database monitoring supported).

**Configure tests in your `schema.yml`:**

```yaml
# Monitor dbt models
models:
  - name: fct_events
    tests:
      # Volume monitoring - metadata mode
      - volume_anomaly:
          sensitivity: very_low

      # Freshness monitoring - metadata mode
      - freshness_anomaly:
          sensitivity: very_low

  - name: fct_orders
    tests:
      # Volume monitoring - timestamp mode (instant 30-day backfill)
      - volume_anomaly:
          sensitivity: medium
          timestamp_column: order_date

      # Freshness monitoring - timestamp mode
      - freshness_anomaly:
          sensitivity: medium
          timestamp_column: updated_at

# Monitor source tables (external databases)
sources:
  - name: fivetran_salesforce
    database: FIVETRAN_SALESFORCE_DB
    tables:
      - name: account
        tests:
          - volume_anomaly:
              sensitivity: very_low
          - freshness_anomaly:
              sensitivity: very_low
```

### Step 4: Query Anomalies

```sql
-- View volume anomalies
SELECT *
FROM {{ ref('volume_metrics_history') }}
WHERE is_anomaly = TRUE
ORDER BY snapshot_timestamp DESC;

-- View freshness anomalies
SELECT *
FROM {{ ref('freshness_metrics_history') }}
WHERE is_stale = TRUE
ORDER BY snapshot_timestamp DESC;
```

---

## How It Works

### Volume Anomaly Detection (Dual Detection Architecture)

The model performs **two types** of anomaly detection simultaneously:

#### 1. Change-Based Detection (Growth Rate)

Detects anomalies in the **rate of change** between observations:

```
Row Count Change = Current Row Count - Previous Row Count
```

**Algorithm:**
1. **Data Collection**:
   - **Metadata mode**: Snapshots `information_schema.tables.row_count` when model runs
   - **Timestamp mode**: Queries table using spine with cumulative counts
2. **Calculate Change**: `row_count_change` using `LAG()` window function
3. **Rolling Statistics**: Mean and stddev of change over 28 days (time-based window)
4. **Detection Rules** (fully dynamic, frequency-agnostic):
   - **Statistical check**: `row_count_change > (mean + σ × stddev)` OR `row_count_change < (mean - σ × stddev)`
   - **Materiality filter** (prevents noise): Requires >1,000 rows deviation AND >200% relative change
   - **Final flag**: Row Count Change - Spike or Row Count Change - Dip

**Why?** Adapts to any table size and load frequency while filtering out statistically significant but meaningless changes (e.g., +10 rows on a table that usually grows by 0).

#### 2. Absolute Row Count Detection

Detects anomalies in the **total table size**:

**Algorithm:**
1. **Rolling Statistics**: Calculate mean and stddev of absolute `row_count` over 28 days (time-based window)
2. **Detection Rules**:
   - **Statistical check**: `row_count < (expected_row_count_mean - σ × expected_row_count_stddev)` OR `row_count > (expected_row_count_mean + σ × expected_row_count_stddev)`
   - **Materiality filter** (prevents noise): Requires >100 rows deviation AND >5% relative change
   - **Final flag**: Row Count - Dip or Row Count - Spike

**Why?** Catches sudden drops (data loss) or unexpected bulk loads while ignoring small fluctuations.

#### Output Columns

```sql
-- Volume metrics history table (key columns)
database_name         | varchar  | Database name
table_name            | varchar  | Table name
snapshot_timestamp    | timestamp| When the snapshot was taken
row_count             | number   | Current row count
row_count_change      | number   | Change from previous snapshot
expected_change_mean  | number   | Expected change (28-day average)
expected_change_min   | number   | Lower threshold for change
expected_change_max   | number   | Upper threshold for change
expected_row_count_mean   | number | Expected absolute row count
expected_row_count_min    | number | Lower threshold for row count
expected_row_count_max    | number | Upper threshold for row count
is_spike_high         | boolean  | Change exceeded upper threshold AND passed materiality filter
is_drop_low           | boolean  | Change fell below lower threshold AND passed materiality filter
is_row_count_anomaly  | boolean  | Absolute count out of range AND passed materiality filter
is_anomaly            | boolean  | TRUE if ANY flag is true
row_count_change_anomaly_reason | varchar | Change-based: 'Row Count Change - Spike', 'Row Count Change - Dip'
row_count_anomaly_reason        | varchar | Size-based: 'Row Count - Spike', 'Row Count - Dip'
anomaly_reason        | varchar  | Combined: Shows all detected issues (comma-separated if multiple)
```

**Example:**

| table_name  | snapshot_timestamp   | row_count | row_count_change | expected_change_mean | expected_change_min | expected_change_max | anomaly_reason           |
|------------|---------------------|-----------|------------------|----------------------|---------------------|---------------------|--------------------------|
| fct_orders | 2025-10-30 14:00:00 | 6,027,281 | -15,000          | 2,629                | -3,800              | 9,000               | Row Count Change - Dip   |
| stg_events | 2025-10-30 16:00:00 | 4,136,322 | 125,000          | 4,296                | 500                 | 8,100               | Row Count Change - Spike |

**Dual-Source Architecture:**
- **Metadata mode**: Snapshot-based (depends on run frequency)
- **Timestamp mode**: Time-based with 30-day instant backfill

### Freshness Anomaly Detection

Monitors data **staleness** (time since last update):

**Algorithm:**
1. **Staleness Measurement**:
   - **Metadata mode**: Uses `information_schema.tables.LAST_ALTERED`
   - **Timestamp mode**: Uses `max(timestamp_column)` with forward-fill logic for time-based tracking
2. **Calculate Staleness**:
   - `hours_since_last_update = datediff('hour', table_last_altered_at, snapshot_timestamp)`
   - `minutes_since_last_update` also available for finer granularity
3. **Rolling Statistics**: Mean and stddev of staleness over 28 days (time-based window) - reads ALL historical snapshot records (frequency-agnostic)
4. **Detection Rule** (upper bound only with intelligent filtering):
   - **Statistical check**: `hours_since_last_update > (expected_staleness_mean + σ × expected_staleness_stddev)`
   - **Minimum staleness filter**: Requires `hours_since_last_update >= 6` hours (configurable via `freshness_anomaly_detection.min_staleness_hours`)
   - **Minimum variation filter**: Requires `expected_staleness_stddev >= 1.0` hour (configurable via `freshness_anomaly_detection.min_stddev_hours`)
   - **Zero variation filter**: Ignores tables with `stddev = 0` (perfectly consistent updates)
   - **Minimum observations**: Requires `observation_count >= 7` historical data points

**Why these filters?** Prevent false positives from normal daily refresh lag (1-2 hours) and tables with perfectly consistent update schedules.

**Why upper bound only?** Tables being "too fresh" isn't a problem - we only care if they're stale.

#### Output Columns

```sql
-- Freshness metrics history table (key columns)
database_name             | varchar   | Database name
table_name                | varchar   | Table name
snapshot_timestamp        | timestamp | When the snapshot was taken
table_created_at          | timestamp | When table was created (tracks CREATE OR REPLACE)
table_last_altered_at     | timestamp | When table was last updated
hours_since_last_update   | number    | Hours since last update (staleness)
minutes_since_last_update | number    | Minutes since last update
expected_staleness_mean   | number    | Expected staleness (28-day average)
expected_staleness_stddev | number    | Standard deviation of staleness
expected_min_hours        | number    | Lower threshold (mean - σ × stddev) - for reference only
expected_max_hours        | number    | Upper threshold (mean + σ × stddev)
observation_count         | number    | Historical observations used
is_stale                  | boolean   | TRUE if stale, FALSE otherwise
sensitivity               | varchar   | Sensitivity level (very_low/low/medium/high/very_high)
sigma_multiplier          | decimal   | Sigma multiplier used for threshold calculation
```

---

## Configuration

### Sensitivity Levels

Control how strict the anomaly detection is using sigma multipliers.

**Note:** Volume and Freshness use different sigma ranges because they measure different phenomena:
- **Volume** (row counts) has high variance → wider sigmas needed
- **Freshness** (staleness hours) has low variance → tighter sigmas appropriate

#### Volume Sensitivity (Row Count Detection)

**Note:** Volume detection uses two separate sigma settings (see dbt_project.yml):
- `row_count_change_sensitivity_*` - For growth rate anomalies
- `row_count_sensitivity_*` - For absolute row count anomalies

| Sensitivity  | Change σ | Count σ | Use Case |
|--------------|----------|---------|----------|
| `very_low`   | 20.0     | 16.0    | **Wide tolerance** - catches major anomalies (**RECOMMENDED for production**) |
| `low`        | 12.0     | 8.0     | Moderate tolerance - significant anomalies |
| `medium`     | 8.0      | 4.0     | Balanced sensitivity |
| `high`       | 6.0      | 2.0     | Tighter tolerance - more sensitive |
| `very_high`  | 5.0      | 1.0     | Narrow tolerance - most sensitive (will be noisy) |

#### Freshness Sensitivity (Staleness Detection)

| Sensitivity  | Sigma (σ) | Typical Threshold | Use Case |
|--------------|-----------|------------------|----------|
| `very_low`   | 10.0      | 3-7 days         | **Forgiving** - catches abandoned tables (**RECOMMENDED for production**) |
| `low`        | 6.0       | 2-4 days         | Moderate tolerance - tables stale days |
| `medium`     | 4.0       | 1-3 days         | Balanced sensitivity |
| `high`       | 2.5       | Hours to 1 day   | Strict - catches tables stale beyond normal pattern |
| `very_high`  | 1.5       | Hours            | Very strict - will generate noise from normal variance |

**How it works:**
- **Fully dynamic** - Adapts to any table size, growth rate, and load frequency (hourly, daily, weekly, etc.)
- **Statistical + Materiality** - Combines wide sigma bounds with materiality filters to eliminate noise
- **Consistent** - Same sensitivity levels work for both volume and freshness detection

**How Sensitivity Works:**

```
Expected Range = mean ± (sigma_multiplier × standard_deviation)

Example with medium sensitivity (8σ):
- Expected change: 3,300 rows/hour
- Standard deviation: 2,156 rows
- Lower threshold: 3,300 - (8.0 × 2,156) = -13,948 rows
- Upper threshold: 3,300 + (8.0 × 2,156) = 20,548 rows
- Statistical check: change < -13,948 OR change > 20,548
- Materiality check: |deviation| > 1,000 rows AND relative change > 200%
- Final anomaly: Both checks must pass
```

### Global Settings

Configure in `dbt_project.yml`:

```yaml
vars:
  # Volume Anomaly Detection
  volume_anomaly_detection:
    min_historical_observations: 7  # Minimum data points needed for baseline

  # Freshness Anomaly Detection
  freshness_anomaly_detection:
    min_historical_observations: 7    # Minimum data points needed for baseline
    min_staleness_hours: 6            # Minimum hours stale to trigger alert (filters normal refresh lag)
    min_stddev_hours: 1.0             # Minimum standard deviation to enable detection (filters perfectly consistent tables)

    # Optional: Override sigma multipliers (freshness detection defaults shown below)
    # sensitivity_very_low: 10.0      # Forgiving tolerance (default)
    # sensitivity_low: 6.0             # Moderate tolerance
    # sensitivity_medium: 4.0          # Balanced sensitivity
    # sensitivity_high: 2.5            # Tighter tolerance
    # sensitivity_very_high: 1.5       # Narrow tolerance
```

**Example: Stricter freshness detection for your project:**

```yaml
vars:
  freshness_anomaly_detection:
    sensitivity_very_low: 6.0         # Override default 10.0σ to be more strict
    sensitivity_low: 4.0              # Override default 6.0σ
    min_staleness_hours: 2            # Alert after just 2 hours stale
```

### Per-Model Configuration

```yaml
models:
  # Critical revenue table - medium sensitivity, custom timestamp
  - name: fct_orders
    tests:
      - volume_anomaly:
          timestamp_column: order_date
          sensitivity: medium  # 2σ threshold

      - freshness_anomaly:
          timestamp_column: updated_at
          sensitivity: medium

  # Event stream - medium sensitivity, metadata mode
  - name: stg_events
    tests:
      - volume_anomaly:
          sensitivity: medium  # Catches growth anomalies
```

---

## Detection Flags

### Volume Anomaly Flags

Each volume anomaly record includes **4 boolean flags**:

- `is_spike_high` - Growth rate exceeded upper threshold (change > mean + σ × stddev)
- `is_drop_low` - Growth rate fell below lower threshold (change < mean - σ × stddev)
- `is_row_count_anomaly` - Absolute row count out of expected range (size-based)
- `is_anomaly` - **Master flag**: TRUE if ANY of the above is true

**Human-readable column:**
- `anomaly_reason` - Text: 'Unusual Spike', 'Unusual Drop', 'Row Count Too Low', 'Row Count Too High', or NULL

**Query Examples:**

```sql
-- Filter to change-based anomalies (growth rate issues)
WHERE is_spike_high = true OR is_drop_low = true

-- Filter to absolute row count issues (size issues)
WHERE is_row_count_anomaly = true

-- Group by anomaly type
GROUP BY anomaly_reason

-- Count anomalies by type
SELECT anomaly_reason, COUNT(*)
FROM {{ ref('volume_metrics_history') }}
WHERE is_anomaly = TRUE
GROUP BY 1
```

### Freshness Anomaly Flag

- `is_stale` - TRUE if `hours_since_last_update > expected_max_hours`

---

## Running the Package

### Build Package Models

```bash
# Recommended: Use tag selector (builds in correct order)
dbt build --select tag:anomaly_detection  # Incremental updates
dbt build --select tag:anomaly_detection --full-refresh  # Rebuild metrics (snapshot protected)

# Alternative: Build manually in dependency order
dbt run --select stg_monitored_tables                    # 1. Enrollment registry
dbt build --select snap_monitored_table_metadata         # 2. Metadata snapshot
dbt build --select volume_metrics_history freshness_metrics_history  # 3. Metrics
```

**🔒 Snapshot Protection:**
The snapshot preserves Type 2 SCD history and ignores `--full-refresh`. Metrics models can be freely refreshed during development/tuning.

The package will:

1. Extract enrolled tables from test configuration (`stg_monitored_tables`)
2. Snapshot metadata from `INFORMATION_SCHEMA.TABLES` for enrolled tables only (`snap_monitored_table_metadata`)
3. Build incremental metrics history tables with 30-day rolling windows
4. Calculate statistical baselines and detect anomalies
5. Store results in `volume_metrics_history` and `freshness_metrics_history`

### Query Results

```sql
-- View all volume anomalies with change-based metrics
SELECT
  database_name,
  schema_name,
  table_name,
  snapshot_timestamp,
  row_count,
  row_count_change,
  expected_change_mean,
  expected_change_min,
  expected_change_max,
  expected_row_count_mean,
  expected_row_count_min,
  expected_row_count_max,
  is_growth_stalled,
  is_spike_high,
  is_drop_low,
  is_row_count_anomaly,
  anomaly_reason,
  sensitivity,
  sigma_multiplier
FROM {{ ref('volume_metrics_history') }}
WHERE is_anomaly = TRUE
ORDER BY snapshot_timestamp DESC;

-- View freshness anomalies
SELECT
  database_name,
  schema_name,
  table_name,
  snapshot_timestamp,
  table_last_altered_at,
  hours_since_last_update,
  minutes_since_last_update,
  expected_staleness_mean,
  expected_staleness_stddev,
  expected_max_hours,
  observation_count,
  sensitivity,
  sigma_multiplier
FROM {{ ref('freshness_metrics_history') }}
WHERE is_stale = TRUE
ORDER BY snapshot_timestamp DESC;
```

---

## Architecture

### Data Flow

```text
1. Test Configuration (schema.yml in your project)
   └─> volume_anomaly / freshness_anomaly tests
        │
        ↓
2. Enrollment Registry
   └─> stg_monitored_tables (extracts tables with anomaly tests)
        │
        ↓
3. Metadata Collection
   └─> snap_monitored_table_metadata (snapshot of INFORMATION_SCHEMA)
        │
        ↓
4. Metrics & Detection
   └─> volume_metrics_history / freshness_metrics_history
```

### Project Structure

```text
dbt_anomaly_detection/
├── models/
│   ├── staging/
│   │   ├── stg_monitored_tables.sql           # Enrollment registry
│   │   ├── schema.yml                         # Model documentation & tests
│   │   └── _sources.yml                       # Source definitions
│   ├── volume/
│   │   ├── volume_metrics_history.sql         # Change-based volume tracking
│   │   └── schema.yml                         # Model documentation & tests
│   └── freshness/
│       ├── freshness_metrics_history.sql      # Staleness tracking
│       └── schema.yml                         # Model documentation & tests
├── snapshots/
│   ├── snap_monitored_table_metadata.sql      # Metadata snapshot
│   └── schema.yml                             # Snapshot documentation & tests
├── macros/
│   ├── volume_anomaly.sql                     # Generic test macro
│   ├── freshness_anomaly.sql                  # Generic test macro
│   └── detect_change_anomaly.sql              # Detection logic macros
│       ├── is_growth_stalled()                # Detects stalled growth
│       ├── is_spike_high()                    # Detects unusual spikes
│       └── is_drop_low()                      # Detects unusual drops
└── packages.yml                               # Dependencies (dbt_utils)
```

### How Enrollment Works

1. **Apply tests** to models OR sources in your project's `schema.yml`:
   ```yaml
   # For dbt models
   models:
     - name: fct_orders
       tests:
         - volume_anomaly:
             sensitivity: very_low

   # For source tables (cross-database)
   sources:
     - name: fivetran_salesforce
       database: FIVETRAN_SALESFORCE_DB
       tables:
         - name: account
           tests:
             - volume_anomaly:
                 sensitivity: very_low
   ```

2. **`stg_monitored_tables`** scans both `graph.nodes` (models) AND `graph.sources` (source tables) at compile time to find all tables with `volume_anomaly` or `freshness_anomaly` tests

3. **`snap_monitored_table_metadata`** dynamically queries `INFORMATION_SCHEMA.TABLES` from each database for tables in `stg_monitored_tables` (cross-database support)

4. **Metrics models** join to the snapshot to calculate anomalies

**Cross-Database Support:** Source tables from external databases (e.g., FIVETRAN_SALESFORCE_DB, FIVETRAN_HEAP_DB) are fully supported. The snapshot queries each database's INFORMATION_SCHEMA to retrieve metadata.

**Note:** Only BASE TABLEs are tracked. VIEWs are excluded because they don't have row_count or LAST_ALTERED metadata.

## Troubleshooting

### Snapshot not refreshing with --full-refresh?

**Q:** I ran `dbt build --full-refresh` but the snapshot didn't rebuild.

**A:** This is intentional! The snapshot preserves Type 2 SCD history (dbt_valid_from/to) which would be lost on full-refresh.

**Solution (rare):** Override only if you need to rebuild snapshot from scratch:
```bash
dbt snapshot --select snap_monitored_table_metadata --vars '{allow_full_refresh_anomaly_detection: true}'
```

**Why this protection exists:**
- Snapshot tracks historical changes over time (Type 2 SCD)
- Metrics models can be rebuilt from snapshot history without data loss
- Full-refresh of snapshot would lose all temporal tracking

**Note:** Metrics models (`volume_metrics_history`, `freshness_metrics_history`) DO support `--full-refresh` for iterative development.

### Table enrolled but not in snapshot?

**Q:** I added `volume_anomaly` test to a model, but it's not showing up in `snap_monitored_table_metadata`.

**A:** Check if the model is a VIEW. The snapshot only tracks BASE TABLEs because:
- VIEWs don't have `row_count` in `INFORMATION_SCHEMA.TABLES`
- VIEWs don't have meaningful `LAST_ALTERED` timestamps
- Volume/freshness detection doesn't make sense for views

```sql
-- Check table type
SELECT table_type
FROM information_schema.tables
WHERE table_name = 'YOUR_MODEL_NAME';
```

**Solution:** Anomaly detection only works on materialized models (table, incremental, snapshot). Change your view to a table:
```yaml
models:
  - name: my_model
    config:
      materialized: table  # or incremental
```

### Enrollment count doesn't match snapshot count?

This is expected! `stg_monitored_tables` includes both BASE TABLEs and VIEWs, but `snap_monitored_table_metadata` only captures BASE TABLEs.

```sql
-- Compare counts
SELECT 'Enrolled' as source, COUNT(*) as count
FROM {{ ref('stg_monitored_tables') }}
UNION ALL
SELECT 'Snapshot' as source, COUNT(*)
FROM {{ ref('snap_monitored_table_metadata') }}
WHERE dbt_valid_to IS NULL;
```

### Build fails with "table is undefined"?

**A:** Build order issue. `snap_monitored_table_metadata` depends on `stg_monitored_tables`.

**Solution:** Build in order:
```bash
dbt run --select stg_monitored_tables
dbt build --select snap_monitored_table_metadata
```

Or use tag selector: `dbt build --select tag:anomaly_detection`

---

## Requirements

- **dbt Core**: 1.3.0+ (tested with 1.10.x)
- **Database**: Snowflake only (uses `information_schema`, `generator()`, `LAG()`, date functions)
- **Permissions**: Read access to `information_schema.tables` in target database
- **Dependencies**: `dbt_utils` (automatically installed via `packages.yml`)
