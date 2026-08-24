# dbt Anomaly Detection

A dbt package for data-quality monitoring on **Snowflake**. Enroll a table or source
by adding a generic test; the package builds its own per-table history and flags two
kinds of problems using robust, self-calibrating statistical baselines:

- **Volume anomalies** — the number of new rows in a period is unusual for the table.
- **Freshness anomalies** — the table hasn't been updated within its own normal cadence.

> **🔷 Snowflake only.** Uses `INFORMATION_SCHEMA`, `generator()`, and Snowflake date
> functions.

> **📎 About this document.** This describes the **robust prep/final architecture**
> (median/MAD for volume, percentile "normal-gap" for freshness). It assumes the legacy
> single-model implementation (`volume_metrics_history`, `freshness_metrics_history` and
> their sigma/materiality logic) has been **removed**. If those models are still present,
> both approaches run side-by-side and this document describes only the new one.

---

## Design principles

- **Robust, not Gaussian.** Volume uses the **median** and **MAD** (median absolute
  deviation) instead of mean/stddev, so a single huge spike can't inflate the baseline
  and hide the very anomaly you want to catch. Freshness uses a **percentile** of the
  observed update gaps rather than a mean ± sigma band.
- **Prior-only baselines.** A row is never part of the baseline it is judged against, so
  the current value can't "explain away" its own anomaly.
- **Per-table & self-calibrating.** Each monitored series gets its own baseline; there
  are no global thresholds to hand-tune. Sensitivity is a single per-table dial.
- **Frequency-agnostic.** Works whether a table loads hourly, daily, or irregularly.
- **Warm-up aware.** Tables without enough history are held in a *training* state rather
  than alerting on noise.

---

## What each detector produces

| Detector | Final model | Grain | Verdict column |
|---|---|---|---|
| Volume | `volume_metrics_history_final` | one row per (object, snapshot) | `is_row_count_change_anomaly` |
| Freshness | `freshness_metrics_history_final` | one row per (object, snapshot); the latest per object (`snapshot_order = 1`) is the current-state verdict | `is_stale` / `is_training` |

An **object** is a monitored series, keyed by `object_id = hash(full_table_name,
source_type, timestamp_column)`. A **metric** is one snapshot of an object, keyed by
`metric_id = hash(full_table_name, snapshot_timestamp, source_type, timestamp_column)`.

---

## Architecture & data flow

```text
1. Enrollment (your project's schema.yml)
   volume_anomaly / freshness_anomaly generic tests on models & sources
        │
        ▼
2. Enrollment registry
   stg_monitored_tables            — scans graph.nodes + graph.sources at compile time
        │
        ▼
3. Metadata collection
   snap_monitored_table_metadata   — Type-2 snapshot of INFORMATION_SCHEMA.TABLES
        │                            (row_count, last_altered, created) for enrolled tables
        │
        ├───────────────► stg_volumn_metric_custom_table   (custom timestamp_column, view)
        │                 stg_freshness_metric_custom_table (custom timestamp_column, incremental)
        ▼
4. Per-snapshot history (prep)
   volume_metrics_history_prep       freshness_metrics_history_prep
        │                                 │
        ▼                                 ▼
5. Scoring (final)
   volume_metrics_history_final      freshness_metrics_history_final
```

Both detectors are **dual-source**:

- **Metadata mode** (default) — reads `row_count` / `last_altered` from the
  `INFORMATION_SCHEMA` snapshot. No configuration beyond the test.
- **Timestamp mode** — set `timestamp_column` on the test. The package reads the table's
  own event-time column, which gives an **instant historical backfill** over the training
  window instead of waiting for snapshots to accumulate.

### Project structure

```text
dbt_anomaly_detection/
├── models/
│   ├── staging/
│   │   ├── stg_monitored_tables.sql             # enrollment registry
│   │   ├── stg_volumn_metric_custom_table.sql   # custom-timestamp volume series (view)
│   │   ├── stg_freshness_metric_custom_table.sql# custom-timestamp freshness series (incremental)
│   │   └── schema.yml
│   ├── volume/
│   │   ├── volume_metrics_history_prep.sql       # per-snapshot row-count history
│   │   ├── volume_metrics_history_final.sql      # median/MAD scoring
│   │   └── schema.yml
│   └── freshness/
│       ├── freshness_metrics_history_prep.sql    # per-snapshot last-updated history
│       ├── freshness_metrics_history_final.sql   # percentile normal-gap scoring
│       └── schema.yml
├── snapshots/
│   └── snap_monitored_table_metadata.sql
└── macros/
    ├── volume_anomaly.sql            # generic test (config marker)
    ├── freshness_anomaly.sql         # generic test (config marker)
    └── get_enrolled_tables.sql       # shared graph-scanning macro
```

---

## Enrolling tables

Apply the generic tests to **models or sources** (cross-database sources supported):

```yaml
models:
  - name: fct_orders
    tests:
      # Volume, timestamp mode (instant backfill from the table's own column)
      - volume_anomaly:
          sensitivity: medium
          timestamp_column: order_date
          training_period_days: 90
      # Freshness, timestamp mode
      - freshness_anomaly:
          sensitivity: medium
          timestamp_column: updated_at
          timezone: America/Los_Angeles

  - name: fct_events
    tests:
      - volume_anomaly:        # metadata mode — no timestamp_column
          sensitivity: very_low
      - freshness_anomaly:
          sensitivity: very_low

sources:
  - name: fivetran_salesforce
    database: FIVETRAN_SALESFORCE_DB
    tables:
      - name: account
        tests:
          - volume_anomaly: {sensitivity: very_low}
          - freshness_anomaly: {sensitivity: very_low}
```

The generic tests are **configuration markers** — they always pass. All detection happens
in the models, which read the test kwargs from the dbt graph via `get_enrolled_tables()`.

### Test arguments

| Argument | Applies to | Default | Meaning |
|---|---|---|---|
| `sensitivity` | both | `very_low` (volume), `medium` (freshness) | Per-table dial (see below). |
| `timestamp_column` | both | *none* → metadata mode | Event-time column to use instead of `INFORMATION_SCHEMA`. |
| `training_period_days` | both | `90` (volume), `30` (freshness) | Lookback window for the baseline. |
| `timezone` | freshness | `America/Los_Angeles` | Timezone the `timestamp_column` is stored in; normalized before measuring staleness. Only relevant in timestamp mode. |

---

## How volume detection works (median / MAD)

**`volume_metrics_history_prep`** builds one row per (object, snapshot) by unioning the
custom-timestamp series with the `INFORMATION_SCHEMA` snapshot, then derives:

```
row_count_change = row_count − row_count of the object's previous snapshot   (LAG)
```

`row_count_change` is `NULL` for an object's first snapshot, and `0` is converted to
`NULL` so flat/no-load snapshots don't distort the baseline.

**`volume_metrics_history_final`** scores each row against a **prior-only** baseline:

1. **K-nearest sampling.** From the object's *earlier* snapshots, keep the
   `med_sample_size` (default **20**) whose `row_count_change` is closest to the current
   row's change. The baseline reflects "what a normal load of roughly this size looks
   like," so rare, very different regimes don't skew it.
2. **MED / MAD.** `MED` = median of that pool. `MAD` = `median(|value − MED|)`, used
   directly as the spread unit (no 1.4826 scaling), so the multiplier is literally *how
   many MADs wide* the band is.
3. **MAD floor.** For tables that barely change, MAD can collapse to 0. It is floored to
   the first non-zero of `median|dev|` → `p75|dev|` → `p90|dev|`, and never below
   `MED × mad_floor` (default **0.2**), so the band is never zero-width.
4. **Band & flag.**
   ```
   lower = MED − multiplier × MAD
   upper = MED + multiplier × MAD
   is_row_count_change_anomaly = row_count_change outside [lower, upper]
       AND row_count_change is not null
       AND prior_obs_count > min_historical_observations   (default 7)
   ```

Sensitivity maps to the **MAD multiplier** (a wider band = less sensitive):

| Sensitivity | MAD multiplier | Behavior |
|---|---|---|
| `very_low`  | 7.0 | Widest band — only extreme changes (**recommended default**) |
| `low`       | 6.0 | |
| `medium`    | 5.0 | |
| `high`      | 4.0 | |
| `very_high` | 3.0 | Narrowest band — most sensitive (noisiest) |

**Diagnostics** carried on every row: `row_count_change_med`, `row_count_change_mad`,
`row_count_change_lower_limit`, `row_count_change_upper_limit`,
`row_count_change_modified_z_score` (distance from MED in MAD units), and
`row_count_change_similar_z_score_count` (how many prior snapshots of the same object had
a z-score within ±20% of this row's — i.e. how routine this magnitude of deviation is).

---

## How freshness detection works (percentile "normal gap")

**`freshness_metrics_history_prep`** builds one row per (object, snapshot) recording when
each table was last updated (`table_last_altered_at`) — event-time `MAX(timestamp_column)`
for timestamp mode, `LAST_ALTERED` (capped at snapshot time) for metadata mode.

**`freshness_metrics_history_final`** turns that history into a staleness verdict:

1. **Training window.** Keep snapshots newer than `training_period_days`, read from the
   object's most recent snapshot so a config change takes effect immediately.
2. **Update gaps.** `minutes_between_updates` = time between consecutive *distinct*
   `table_last_altered_at` values. These are **adjusted** to `NULL` when they aren't a
   real cadence signal: snapshots more than 24h apart, gaps under 30 minutes, or
   re-observations of an already-counted update (`update_observation_seq > 1`).
3. **Normal gap.** `normal_gap_upper` = `percentile_cont(sensitivity_pct)` of the adjusted
   gaps, per object — the upper edge of how long this table normally goes between updates.
4. **Verdict.**
   ```
   minutes_since_last_update = now(snapshot) − table_last_altered_at

   is_stale = minutes_since_last_update > normal_gap_upper
       AND normal_gap_upper is not null                       -- has a baseline
       AND observation_day_count >= min_historical_observation_days  (default 7)
       AND minutes_since_last_update >= min_staleness_minutes        (default 120)

   is_training = no baseline yet OR fewer than min_historical_observation_days
                 distinct days of history        -- mutually exclusive with is_stale
   ```

Sensitivity maps to the **percentile** (a higher percentile = rarer alerts):

| Sensitivity | Percentile | Behavior |
|---|---|---|
| `very_low`  | 0.95 | Flags only when well past normal (**recommended default**) |
| `low`       | 0.92 | |
| `medium`    | 0.90 | |
| `high`      | 0.85 | |
| `very_high` | 0.80 | Most sensitive (noisiest) |

**Grain note.** The model emits every snapshot in the training window. The **latest
snapshot per object** carries `snapshot_order = 1` and is the current-state verdict that
downstream alerting reads. `observation_day_count` counts *distinct calendar days* with a
usable gap sample (not raw sample count), because sub-daily snapshots would otherwise make
a single busy afternoon look like a week of history.

---

## Querying results

```sql
-- Volume anomalies
select database_name, table_name, snapshot_timestamp, row_count, row_count_change,
       row_count_change_lower_limit, row_count_change_upper_limit,
       row_count_change_modified_z_score
from {{ ref('volume_metrics_history_final') }}
where is_row_count_change_anomaly
order by snapshot_timestamp desc;

-- Freshness: current stale tables
select full_table_name, sensitivity, minutes_since_last_update, normal_gap_upper
from {{ ref('freshness_metrics_history_final') }}
where snapshot_order = 1 and is_stale
order by minutes_since_last_update desc;

-- Freshness: tables still warming up (not yet alertable)
select full_table_name, observation_day_count
from {{ ref('freshness_metrics_history_final') }}
where snapshot_order = 1 and is_training;
```

---

## Configuration (`dbt_project.yml`)

```yaml
vars:
  volume_anomaly_detection:
    min_historical_observations: 7   # prior samples required before a row can be flagged
    med_sample_size: 20              # K-nearest prior samples used for MED/MAD
    mad_floor: 0.2                   # MAD never below MED × this (guards zero-width bands)
    # MAD multipliers by sensitivity (defaults 7/6/5/4/3):
    # row_count_change_sensitivity_very_low: 7.0
    # row_count_change_sensitivity_low: 6.0
    # row_count_change_sensitivity_medium: 5.0
    # row_count_change_sensitivity_high: 4.0
    # row_count_change_sensitivity_very_high: 3.0

  freshness_anomaly_detection:
    min_historical_observation_days: 7   # distinct days of gap history before alerting
    min_staleness_minutes: 120           # absolute floor — ignore anything fresher than this
```

> The freshness **percentiles** (0.95 … 0.80) are currently defined in
> `freshness_metrics_history_final.sql`, not as vars. Move them to `vars` if you want them
> overridable without editing the model.

---

## Building & running

```bash
# Build the whole package in dependency order
dbt build --select tag:anomaly_detection

# Manual order if needed
dbt run   --select stg_monitored_tables
dbt build --select snap_monitored_table_metadata
dbt build --select stg_volumn_metric_custom_table stg_freshness_metric_custom_table
dbt build --select volume_metrics_history_prep  volume_metrics_history_final
dbt build --select freshness_metrics_history_prep freshness_metrics_history_final
```

**Materializations & refresh:**

- `snap_monitored_table_metadata` — Type-2 snapshot; it is the historical collection
  layer. Do not routinely full-refresh it (that discards SCD history).
- `freshness_metrics_history_prep` — **incremental**; full-refresh is gated behind
  `--vars '{allow_full_refresh_anomaly_detection: true}'` because sub-daily custom-timestamp
  readings are point-in-time and **cannot be reconstructed** after the fact.
- `stg_freshness_metric_custom_table` — incremental, full-refresh disabled, same reason.
- `volume_metrics_history_prep` / `_final` and `volume_metrics_history_final` are plain
  tables (row counts are retroactively derivable, so they rebuild safely).
- `stg_volumn_metric_custom_table` is a view.

---

## Notes & considerations

- **Warm-up / cold start.** New objects won't alert until they have a baseline: volume
  needs more than `min_historical_observations` prior samples; freshness needs
  `min_historical_observation_days` distinct days and surfaces meanwhile via `is_training`.
- **Timestamp vs metadata mode.** Timestamp mode backfills history instantly from the
  table's own column and is immune to run-frequency gaps; metadata mode requires no config
  but only accumulates history as snapshots run. Switching a table's `timestamp_column`
  changes its `object_id`, so it starts a **fresh baseline** rather than mixing regimes.
- **Cross-database & table type.** Sources in other databases are fully supported (each
  database's `INFORMATION_SCHEMA` is queried). Only **BASE TABLEs** are tracked — views
  have no `row_count` or meaningful `LAST_ALTERED`, so `stg_monitored_tables` may list more
  tables than the snapshot captures. That count mismatch is expected.
- **No self-monitoring.** The package's own metrics models are excluded from enrollment so
  detection can't become self-referential.
- **Staleness floor.** `min_staleness_minutes` (default 2h) suppresses alerts on chatty
  tables whose "normal gap" is very short; raise it for tables with noisy refresh lag.
- **MAD floor & burst loads.** Tables that load in bursts (e.g. an hourly snapshot of a
  once-a-day table) would otherwise get a zero-width band; the MAD floor keeps the band
  usable. Tune `mad_floor` if such tables over- or under-alert.

---

## Requirements

- **dbt Core** 1.3.0+ (developed against 1.10.x)
- **Database:** Snowflake only
- **Permissions:** read access to `INFORMATION_SCHEMA.TABLES` in each monitored database
- **Dependencies:** `dbt_utils` (via `packages.yml`)
