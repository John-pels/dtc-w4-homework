# Module 4 Homework: Analytics Engineering (dbt)

In this homework, we use the dbt project `taxi_rides_ny` to transform NYC taxi data and answer questions by querying the models.

## Setup

1. **Set up dbt project** following the [setup guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering/setup)

2. **Load data** into your BigQuery warehouse:
   - Green taxi data for 2019-2020
   - Yellow taxi data for 2019-2020
   - FHV trip data for 2019 (for Question 6)

3. **Build dbt models**:
   ```bash
   dbt build --target prod
   ```

4. After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

---

## Question 1: dbt Lineage and Execution

**Given a dbt project with the following structure:**
```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

**If you run `dbt run --select int_trips_unioned`, what models will be built?**

### Answer: **C. int_trips_unioned only**

### Explanation:
When you run `dbt run --select <model_name>`, dbt only builds the specified model. It does **not** automatically build upstream or downstream dependencies unless you explicitly use the `+` operator:

- `dbt run --select +int_trips_unioned` — builds **upstream** dependencies + the model
- `dbt run --select int_trips_unioned+` — builds the model + **downstream** dependencies
- `dbt run --select +int_trips_unioned+` — builds both upstream and downstream

Without the `+` operator, only `int_trips_unioned` itself is built.

---

## Question 2: dbt Tests

**You've configured a generic test like this in `schema.yml`:**
```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

**Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data. What happens when you run `dbt test --select fct_trips`?**

### Answer: **B. dbt will fail the test, returning a non-zero exit code**

### Explanation:
The `accepted_values` test checks that all values in the specified column are within the defined list `[1, 2, 3, 4, 5]`. When value `6` appears:

- dbt **does not** skip tests because the model didn't change — tests always re-run against current data
- dbt **does not** automatically update the configuration — tests are static definitions
- dbt **does not** just warn — the default severity for `accepted_values` is `error`
- dbt **will fail** the test and return a non-zero exit code because `6` is not in the accepted values list

---

## Question 3: Counting Records in `fct_monthly_zone_revenue`

**After running your dbt project, query the `fct_monthly_zone_revenue` model.**

### SQL Query:
```sql
-- File: queries/q3_fct_monthly_zone_revenue_count.sql
SELECT COUNT(*) AS record_count
FROM `dtc-w4-bquery.fct_monthly_zone_revenue`;
```

### Answer: **A. 12,998**

### Explanation:
After running `dbt build --target prod` with the Green and Yellow taxi data for 2019-2020 loaded, the `fct_monthly_zone_revenue` model aggregates trip data by month and zone. The total record count is **12,998**.

---

## Question 4: Best Performing Zone for Green Taxis (2020)

**Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue (`revenue_monthly_total_amount`) for Green taxi trips in 2020.**

### SQL Query:
```sql
-- File: queries/q4_best_performing_zone_green_2020.sql
SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue
FROM `dtc-w4-bquery.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

### Answer: **A. East Harlem North**

### Explanation:
Among all pickup zones for Green taxi trips in 2020, **East Harlem North** had the highest total revenue.

---

## Question 5: Green Taxi Trip Counts (October 2019)

**Using the `fct_monthly_zone_revenue` table, what is the total number of trips (`total_monthly_trips`) for Green taxis in October 2019?**

### SQL Query:
```sql
-- File: queries/q5_green_taxi_trips_oct_2019.sql
SELECT
    SUM(total_monthly_trips) AS total_trips
FROM `dtc-w4-bquery.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND revenue_month = '2019-10-01';
```

### Answer: **C. 384,624**

### Explanation:
The total number of Green taxi trips across all zones in October 2019 is **384,624**.

---

## Question 6: Build a Staging Model for FHV Data

**Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019.**

### Prerequisites:
1. Load FHV trip data from [GitHub releases](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) for 2019 into BigQuery
2. Create the staging model `stg_fhv_tripdata.sql`

### Staging Model:
The model file is located at [`models/staging/stg_fhv_tripdata.sql`](models/staging/stg_fhv_tripdata.sql).

Key transformations:
- **Filter**: Exclude records where `dispatching_base_num IS NULL`
- **Rename**: `PUlocationID` → `pickup_location_id`, `DOlocationID` → `dropoff_location_id`, etc.

### SQL Query (to verify count):
```sql
-- File: queries/q6_stg_fhv_count.sql
SELECT COUNT(*) AS record_count
FROM `dtc-w4-bquery.stg_fhv_tripdata`;
```

### Answer: **B. 43,244,693**

### Explanation:
After filtering out records where `dispatching_base_num IS NULL`, the `stg_fhv_tripdata` model contains **43,244,693** records from the 2019 FHV trip data.

---

## Summary of Answers

| Question | Answer |
|----------|--------|
| Q1: dbt Lineage and Execution | **C.** `int_trips_unioned` only |
| Q2: dbt Tests | **B.** dbt will fail the test, returning a non-zero exit code |
| Q3: Records in `fct_monthly_zone_revenue` | **A.** 12,998 |
| Q4: Best Zone for Green Taxis 2020 | **A.** East Harlem North |
| Q5: Green Taxi Trips Oct 2019 | **C.** 384,624 |
| Q6: Records in `stg_fhv_tripdata` | **B.** 43,244,693 |

---

## File Structure

```
w4-homework/
├── README.md                    # This file - solutions and explanations
├── .gitignore                   # Git ignore rules
├── models/
│   └── staging/
│       └── stg_fhv_tripdata.sql # FHV staging model (Q6)
└── queries/
    ├── q3_fct_monthly_zone_revenue_count.sql
    ├── q4_best_performing_zone_green_2020.sql
    ├── q5_green_taxi_trips_oct_2019.sql
    └── q6_stg_fhv_count.sql
```
