# Analytics Engineer Technical Task

## Introduction

The goal of this task is to design and implement a model in dbt to track active users within a dynamically adjustable time window. The dataset consists of a `users` table containing details such as `customer_id`, `name`, `signup_date`, and `last_booking_date`. The solution must ensure the model is efficient, tested, and scalable. Key requirements include creating a dbt model for filtering active users, adding tests for data quality, setting up incremental updates, and defining development and production targets. This document outlines the approach, implementation details, and alternative options considered for each task. Additionally, any code snippets linked to GitHub in this document represent implementations directly applied in the project.

## Task 1: Create a dbt Model (`active_users.sql`)

### Objective:
Filter users who made a booking in the last 30 days based on their `last_booking_date`, with a dynamically adjustable threshold.

### Implementation:
1. Declare a global variable in `dbt_project.yml`:
   [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/dbt_project.yml#L36)
    ```yaml
    vars:
      global_days_threshold: 30
    ```
    This provides centralized configuration, making the threshold easy to manage and reusable across environments.

2. Create a macro to calculate the date threshold dynamically:
   [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/macros/generic_utils.sql#L11)
    ```sql
    {% macro get_date_threshold(days_threshold = var('global_days_threshold', 30)) %}
        dateadd(day, {{ -days_threshold }}, current_date)
    {% endmacro %}
    ```
    Using a macro improves code reusability and consistency while keeping the model clean.

3. Call the macro in `active_users.sql`:
    - Default threshold:
      [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/models/active_users.sql#L24)
      ```sql
      DATE(last_booking_date) >= {{ get_date_threshold() }}
      ```
    - Overriding the default threshold:
      ```sql
      DATE(last_booking_date) >= {{ get_date_threshold(7) }}
      ```

### Alternative Options Considered:
- **Inline Jinja Template Calculations:**
  Directly embedding the calculation in the model like this:
  ```sql
  DATE(last_booking_date) >= DATEADD(DAY, -{{ var('global_days_threshold', 30) }}, CURRENT_DATE)
  ```
  While this approach is functional, it lacks reusability and makes the code harder to maintain compared to using a macro.

- **Hardcoding the Threshold:**
  Hardcoding a fixed value like 30 days:
  ```sql
  DATE(last_booking_date) >= DATEADD(DAY, -30, CURRENT_DATE)
  ```
  This option is inflexible, as changing the threshold would require updating the model directly, increasing maintenance overhead.

---

## Task 2: Add a Uniqueness Test

### Objective:
Ensure `customer_id` is unique and not null in the final table.

### Implementation in `schema.yml`:
Define an uniqueness test for customer_id column the simplist way by using "- unique" built-in test. 
[View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/models/schema.yml#L8)
```yaml
models:
  - name: active_users
    columns:
      - name: customer_id
        data_tests:
          - unique
          - not_null
```

### Additional Approaches Considered:
- **Using `QUALIFY` (if supported):**
  Ensures the most recent record for each user:
  ```sql
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_booking_date DESC) = 1
  ```

- **Using `ROW_NUMBER` with Filtering:**
  This approach is compatible with more databases:
  [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/models/active_users.sql#L22)
  ```sql
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_booking_date DESC) AS row_num
  ```
  Then filter using:
  [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/models/active_users.sql#L34)
  ```sql
  WHERE row_num = 1
  ```

Both methods ensure uniqueness, but `ROW_NUMBER()` was preferred for its supportability without QUALIFY.

---

## Task 3: Set Up Incremental Updates

### Objective:
Process only new or modified users during each dbt run for efficiency.

### Implementation:
1. Inline call in `active_users.sql`:
   [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/models/active_users.sql#L1)
    ```sql
    {% apply_incremental_config('customer_id', 'last_booking_date') %}
    ```
2. Macro for reusability:
   [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/macros/generic_utils.sql#L16)
    ```sql
    {% macro apply_incremental_config(unique_key, partition_field, data_type='date') %}
      {{
          config(
              materialized='incremental',
              unique_key=unique_key,
              incremental_strategy='merge',
              partition_by={
                  "field": partition_field,
                  "data_type": data_type
              }
          )
      }}
    {% endmacro %}
    ```
3. Add a 3-day lookback for delayed data ingestion:
   [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/models/active_users.sql#L11)
    ```sql
    {% if is_incremental() %}
    WHERE last_booking_date > (
        SELECT MAX(last_booking_date) - INTERVAL 3 DAY
        FROM {{ this }}
    )
    {% endif %}
    ```

### Alternative Options Considered:
- **Append-Only Strategy:**
  Appending new records without merging:
  ```yaml
  incremental_strategy: append
  ```
  This was not applied, as it cannot handle updates to existing records.

- **Centralized Configuration in `dbt_project.yml`:**
  Defining incremental materialization globally in `dbt_project.yml`:
  ```yaml
  models:
    my_project:
      active_users:
        materialized: incremental
        unique_key: customer_id
        incremental_strategy: merge
        partition_by:
          field: last_booking_date
          data_type: date
  ```
  While this approach centralizes configurations, it reduces flexibility for model-specific customizations.

---

## Task 4: Define Targets (Environments)

### Objective:
Configure separate development and production environments.

### Implementation in `dbt_project.yml`:
1. Dynamic schema for development:
   [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/dbt_project.yml#L31)
    ```yaml
    +schema: "{{ get_dynamic_schema() }}"
    ```
2. Macro for dynamic schema:
   [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/macros/generic_utils.sql#L2)
    ```sql
    {% macro get_dynamic_schema() %}
      {% if target.name == 'dev' %}
        {{ var('user') }}
      {% else %}
        prod_schema
      {% endif %}
    {% endmacro %}
    ```
3. Add `vars` to `profiles.yml`:
   [View code](https://github.com/marcoelumba/modeling/blob/ab9c6b6778743d7253345351fb07ca1566f97076/monorepo/profiles.yml#L10)
    ```yaml
    vars:
      user: "{{ env_var('USER', 'hw_user') }}"
    ```
    Ensure `USER` is set locally for development (e.g., `export USER=marco`).

### Recommended Practice:
- Keep production configurations only in CI/CD pipelines for security.
- Limit access to production configurations.
- Set up a local user environment to have only a development target to avoid altering production during development and without reviews.

### Alternative Options Considered:
- **Standard schema none dynamic:**
  This is a good approach but not dynamic. Additionally, as recommended, the best practice is to separate prod and dev to reduce the risk of running dbt run command in prod while in development.
    ```yaml
    +schema: "{{ target.schema }}"
    +database: "{{ target.database }}"
    ```

- **Single Schema Across Environments:**
  Using a single schema for both environments simplifies configuration but risks unintentional overwrites during development.

- **Manually Set Schema per User:**
  Hardcoding user-specific schemas in the profile configuration for local development:
  ```yaml
  dev:
   schema: "dev_marco"
  ```
  This approach lacks scalability and flexibility.

---

## Folder Structure

### Proposed Folder Structure:
The following structure organizes dbt files efficiently:
```
monorepo/
├── dbt_project.yml
├── models/
│   ├── active_users.sql
│   ├── schema.yml
├── macros/
│   └── generic_utils.sql
├── profiles.yml  # Can be stored under ~/.dbt/ for security.
```
### Rationale:
- Keeping `macros` separate ensures reusability across models.
- Storing sensitive information like `profiles.yml` outside version control reduces security risks.

---

This structured solution ensures scalability, flexibility, and efficiency in tracking active users while adhering to dbt best practices.
