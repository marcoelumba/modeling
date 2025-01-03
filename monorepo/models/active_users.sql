{% apply_incremental_config('customer_id', 'last_booking_date') %} -- Use incremental config from generic_utils

WITH raw_users AS (
    SELECT 
        customer_id,
        LOWER(name) AS name,  -- Normalize names to lowercase
        DATE(signup_date) AS signup_date,  -- Ensure date format
        DATE(last_booking_date) AS last_booking_date,  -- Ensure date format
        others
    FROM {{ ref('users') }}
    {% if is_incremental() %}
    WHERE last_booking_date > (
        SELECT MAX(last_booking_date) - INTERVAL 3 DAY  -- Incremental lookback window
        FROM {{ this }}
    )
    {% endif %}
),

active_users AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_booking_date DESC) AS row_num
    FROM raw_users
    WHERE last_booking_date >= {{ get_date_threshold() }} -- Threshold logic from generic_utils
)

SELECT
    customer_id,
    name,
    signup_date,
    last_booking_date,
    others
FROM active_users
WHERE row_num = 1  -- Keep only the latest record per customer

