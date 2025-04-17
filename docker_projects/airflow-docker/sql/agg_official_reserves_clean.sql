DROP TABLE IF EXISTS mid.official_reserves_cleaned; -- Added IF EXISTS for safety

CREATE TABLE mid.official_reserves_cleaned AS
SELECT
    CAST(year AS FLOAT) AS year,
    CAST(REGEXP_REPLACE(TRIM("       Foreign currency reserves "), '[^0-9.-]+', '', 'g') AS FLOAT) AS foreign_currency_reserves,
    CAST(REGEXP_REPLACE(TRIM("       IMF reserve position"), '[^0-9.-]+', '', 'g') AS FLOAT) AS imf_reserve_position,
    CAST(REGEXP_REPLACE(TRIM("       SDRs"), '[^0-9.-]+', '', 'g') AS FLOAT) AS sdrs,
    CAST(REGEXP_REPLACE(TRIM("       Gold "), '[^0-9.-]+', '', 'g') AS FLOAT) AS gold,
    "Gold Weight" AS gold_weight, -- Assuming this doesn't need the same aggressive cleaning
    CAST(REGEXP_REPLACE(TRIM("      Other reserve assets "), '[^0-9.-]+', '', 'g') AS FLOAT) AS other_reserve_assets,
    CAST(REGEXP_REPLACE(TRIM("      Total"), '[^0-9.-]+', '', 'g') AS FLOAT) AS total,
    data_file_name,
    ingestiond_dt
FROM (
    SELECT *
    FROM land."2024__Official_reserve_assets__clean"
    UNION ALL
    SELECT *
    FROM land."2023__Official_reserve_assets__clean"
    UNION ALL
    SELECT *
    FROM land."2022__Official_reserve_assets__clean"
) AS a;






