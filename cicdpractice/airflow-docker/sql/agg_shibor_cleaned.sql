DROP TABLE IF EXISTS mid.shanghai_interbank_offer_rate_cleaned;
CREATE TABLE mid.shanghai_interbank_offer_rate_cleaned AS
SELECT
  "Year" AS year,
  "Overnight" AS overnight,
  "1-Week" AS one_week,
  "2-Week" AS two_week,
  "1-Month" AS one_month,
  "3-Month" AS three_month,
  "6-Month" AS six_month,
  "9-Month" AS nine_month,
  "1-Year" AS one_year,
  data_file_name,
  ingestiond_dt
FROM (
  SELECT * FROM land."2024Shibor__clean"
  UNION ALL
  SELECT * FROM land."2023Shibor__clean"
  UNION ALL
  SELECT * FROM land."2022Shibor__clean"
) a;