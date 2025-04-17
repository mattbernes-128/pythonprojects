DROP TABLE IF EXISTS mid.stockmarket_cleaned;
CREATE TABLE mid.stockmarket_cleaned as
SELECT
  "Year" AS year,
  "Equity Financing" AS equity_financing,
  "Volume of Trading" AS volume_of_trading,
  "Turnover of Trading" AS turnover_of_trading,
  "Volume Issued" AS volume_issued,
  "Total Market Capitalization at End of Period" AS total_market_capitalization_end_period,
  "Number of listed company" AS number_of_listed_company,
  data_file_name,
  ingestiond_dt
FROM (
SELECT *
FROM land."2024Stock_Market__clean"
UNION ALL
SELECT *
FROM land."2023Stock_Market__clean"
UNION ALL
SELECT *
FROM land."2022Stock_Market__clean"
UNION ALL
SELECT *
FROM land."2021Stock_Market__clean"
UNION ALL
SELECT *
FROM land."2020Stock_Market__clean"
)a