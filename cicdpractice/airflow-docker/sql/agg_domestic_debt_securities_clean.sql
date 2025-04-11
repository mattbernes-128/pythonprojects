DROP TABLE IF EXISTS mid.domestic_debt_securities_cleaned;
CREATE TABLE mid.domestic_debt_securities_cleaned AS
SELECT
    "Year" AS year,
    "Government_Securities__Issue" AS government_securities_issue,
    "Government_Securities__Outstanding" AS government_securities_outstanding,
    "Central_Bank_Bills__Issue" AS central_bank_bills_issue,
    "Central_Bank_Bills__Outstanding" AS central_bank_bills_outstanding,
    "Financial_Bonds__Issue" AS financial_bonds_issue,
    "Financial_Bonds__Outstanding" AS financial_bonds_outstanding,
    "Corporate_Debenture_Bonds__Issue" AS corporate_debenture_bonds_issue,
    "Corporate_Debenture_Bonds__Outstanding" AS corporate_debenture_bonds_outstanding,
    "International_Institution_Bonds__Issue" AS international_institution_bonds_issue,
    "International_Institution_Bonds__Outstanding" AS international_institution_bonds_outstanding,
    "Total__Issue" AS total_issue,
    "Total__Outstanding" AS total_outstanding,
    data_file_name,
    ingestiond_dt
FROM (
    SELECT * FROM land."2024Domestic_Debt_Securities__clean"
    UNION ALL
    SELECT * FROM land."2023Domestic_Debt_Securities__clean"
    UNION ALL
    SELECT * FROM land."2022Domestic_Debt_Securities__clean"
) a;
