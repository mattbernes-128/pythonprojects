
CREATE TABLE mid.aggregate_financing_flow_cleaned AS
SELECT
    "Month" AS month,
    "AFRE(flow)" AS afre_flow,
    "RMB loans" AS rmb_loans,
    "Foreign currency-denominated loans" AS foreign_currency_loans,
    "Entrusted loans" AS entrusted_loans,
    "Trust loans" AS trust_loans,
    "Undiscounted bankers' acceptances" AS undiscounted_bankers_acceptances,
    "Net financing of corporate bonds" AS net_financing_corporate_bonds,
    "Government  bonds" AS government_bonds,
    "Equity financing on the domestic stock market by non-financial " AS equity_financing_non_financial,
    "Asset-backed securities of depository financial institutions" AS asset_backed_securities,
    "Loans written off" AS loans_written_off,
    data_file_name,
    ingestiond_dt
FROM (
SELECT "Month", "AFRE(flow)", "RMB loans", "Foreign currency-denominated loans", "Entrusted loans", "Trust loans", "Undiscounted bankers' acceptances", "Net financing of corporate bonds", "Government  bonds", "Equity financing on the domestic stock market by non-financial ", "Asset-backed securities of depository financial institutions", "Loans written off", data_file_name, ingestiond_dt
	FROM land."2024__Aggregate_Financing__Aggregate_Financing_XFlowX__clean"
union all
SELECT "Month", "AFRE(flow)", "RMB loans", "Foreign currency-denominated loans", "Entrusted loans", "Trust loans", "Undiscounted bankers' acceptances", "Net financing of corporate bonds", "Government  bonds", "Equity financing on the domestic stock market by non-financial ", "Asset-backed securities of depository financial institutions", "Loans written off", data_file_name, ingestiond_dt
	FROM land."2023__Aggregate_Financing__Aggregate_Financing_XFlowX__clean"
	) a