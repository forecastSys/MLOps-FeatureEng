WITH comp_500 AS (
    SELECT ID_BB_COMPANY FROM caesars_reporting_system.ent_company_info
    where BICS_LEVEL_1 = 10
),

cr2 AS (
    SELECT *,
        RANK() OVER (PARTITION BY LATEST_PERIOD_END_DT_FULL_RECORD,
            ID_BB_COMPANY,
            ID_BB_UNIQUE,
            FILING_STATUS,
            ACCOUNTING_STANDARD,
            EQY_CONSOLIDATED,
            EQY_FUND_CRNCY,
            FISCAL_YEAR_PERIOD
            ORDER BY REVISION_ID DESC) AS revision_rank
    FROM staging.bbgftp_fundamentals_cr2
    WHERE ID_BB_COMPANY IN (SELECT ID_BB_COMPANY FROM comp_500)
),

bs as (
select *
from staging.bbgftp_fundamentals_bs
where id_bb_company IN (SELECT ID_BB_COMPANY FROM comp_500)
),

`is` as (
select *
from staging.bbgftp_fundamentals_is
where ID_BB_COMPANY IN (SELECT ID_BB_COMPANY FROM comp_500)
),

cr as (
select *
from staging.bbgftp_fundamentals_cr
where ID_BB_COMPANY IN (SELECT ID_BB_COMPANY FROM comp_500)
),

cf as (
select *
from staging.bbgftp_fundamentals_cf
where ID_BB_COMPANY IN (SELECT ID_BB_COMPANY FROM comp_500)
),

sard_bs as (
select *
from staging.bbgftp_fundamentals_sard_bs
where ID_BB_COMPANY IN (SELECT ID_BB_COMPANY FROM comp_500)
),

sard_is as (
select *
from staging.bbgftp_fundamentals_sard_is
where ID_BB_COMPANY IN (SELECT ID_BB_COMPANY FROM comp_500)
),

sard_cf as (
select *
from staging.bbgftp_fundamentals_sard_cf
where ID_BB_COMPANY IN (SELECT ID_BB_COMPANY FROM comp_500)
)

-- select *
-- from cr2

-- select count(distinct ID_BB_UNIQUE)
-- from `is`


select a.*, b.*, c.*, d.*, e.*, f.*, g.*, h.*
from `is` as a
left join cr as b using (
LATEST_PERIOD_END_DT_FULL_RECORD,
-- join_date,
ID_BB_COMPANY,
ID_BB_UNIQUE,
FILING_STATUS,
ACCOUNTING_STANDARD,
EQY_CONSOLIDATED,
EQY_FUND_CRNCY,
FISCAL_YEAR_PERIOD)
left join cr2 as c using (
LATEST_PERIOD_END_DT_FULL_RECORD,
-- join_date,
ID_BB_COMPANY,
ID_BB_UNIQUE,
FILING_STATUS,
ACCOUNTING_STANDARD,
EQY_CONSOLIDATED,
EQY_FUND_CRNCY,
FISCAL_YEAR_PERIOD)
left join cf as d using (
LATEST_PERIOD_END_DT_FULL_RECORD,
-- join_date,
ID_BB_COMPANY,
ID_BB_UNIQUE,
FILING_STATUS,
ACCOUNTING_STANDARD,
EQY_CONSOLIDATED,
EQY_FUND_CRNCY,
FISCAL_YEAR_PERIOD)
left join bs as e using (
LATEST_PERIOD_END_DT_FULL_RECORD,
-- join_date,
ID_BB_COMPANY,
ID_BB_UNIQUE,
FILING_STATUS,
ACCOUNTING_STANDARD,
EQY_CONSOLIDATED,
EQY_FUND_CRNCY,
FISCAL_YEAR_PERIOD)
left join sard_bs as f using (
LATEST_PERIOD_END_DT_FULL_RECORD,
-- join_date,
ID_BB_COMPANY,
ID_BB_UNIQUE,
FILING_STATUS,
ACCOUNTING_STANDARD,
EQY_CONSOLIDATED,
EQY_FUND_CRNCY,
FISCAL_YEAR_PERIOD)
left join sard_is as g using (
LATEST_PERIOD_END_DT_FULL_RECORD,
-- join_date,
ID_BB_COMPANY,
ID_BB_UNIQUE,
FILING_STATUS,
ACCOUNTING_STANDARD,
EQY_CONSOLIDATED,
EQY_FUND_CRNCY,
FISCAL_YEAR_PERIOD)
left join sard_cf as h using (
LATEST_PERIOD_END_DT_FULL_RECORD,
-- join_date,
ID_BB_COMPANY,
ID_BB_UNIQUE,
FILING_STATUS,
ACCOUNTING_STANDARD,
EQY_CONSOLIDATED,
EQY_FUND_CRNCY,
FISCAL_YEAR_PERIOD)
where a.latest_period_end_dt_full_record < '2025-01-01' and a.latest_period_end_dt_full_record > '1990-01-01'
and c.revision_rank = 1;

-- SELECT *
-- FROM staging.bbgftp_fundamentals_cr2;

-- SELECT *
-- FROM staging.bbgftp_fundamentals_cr2
-- WHERE ID_BB_UNIQUE in (select DISTINCT ID_BB_UNIQUE from caesars_reporting_system.forecast_union_ebitda_rev_cashflow)

-- SELECT *
-- FROM staging.bbgftp_fundamentals_bs
-- WHERE ID_BB_UNIQUE in (select distinct ID_BB_UNIQUE from caesars_reporting_system.forecast_company_info);


-- WHERE revision_rank = 1;