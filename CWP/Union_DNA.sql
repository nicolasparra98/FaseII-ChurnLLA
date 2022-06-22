--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_fix_union_dna" as

WITH

UsefulDays_DEV as(
SELECT *  FROM "db-analytics-dev"."dna_fixed_cwp" 
WHERE date(dt) not in (date('2021-11-25'), date('2021-11-26'), date('2021-11-27'), date('2021-11-28')
, date('2021-11-29'), date('2021-11-30'), date('2021-12-01'), date('2021-12-02'), date('2021-12-03'), date('2021-12-04'),
date('2021-12-05'), date('2021-02-03'),date('2021-02-04'), date('2021-04-02'))
--AND act_cust_typ_nm = 'Residencial'
--AND act_acct_typ_grp ='MAS MOVIL'
)
,
UsefulDays_PROD as (
SELECT * FROM
"db-analytics-prod"."fixed_cwp"
--WHERE act_cust_typ_nm = 'Residencial'
--AND act_acct_typ_grp ='MAS MOVIL'
)
,
DEV_PROD_JOIN as(
SELECT *
FROM UsefulDays_DEV UNION ALL
SELECT * FROM UsefulDays_PROD)
select * from dev_prod_join
