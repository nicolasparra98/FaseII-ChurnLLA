WITH
Lag AS(
SELECT DISTINCT DATE(load_dt) AS load_dt,act_acct_cd,fi_outst_age
,lag(fi_outst_age) over (partition by act_acct_cd order by date(load_dt)) as prev_outst_age_1
,lag(fi_outst_age,2) over (partition by act_acct_cd order by date(load_dt)) as prev_outst_age_2
FROM "lla_cco_int_stg"."cwp_fix_union_dna" 
)
SELECT DISTINCT load_dt,act_acct_cd,fi_outst_age
FROM Lag
WHERE fi_outst_age>1 AND prev_outst_age_1 IS NULL AND prev_outst_age_2=fi_outst_age-2 AND fi_outst_age<=90
and date_trunc('month',date(load_dt))=date('2022-02-01')
ORDER BY 1 desc,2
