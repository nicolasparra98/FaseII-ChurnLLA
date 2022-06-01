with
backlog as(
select distinct act_acct_cd
from (select distinct date(date_trunc('month',load_dt)) as month, date(load_dt) as load_dt,act_acct_cd,fi_outst_age
FROM "lla_cco_int_stg"."cwp_fix_union_dna")
where month=date('2022-02-01') and date(load_dt)=date(date_trunc('month',load_dt)) and fi_outst_age BETWEEN (90-(date_diff('day',date_trunc('Month', date(load_dt)),date_trunc('Month',date(load_dt)) + interval '1' MONTH - interval '1' day))) AND 89 
)
,harddx as(
select distinct act_acct_cd
from (select distinct date(date_trunc('month',load_dt)) as month, date(load_dt) as load_dt,act_acct_cd,fi_outst_age,CASE WHEN ACT_BLNG_CYCL IN('A','B','C') THEN 15 ELSE 28 END AS FirstOverdueDay
FROM "lla_cco_int_stg"."cwp_fix_union_dna")
where month=date('2022-02-01') and fi_outst_age=90
)
select count(distinct b.act_acct_Cd)
from backlog b left join harddx h on b.act_acct_cd=h.act_acct_cd
where h.act_acct_cd is null
