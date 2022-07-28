with prueba as(
select distinct dt,fi_outs_age_raw,substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7) as m,lpad(billcycle_m1,2,'0') as pad,lpad(billcycle,2,'0') as pad2,
--date(
--concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle_m1,2,'0')) as conc
--,date_add('day',-1,date(concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle,2,'0')))) as conc2
--)
--date_add('day', -fi_outs_age_raw, dt)
--date(concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle_m1,2,'0'))) - interval '1' month
case when date_add('day',-1,date(concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle,2,'0')))) >  date_add('day', -fi_outs_age_raw, dt) then  date_add('day',-1,date(concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle,2,'0')))) - interval '1' month 
else date_add('day',-1,date(concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle,2,'0')))) end as OLDEST_UNPAID_BILL_CORRECTED_corrected
from(
select distinct date(dt) as dt
, billcycle
, cast(cast(billcycle as int)-1 as varchar) as billcycle_m1
,CASE WHEN cast(cast(flg_outstd_d as double) as int) = 0 THEN 0 ELSE (SUM (cast(cast(flg_outstd_d as double) as int)) OVER (PARTITION BY accountno,inv_paymt_dt ORDER BY dt ASC)) END as fi_outs_age_raw
    FROM "db-analytics-dev"."tbl_postpaid_cwp"
    where date(dt) between  date('2022-02-01') and date('2022-07-25')
    and billcycle IN ('1','2','7','15','21','28')
)
)
select *
from prueba
--where conc='2022-03-00'
