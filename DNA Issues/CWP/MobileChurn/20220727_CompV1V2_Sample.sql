----------------------------v2
WITH 
clean_data as (
SELECT *,
CAST (billcycle_temp as BIGINT) as billcycle,CAST (invoutstanding_d_temp as decimal) as invoutstanding_d,CAST (FLG_OUTSTD_CURR_MO_temp as BIGINT) as FLG_OUTSTD_CURR_MO,CAST (FLG_OUTSTD_M1_temp as BIGINT) as FLG_OUTSTD_M1,CAST (FLG_OUTSTD_M2_temp as BIGINT) as FLG_OUTSTD_M2,CAST (FLG_OUTSTD_M3_temp as BIGINT) as FLG_OUTSTD_M3,CAST(flg_outstd_d_temp as BIGINT) as flg_outstd_d
FROM(
SELECT CAST(dt as DATE) as dt,accountno,account_status,biz_unit_d,invoutstanding_d as invoutstanding_d_temp,FLG_OUTSTD_CURR_MO as FLG_OUTSTD_CURR_MO_temp,FLG_OUTSTD_M1 as FLG_OUTSTD_M1_temp,FLG_OUTSTD_M2 as FLG_OUTSTD_M2_temp,FLG_OUTSTD_M3 as FLG_OUTSTD_M3_temp,billcycle as billcycle_temp,flg_outstd_d as flg_outstd_d_temp
,CAST(date_parse(REPLACE(inv_paymt_dt,'.','-'), '%Y-%m-%d %H:%i:%s') as DATE) as inv_paymt_dt
,CAST(date_parse(REPLACE(inv_exp_dt,'.','-'), '%Y-%m-%d %H:%i:%s') as DATE) as inv_exp_dt
FROM "db-analytics-dev"."tbl_postpaid_cwp"
WHERE "biz_unit_d"='B2C' AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN', 'RESTRICTED') AND INV_EXP_DT<>'nan' 
AND billcycle IN ('1','2','7','15','21','28')
AND  (FLG_OUTSTD_CURR_MO = '1' OR FLG_OUTSTD_CURR_MO = '0')
AND  (FLG_OUTSTD_M1 = '1' OR FLG_OUTSTD_M1 = '0')
AND  (FLG_OUTSTD_M2 = '1' OR FLG_OUTSTD_M2 = '0')
AND  (FLG_OUTSTD_M3 = '1' OR FLG_OUTSTD_M3 = '0')
AND CAST(dt as DATE) >= DATE ('2022-01-01')
))
,MONTHS_OD as (
SELECT *,month(DATE_ADD('month',-1,dt)) as dt_n1,DATE_DIFF('Month',inv_paymt_dt,dt) as pmntvsdt,
CASE WHEN DATE_DIFF('Month',inv_paymt_dt,dt) <= 1 THEN 0 
    WHEN invoutstanding_d = 0 THEN 0
    WHEN month(DATE_ADD('month',0,dt)) = month(CURR_M0_BILL_DT) THEN (MONTHS_OVERDUE+1)
    ELSE MONTHS_OVERDUE END AS MONTHS_OVERDUE_ADJST
FROM
(SELECT *,
DATE_ADD('day',billcycle-2,DATE_TRUNC('month', DATE_ADD('day',-15,inv_exp_dt))) as CURR_M0_BILL_DT,
CASE
    WHEN invoutstanding_d = 0 THEN 0 ELSE
        (CASE WHEN FLG_OUTSTD_CURR_MO = 1 THEN   
            (CASE WHEN FLG_OUTSTD_M1 = 1 THEN 
                (CASE WHEN FLG_OUTSTD_M2 = 1 THEN 
                    (CASE WHEN FLG_OUTSTD_M3 = 1 THEN 4 
                    ELSE 3 END)
                ELSE 2 END) 
            ELSE 1 END)
        ELSE 0 END)
    END as MONTHS_OVERDUE
FROM clean_data
)
)
,Oldest_unpaid_bill as (
SELECT *,
DATE_ADD('month',-MONTHS_OVERDUE,CURR_M0_BILL_DT) as oldest_v1,
DATE_ADD('month',-MONTHS_OVERDUE_ADJST,CURR_M0_BILL_DT) as oldest_v2
FROM MONTHS_OD
)
,outstanding_age as (
SELECT *, 
DATE_DIFF('day',oldest_v1,dt) fi_outsage_V1,
DATE_DIFF('day',oldest_v2,dt) fi_outs_age
FROM Oldest_unpaid_bill
)
,results_v2 as(
SELECT date_trunc('month',dt) as month,accountno
FROM outstanding_age
where dt >= DATE ('2022-02-01') and fi_outs_age=90
--group by 1 order by 1
)
-------------------------------------------------------------------------v1
,clean_data2 as (
SELECT *,
CAST (billcycle_temp as BIGINT) as billcycle,
CAST(flg_outstd_d_temp as BIGINT) as flg_outstd_d
FROM(
SELECT 
accountno,
account_status,
biz_unit_d,
billcycle as billcycle_temp,
flg_outstd_d as flg_outstd_d_temp,
CAST(dt as DATE) as dt,
CAST (FLG_OUTSTD_CURR_MO as BIGINT) as FLG_OUTSTD_CURR_MO,
CAST(date_parse(REPLACE(inv_paymt_dt,'.','-'), '%Y-%m-%d %H:%i:%s') as DATE) as inv_paymt_dt,
CAST(date_parse(REPLACE(inv_exp_dt,'.','-'), '%Y-%m-%d %H:%i:%s') as DATE) as inv_exp_dt
FROM "db-analytics-dev"."tbl_postpaid_cwp"
WHERE "biz_unit_d"='B2C' AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN', 'RESTRICTED') AND INV_EXP_DT<>'nan' 
AND billcycle IN ('1','2','7','15','21','28')
AND  (flg_outstd_d = '1' OR flg_outstd_d = '0')
)
),

custom_fields_data as (
SELECT  
*, 
CASE WHEN  CAST(flg_outstd_d as BIGINT) = 1 THEN 1
     WHEN  CAST(flg_outstd_d as BIGINT) = 0
        AND bill_dt <= dt
        AND inv_paymt_dt  <= bill_dt THEN 1 ELSE 0 END AS flg_outstd_d_final
FROM        
(
SELECT *,
DATE_ADD('day',billcycle-2,DATE_TRUNC('month', DATE_ADD('day',-15,inv_exp_dt))) as bill_dt
FROM clean_data2
)
),

fi_outsage as (
SELECT *, date_add('day',-fi_outs_age,dt) as oldest_unpaid_bill
FROM
(
SELECT dt,accountno,account_status,biz_unit_d,billcycle,bill_dt,inv_paymt_dt,inv_exp_dt,
   CASE WHEN 	
flg_outstd_d_final = 0 THEN 0 ELSE 
   (SUM (flg_outstd_d_final) OVER (PARTITION BY accountno,inv_paymt_dt ORDER BY dt ASC)) END as fi_outs_age 
FROM custom_fields_data
ORDER BY dt DESC
)
),

-- 8. Rangos oustanding age
aging_panel as (
SELECT *,
CASE
        WHEN fi_outs_age >=1 AND fi_outs_age <= 15 then 'a. 1 - 15 days'
        WHEN fi_outs_age >15 AND fi_outs_age <= 30 then 'b. 16 - 30 days'
        WHEN fi_outs_age >30 AND fi_outs_age <= 45 then 'c. 31 - 45 days'
        WHEN fi_outs_age >45 AND fi_outs_age <= 60 then 'd. 46 - 60 days'
        WHEN fi_outs_age >60 AND fi_outs_age <= 90 then 'e. 61 - 90 days'
        WHEN fi_outs_age >90 THEN 'f. more than 90 days'
        Else 'NA' END AS Range_fi_outs_age 
FROM fi_outsage
WHERE account_status != 'CEASED' 
AND biz_unit_d ='B2C'
)
,results_v1 as(
SELECT distinct date_trunc('month',dt) as month,accountno
FROM aging_panel
WHERE dt >= DATE ('2022-02-01') and fi_outs_age =90 
--group by 1 order by 1
)
select distinct --a.month as month_v1,a.accountno as account_v1,
b.month as month_v2,b.accountno as account_v2
--a.month,count(distinct a.accountno)
from results_v1 a right join results_v2 b on a.accountno=b.accountno
--where a.month<>b.month
where a.accountno is null
--group by 1--,2
--order by 1,2
