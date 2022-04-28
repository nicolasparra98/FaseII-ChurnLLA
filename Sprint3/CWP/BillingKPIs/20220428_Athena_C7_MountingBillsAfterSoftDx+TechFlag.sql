WITH TEch_filter as (
Select *
,CASE WHEN (PD_BB_ACCS_MEDIA='FTTH' OR PD_TV_ACCS_MEDIA ='FTTH' OR PD_VO_ACCS_MEDIA='FTTH') THEN 'FTTH'
      WHEN (PD_BB_ACCS_MEDIA='HFC' OR PD_TV_ACCS_MEDIA ='HFC' OR PD_VO_ACCS_MEDIA='HFC') THEN 'HFC'
      WHEN (PD_BB_ACCS_MEDIA='VDSL' OR PD_TV_ACCS_MEDIA ='VDSL' OR PD_VO_ACCS_MEDIA='VDSL' OR 
            PD_BB_ACCS_MEDIA='COPPER' OR PD_TV_ACCS_MEDIA ='COPPER' OR PD_VO_ACCS_MEDIA='COPPER') THEN 'COPPER'
      ELSE 'Other' END AS TechFlag
--era con desarrollo
FROM "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
AND act_acct_typ_grp ='MAS MOVIL'
),
customers as (
Select act_acct_cd,DATE_TRUNC('MONTH',CAST(load_dt AS DATE)) AS month_load, fi_outst_age,
CASE 
    WHEN fi_outst_age =46 then 'soft_dx'  
    WHEN fi_outst_age =90 then 'hard_dx'
ELSE NULL END as dx_flag
from TEch_filter
WHERE TechFlag  IS NOT NULL
    and  fi_bill_pmnt_dt_m0 is null
    and fi_bill_pmnt_dt_m1 IS null
    and fi_bill_pmnt_dt_m2 IS null
)
,Customers_bill as (
SELECT
act_acct_cd as act_acct_cd_bill ,act_cust_strt_dt,fi_bill_pmnt_dt_m0,fi_bill_pmnt_dt_m1, fi_bill_pmnt_dt_m2, fi_bill_pmnt_dt_m3, load_dt as load_dt_bill
--era con desarrollo
FROM "db-analytics-prod"."fixed_cwp"
)
,Join_billing as (
SELECT a.*, b.*
FROM customers AS a
LEFT JOIN Customers_bill AS b
ON a.act_acct_cd = b.act_acct_cd_bill
)

Select month_load,date_trunc('month',CAST(fi_bill_pmnt_dt_m0 AS DATE)),count(distinct act_acct_Cd)
from Join_billing
where dx_flag = 'soft_dx'
group by 1,2 order by 1,2
