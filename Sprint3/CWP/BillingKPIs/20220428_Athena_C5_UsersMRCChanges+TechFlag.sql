ITH check_tech as (
Select *
,CASE WHEN (PD_BB_ACCS_MEDIA='FTTH' OR PD_TV_ACCS_MEDIA ='FTTH' OR PD_VO_ACCS_MEDIA='FTTH') THEN 'FTTH'
      WHEN (PD_BB_ACCS_MEDIA='HFC' OR PD_TV_ACCS_MEDIA ='HFC' OR PD_VO_ACCS_MEDIA='HFC') THEN 'HFC'
      WHEN (PD_BB_ACCS_MEDIA='VDSL' OR PD_TV_ACCS_MEDIA ='VDSL' OR PD_VO_ACCS_MEDIA='VDSL' OR 
            PD_BB_ACCS_MEDIA='COPPER' OR PD_TV_ACCS_MEDIA ='COPPER' OR PD_VO_ACCS_MEDIA='COPPER') THEN 'COPPER'
      ELSE 'Other' END AS TechFlag
FROM "db-analytics-prod"."fixed_cwp"
), 
Monthly_customers as (
Select act_acct_cd, CAST(date_trunc('month',load_dt) AS DATE) as month_load ,CONCAT(act_acct_cd, SUBSTR(CAST(load_dt AS varchar),1,7)) AS key_dna
from check_tech
WHERE act_cust_typ_nm = 'Residencial'
AND act_acct_typ_grp ='MAS MOVIL'
AND techflag is not null
GROUP BY  CONCAT(act_acct_cd, SUBSTR(CAST(load_dt AS varchar),1,7)) ,act_acct_cd, date_trunc('month',load_dt), techflag
order by  act_acct_cd
),
MRC_changes as (
SELECT  CONCAT(act_acct_cd, SUBSTR(CAST(load_dt AS VARCHAR),1,7)) as Key_MRC_changes,((fi_tot_mrc_amt-fi_tot_mrc_amt_prev)/fi_tot_mrc_amt_prev)  AS MRC_Change
FROM  "db-analytics-prod"."fixed_cwp"
WHERE pd_vo_prod_nm_prev = pd_vo_prod_nm
AND pd_bb_prod_nm_prev = pd_BB_prod_nm
AND pd_tv_prod_nm_prev = pd_tv_prod_nm
group by CONCAT(act_acct_cd, SUBSTR(CAST(load_dt AS VARCHAR),1,7)),((fi_tot_mrc_amt-fi_tot_mrc_amt_prev)/fi_tot_mrc_amt_prev) 
),
Join_MRC_chage as (
SELECT
a. *,
b. *
FROM Monthly_customers AS a
LEFT JOIN MRC_changes AS b
ON a.key_dna = b.Key_MRC_changes
)
,ChangeMRC_Flag AS (
SELECT act_acct_cd, month_load, MRC_change,
CASE WHEN MRC_change > 0.1 or MRC_change< -0.1 then 1 else 0 END as MRC_change_flag
FROM Join_MRC_chage
)
SELECT DISTINCT Month_load,COUNT(DISTINCT act_acct_cd)
FROM ChangeMRC_Flag
WHERE MRC_change_flag=1
GROUP BY 1 ORDER BY 1
