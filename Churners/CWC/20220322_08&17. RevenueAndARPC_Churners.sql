WITH 
---------------------------------------------------Voluntary Churners-------------------------------------------------------------------
-- Last churn date on the voluntary churn base per customer
MAXFECHACHURNMES AS(
SELECT DISTINCT src_account_id, PARSE_DATE("%Y%m%d",max(reporting_date_key)) AS MaxFecha
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-17_Jamaica_VoluntaryChurners_v2` 
GROUP BY src_account_id
),
-- Number of churned RGUs on the maximum date - it doesn't consider mobile yet
FIXEDCHURNEDRGUS AS(
SELECT DISTINCT DATE_TRUNC(MaxFecha, MONTH) AS ChurnMonth, t.src_account_id, count(*) as NumChurns
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-17_Jamaica_VoluntaryChurners_v2` t
INNER JOIN MAXFECHACHURNMES m ON t.src_account_id = m.src_account_id AND PARSE_DATE("%Y%m%d",t.reporting_date_key) = MaxFecha
WHERE lob <> "Mobile Postpaid"
GROUP BY src_account_id, ChurnMonth
ORDER BY NumChurns desc
),
-- Number of RGUs a customer has on the last record of the month
RGUSLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC(LOAD_DT, MONTH) AS Month, act_acct_cd,
CASE WHEN last_value(pd_mix_nm) over(partition by act_acct_cd order by load_dt) IN ('VO', 'BO', 'TV') THEN 1
WHEN last_value(pd_mix_nm) over(partition by act_acct_cd order by load_dt) IN ('BO+VO', 'BO+TV', 'VO+TV') THEN 2
WHEN last_value(pd_mix_nm) over(partition by act_acct_cd order by load_dt) IN ('BO+VO+TV') THEN 3
ELSE 0 END AS NumRgusLastRecord,
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND (fi_outst_age <= 90 OR fi_outst_age IS NULL) 
 ORDER BY act_acct_cd
),
-- Date of the last record of the month per customer
LastRecordDateDNA AS(
SELECT DISTINCT DATE_TRUNC(LOAD_DT, MONTH) AS Month, act_acct_cd,max(load_dt) as LastDate
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND (fi_outst_age <= 90 OR fi_outst_age IS NULL) 
 GROUP BY MONTH, act_acct_cd
 ORDER BY act_acct_cd 
),
-- Number of outstanding days on the last record date
OverdueLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC(LOAD_DT, MONTH) AS Month, t.act_acct_cd, fi_outst_age as LastOverdueRecord
,fi_tot_mrc_amt
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` t 
INNER JOIN LastRecordDateDNA d ON t.act_acct_cd = d.act_acct_cd AND t.load_dt = d.LastDate
WHERE org_cntry="Jamaica" AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
),
-- Total Voluntary Churners considering number of churned RGUs, outstanding age and churn date
VoluntaryTotalChurners AS(
SELECT distinct l.Month, l.act_acct_cd, d.LastDate,fi_tot_mrc_amt,
CASE WHEN length(cast(l.act_acct_cd AS STRING)) = 12 THEN "1. Liberate"
ELSE "2. Cerilion" END AS BillingSystem,
CASE WHEN (d.LastDate = date_trunc(d.LastDate, Month) or d.LastDate = LAST_DAY(d.LastDate, MONTH)) THEN "1. First/Last Day Churner"
ELSE "2. Other Date Churner" END AS ChurnDateType,
CASE WHEN LastOverdueRecord >= 90 THEN "2. MixedChurner"
ELSE "1. VoluntaryChurner" END AS ChurnerType
FROM FIXEDCHURNEDRGUS f INNER JOIN RGUSLastRecordDNA l ON f.src_account_id = l.act_acct_cd 
AND f.NumChurns = l.NumRgusLastRecord AND f.ChurnMonth = l.Month
INNER JOIN LastRecordDateDNA d on f.src_account_id = d.act_acct_cd AND f.ChurnMonth = d.Month
INNER JOIN OverdueLastRecordDNA o ON f.src_account_id = o.act_acct_cd AND f.ChurnMonth = o.Month
)
,VoluntaryChurners AS(
SELECT Month, SAFE_CAST(act_acct_cd AS STRING) AS Account, ChurnerType,fi_tot_mrc_amt
FROM VoluntaryTotalChurners 
WHERE ChurnerType="1. VoluntaryChurner"
GROUP BY Month, act_acct_cd, ChurnerType,fi_tot_mrc_amt
)
---------------------------------------------------Involuntary Churners-------------------------------------------------------------------
,CUSTOMERS_FIRSTLAST_RECORD AS(
 SELECT DISTINCT DATE_TRUNC (LOAD_DT, MONTH) AS MES, act_acct_cd AS Account, Min(load_dt) as FirstCustRecord, Max(load_dt) as LastCustRecord
 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
 WHERE org_cntry = "Jamaica" 
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') 
 GROUP BY MES, account
),
NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC (LOAD_DT, MONTH) AS MES, act_acct_cd AS Account, fi_outst_age
 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` t
 INNER JOIN CUSTOMERS_FIRSTLAST_RECORD r ON t.load_dt = r.FirstCustRecord and r.account = t.act_acct_cd
 WHERE org_cntry = "Jamaica" 
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') 
 AND fi_outst_age <= 90
 GROUP BY MES, account, fi_outst_age
),
OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC (LOAD_DT, MONTH) AS MES, act_acct_cd AS Account, fi_outst_age,fi_tot_mrc_amt 
 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` t
 INNER JOIN CUSTOMERS_FIRSTLAST_RECORD r ON t.load_dt = r.LastCustRecord and r.account = t.act_acct_cd
 WHERE org_cntry = "Jamaica" 
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') 
 AND fi_outst_age >= 90
 GROUP BY MES, account, fi_outst_age,fi_tot_mrc_amt
),
INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account,fi_tot_mrc_amt
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,InvoluntaryChurners AS(
SELECT DISTINCT Month, SAFE_CAST(Account AS STRING) AS Account, fi_tot_mrc_amt
,CASE WHEN Account IS NOT NULL THEN "2. InvoluntaryChurner" END AS ChurnerType
FROM INVOLUNTARYNETCHURNERS 
GROUP BY Month, Account,ChurnerType,fi_tot_mrc_amt
)
,AllChurners AS(
SELECT DISTINCT Month,Account,ChurnerType,SAFE_CAST(fi_tot_mrc_amt AS FLOAT64) AS LastBill
from (SELECT Month,Account,ChurnerType,fi_tot_mrc_amt from VoluntaryChurners a 
      UNION ALL
      SELECT Month,Account,ChurnerType,fi_tot_mrc_amt from InvoluntaryChurners b)
)
,FirstDayBill AS(
SELECT act_acct_cd, DATE_TRUNC(load_dt,Month) AS MonthBill,SAFE_CAST(fi_tot_mrc_amt AS FLOAT64) AS FirstBill
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt=DATE_TRUNC(load_dt,Month)
 AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
 AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
,RevenuePerChurner AS(
SELECT Month,Account,ChurnerType,FirstBill,LastBill,(LastBill-FirstBill) AS BillDiff
FROM AllChurners c INNER JOIN FirstDayBill f ON c.Account=SAFE_CAST(f.act_acct_cd AS STRING)
)
SELECT Month,ChurnerType
,ROUND(SUM(BillDiff),2) AS Revenue,ROUND(AVG(BillDiff),2) AS ARPC 
,ROUND(SUM(LastBill),2) AS RevLastBill,ROUND(AVG(LastBill),2) AS ARPC_LastBill
,COUNT(DISTINCT Account) AS Records
FROM RevenuePerChurner
GROUP BY Month,ChurnerType
ORDER BY ChurnerType

