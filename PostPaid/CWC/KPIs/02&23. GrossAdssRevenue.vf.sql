WITH 
GrossAddsPostpaid AS (
SELECT DISTINCT account_id, min(safe_cast(account_creation_date as date)) AS MinStartDate
,DATE_TRUNC(safe_cast(dt as date),MONTH) AS Month,total_mrc_mo AS mrc_amt,dt
,max(safe_cast(dt as date)) as MaxDateMonth, max(safe_cast(account_creation_date as date)) as MaxStart
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential"
 AND account_status NOT IN('Ceased','Closed','Recommended for cease')
 AND dt=LAST_DAY(dt,Month)
GROUP BY account_id,Month,total_mrc_mo,dt
HAVING MinStartDate>=Month AND DATE_TRUNC(MinStartDate,Month)=Month
)
,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, account_id, avg(safe_cast(mrc_amt as float64)) AS AvgMRC
  FROM GrossAddsPostpaid
  WHERE mrc_amt IS NOT NULL AND safe_cast(mrc_amt AS float64) <> 0 AND NOT IS_NAN(MRC_AMT)
  GROUP BY Month,account_id,mrc_amt
)
,GrossAddsAdjusted AS(
SELECT DISTINCT g.Month,g.account_id,avgMRC,g.mrc_amt
FROM GrossAddsPostpaid g LEFT JOIN AverageMRC_User a ON g.account_id=a.account_id
WHERE ((AvgMRC IS NOT NULL AND AvgMRC <> 0 AND NOT IS_NAN(AvgMRC)
 AND DATE_DIFF(MaxDateMonth, MaxStart, DAY)>60) OR  (DATE_DIFF(MaxDateMonth, MaxStart, DAY)<=60))
)
SELECT DISTINCT Month,COUNT(DISTINCT account_id) AS Records
,ROUND(SUM(avgMRC),2) AS Revenue,ROUND(SUM(avgMRC)/COUNT(DISTINCT account_id),2) AS ARPU
FROM GrossAddsAdjusted 
GROUP BY Month
ORDER BY Month
