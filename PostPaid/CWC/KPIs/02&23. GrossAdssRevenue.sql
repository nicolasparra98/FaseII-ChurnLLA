WITH 
GrossAddsPostpaid AS (
SELECT DISTINCT account_id, min(safe_cast(account_creation_date as date)) AS MinStartDate
,DATE_TRUNC(safe_cast(dt as date),MONTH) AS Month
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history`   
WHERE org_id = "338" AND account_type ="Residential"
 AND account_status NOT IN('Ceased','Closed','Recommended for cease')
GROUP BY account_id,Month
HAVING MinStartDate>=Month AND DATE_TRUNC(MinStartDate,Month)=Month
)
,LastDNAValue AS (
SELECT DISTINCT g.month,g.account_id,LAST_DAY(safe_cast(dt as date),MONTH) AS LastDay
,AVG(safe_cast(total_mrc_mo as float64)) AS MRCprom
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history` l INNER JOIN GrossAddsPostpaid g
 ON l.account_id=g.account_id AND DATE_TRUNC(safe_cast(l.dt as date),Month)=g.Month
WHERE total_mrc_mo IS NOT NULL AND NOT IS_NAN(safe_cast(total_mrc_mo as float64)) AND safe_cast(total_mrc_mo as float64)<> 0 
GROUP BY 1,2,3
)
SELECT DISTINCT --*
Month,count(distinct account_id) AS Records,ROUND(SUM(MRCprom),2) AS Revenue
,ROUND(ROUND(SUM(MRCprom),2)/count(distinct account_id),2) AS ARPU
FROM LastDNAValue 
GROUP BY Month ORDER BY Month
