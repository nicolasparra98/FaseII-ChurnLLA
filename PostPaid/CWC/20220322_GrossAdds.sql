WITH 
GrossAddsPostpaid AS (
SELECT DISTINCT account_id, min(safe_cast(account_creation_date as date)) AS MinStartDate
,DATE_TRUNC(safe_cast(dt as date),MONTH) AS Month
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history`   
WHERE org_id = "338" AND account_type ="Residential"
 AND account_status NOT IN('Ceased','Closed','Recommended for cease')
GROUP BY account_id,Month
HAVING MinStartDate>=Month
)
SELECT --Month,account_id
Month,COUNT(DISTINCT account_id) AS GrossAdds
FROM GrossAddsPostpaid
GROUP BY Month ORDER BY Month
