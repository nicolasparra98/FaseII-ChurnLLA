WITH 
GrossAddsPostpaid AS (
SELECT DISTINCT account_id, min(safe_cast(customer_creation_date as date)) AS MinStartDate,DATE_TRUNC(safe_cast(dt as date),MONTH) AS Month
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history`   
WHERE country_name="nan"
 AND (region_name LIKE "%jama%" OR region_name LIKE "%JAMA%" OR region_name LIKE "%Jama%" OR region_name LIKE "%Jami%" 
 OR region_name LIKE "%aica%" OR region_name LIKE "%KINGSTON%"OR region_name LIKE "%Kingston%"OR region_name LIKE "%kingston%")
 AND (DATE_DIFF(safe_cast(dt as date),safe_cast(lst_pymt_dt as date),DAY)<=90 OR (dt is null OR lst_pymt_dt is null))
 AND account_status IN('Working','In Default','Promise to pay','TOSd')
 AND account_type ="Residential"
GROUP BY account_id,Month
HAVING MinStartDate>=Month
)
SELECT --Month,account_id
Month,COUNT(DISTINCT account_id) AS GrossAdds
FROM GrossAddsPostpaid
GROUP BY Month ORDER BY Month
