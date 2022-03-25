WITH 
Mobile AS(
SELECT  DISTINCT DATE_TRUNC(dt,Month) AS Month,customer_id,account_id,account_name
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential"
GROUP BY 1,2,3,4
)
,DNA AS(
SELECT DISTINCT DATE_TRUNC(PARSE_DATE("%Y%m%d",reporting_date_key),Month) AS Fecha,src_account_id,account_name,lob
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-17_Jamaica_VoluntaryChurners_v2`
WHERE lob="Mobile Postpaid"
GROUP BY 1,2,3,4
)
SELECT DISTINCT *
FROM Mobile m INNER JOIN DNA d ON m.account_id=d.src_account_id AND Fecha=Month
ORDER BY account_id
