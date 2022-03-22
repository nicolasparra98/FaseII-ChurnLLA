SELECT DISTINCT country_name, region_name, COUNT(DISTINCT customer_id) as reg
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history` 
WHERE country_name="nan"
 AND (region_name LIKE "%jama%" OR region_name LIKE "%JAMA%" OR region_name LIKE "%Jama%" OR region_name LIKE "%Jami%" 
 OR region_name LIKE "%aica%" OR region_name LIKE "%KINGSTON%"OR region_name LIKE "%Kingston%"OR region_name LIKE "%kingston%")
GROUP BY country_name, region_name
ORDER BY region_name asc
--country_name asc
