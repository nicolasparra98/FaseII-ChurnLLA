WITH

SO_flag AS(
Select distinct 
date_trunc('Month', date(order_start_date)) as month
,cease_reason_code, cease_reason_desc,cease_reason_group
,CASE 
 WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
 WHEN cease_reason_code IN('2','74') THEN 'Involuntario'
 WHEN (cease_reason_code = '9' AND cease_reason_desc='CAMBIO DE TECNOLOGIA') OR (cease_reason_code IN('32','44','55','72')) THEN 'Migracion'
 WHEN cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA' THEN 'Voluntario'
ELSE NULL END AS DxType
,count(distinct(account_id))
from "db-analytics-prod"."so_hdr_cwp" 
where order_type = 'DEACTIVATION'
group by 1,2,3,4,5
order by 5 , 4 desc
)
SELECT *
FROM SO_FLAG
WHERE DxType IS NULL
