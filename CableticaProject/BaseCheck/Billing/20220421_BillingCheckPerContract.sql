SELECT DISTINCT FECHA_EXTRACCION,DATE_TRUNC(FECHA_EXTRACCION,MONTH) AS MONTH, ACT_ACCT_CD,CST_CUST_NAME
,ROUND(VO_FI_TOT_MRC_AMT,2) AS VO_MRC,ROUND(VO_FI_TOT_MRC_AMT_DESC,2) AS VO_DESC
,ROUND(BB_FI_TOT_MRC_AMT,2) AS BB_MRC,ROUND(BB_FI_TOT_MRC_AMT_DESC,2) AS BB_DESC
,ROUND(TV_FI_TOT_MRC_AMT,2) AS TV_MRC,ROUND(TV_FI_TOT_MRC_AMT_DESC,2) AS TV_DESC
,ROUND(TOT_BILL_AMT,2) AS TOT_MRC,ROUND(TOT_DESC_AMT,2) AS TOT_DESC
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` 
WHERE FECHA_EXTRACCION IN ('2021-01-01','2021-02-01','2021-03-01','2021-04-01')
--('2021-10-01','2021-11-01','2021-12-01')
--('2021-08-01','2021-09-01','2021-10-01')
--('2021-05-01','2021-06-01','2021-07-01')
--('2022-01-01','2022-02-01','2022-03-01')
 AND ACT_ACCT_CD=667379 
ORDER BY 1
