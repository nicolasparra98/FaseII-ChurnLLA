WITH
AtypicalFlag AS(
select distinct *,DATE(DATE_TRUNC(UPDATED,DAY)) AS Day,DATE(DATE_TRUNC(UPDATED,Month)) AS Month
,CASE WHEN ((prev_SYSUPTIME > SYSUPTIME) OR (SYSUPTIME IS NULL AND STATUS_VALUE="online")) THEN 1 ELSE 0 END AS Atypical
from 
  (SELECT *,
  lag(SYSUPTIME) over (partition by MAC order by UPDATED) as prev_SYSUPTIME,
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220516_SERVASSURE_SYSUPTIME_2021-07_A_2022-05` 
  order by MAC, UPDATED)
)
,DailyIncidents AS(
SELECT DISTINCT Month,Day,MAC
,SUM(ATYPICAL) OVER (PARTITION BY MAC,DAY) AS Incidents
FROM AtypicalFlag
ORDER BY MAC,DAY
)
,AvgMonthlyIncidents AS(
SELECT DISTINCT Month,MAC,ROUND(AVG(Incidents),2) AS AvgInc,MAX(Incidents) AS MaxInc,MIN(Incidents) AS MinInc
FROM DailyIncidents
GROUP BY 1,2
ORDER BY AvgInc desc
)
,JoinAccounts AS(
SELECT DISTINCT i.*,RIGHT(CONCAT('0000000000',Contrato),10) AS ACT_ACCT_CD
FROM AvgMonthlyIncidents i INNER JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220517_cabletica_mac_address` m
 ON i.MAC=m.MAC
ORDER BY 2,1
)
,DnaCheck AS(
SELECT DISTINCT i.* --,CONTRATOCRM
--,CASE WHEN c.CONTRATOCRM IS NOT NULL THEN 1 ELSE 0 END AS ChurnFlag,MesChurnF
FROM JoinAccounts i INNER JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`  c
 ON i.ACT_ACCT_CD=RIGHT(CONCAT('0000000000',c.act_acct_cd),10) AND i.Month=DATE_TRUNC(c.FECHA_EXTRACCION,MONTH)
--WHERE MesChurnF="2021-11-01"
)
SELECT DISTINCT Month,COUNT(DISTINCT ACT_ACCT_CD)
FROM DNACheck
GROUP BY 1 ORDER BY 1
