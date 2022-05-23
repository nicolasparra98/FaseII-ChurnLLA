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
,CASE WHEN AvgInc <= 1 THEN "0-1"
      WHEN AvgInc > 1 AND AvgInc <= 2 THEN "01-2" 
      WHEN AvgInc > 2 AND AvgInc <= 3 THEN "02-3"
      WHEN AvgInc > 3 AND AvgInc <= 4 THEN "03-4"
      WHEN AvgInc > 4 AND AvgInc <= 6 THEN "04-6"
      WHEN AvgInc > 6 AND AvgInc <= 8 THEN "06-8"
      WHEN AvgInc > 8 AND AvgInc <= 10 THEN "08-10"
      WHEN AvgInc > 10 THEN "10-"
END AS AvgIncidentsTier
FROM AvgMonthlyIncidents i INNER JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220517_cabletica_mac_address` m
 ON i.MAC=m.MAC
ORDER BY 2,1
)
,Churners AS(
SELECT DISTINCT i.*,CONTRATOCRM
,CASE WHEN i.ACT_ACCT_CD IS NOT NULL THEN 1 ELSE 0 END AS ChurnFlag,MesChurnF
FROM JoinAccounts i RIGHT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-06_ChurnTypeFlagChurners_D` c
 ON i.ACT_ACCT_CD=c.CONTRATOCRM --AND i.Month=c.MesChurnF
--WHERE MesChurnF="2022-01-01"
)
SELECT DISTINCT MesChurnF, ChurnFlag,COUNT(DISTINCT CONTRATOCRM)
FROM Churners
WHERE ChurnFlag=1
GROUP BY 1,2 ORDER BY 1,2
