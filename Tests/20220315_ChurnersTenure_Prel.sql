WITH 

CustomersBegFebruary AS (
SELECT DISTINCT act_acct_cd, min(act_acct_inst_dt) AS MinStartDate,load_dt
,DATE_TRUNC(load_dt, MONTH) AS February
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-01'
AND (fi_outst_age <90 OR fi_outst_age is null)
GROUP BY act_acct_cd,February,load_dt
)
, CustomersBegFebruaryAdj AS (
SELECT DISTINCT act_acct_cd,February, DATE_DIFF(load_dt,MinStartDate,DAY) AS Tenure
FROM CustomersBegFebruary 
WHERE MinStartDate<'2022-02-01'
GROUP BY act_acct_cd,February, Tenure
)
, CustomersEndFebruary AS (
SELECT DISTINCT act_acct_cd,DATE_TRUNC(load_dt, MONTH) AS February
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-28'
 AND (fi_outst_age <90 OR fi_outst_age is null)
GROUP BY act_acct_cd,February
)
, GrossAddsFebruary AS (
SELECT DISTINCT act_acct_cd, min(act_acct_inst_dt) AS MinStartDate,DATE_TRUNC(load_dt, MONTH) AS February, load_dt
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE org_cntry="Jamaica" AND load_dt='2022-02-28'
 AND (fi_outst_age <90 OR fi_outst_age is null)
GROUP BY act_acct_cd, February, load_dt
HAVING MinStartDate>='2022-02-01'
)
, GrossAddsFebruaryTenure AS(
SELECT act_acct_cd, February,DATE_DIFF(load_dt,MinStartDate,DAY) AS Tenure
FROM GrossAddsFebruary
GROUP BY act_acct_cd, February,Tenure
)
, SumBegFebruaryAndGrossAdds AS(
SELECT DISTINCT act_acct_cd, Tenure
from (SELECT act_acct_cd,Tenure from CustomersBegFebruaryAdj a 
      UNION ALL
      SELECT act_acct_cd,Tenure from GrossAddsFebruaryTenure b)
)
, Churners AS (
SELECT DISTINCT s.act_acct_cd AS ContratosChurners, e.act_acct_cd, s.Tenure
FROM SumBegFebruaryAndGrossAdds s LEFT JOIN CustomersEndFebruary e ON s.act_acct_cd=e.act_acct_cd
WHERE e.act_acct_cd IS NULL
GROUP BY s.act_acct_cd, e.act_acct_cd,s.Tenure
ORDER BY e.act_acct_cd
)
SELECT DISTINCT ContratosChurners,Tenure FROM Churners
