-- ################################################# Fixed Customers ###############################################################################################
--CREATE TABLE IF NOT EXISTS "db-analytics-dev"."cwp-fixed-dashboardinput" AS
WITH
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d')) as Mes
FROM "lla_cco_int_ext"."cwp_con_ext_base"
WHERE telefonia='Pospago' AND "unidad de negocio"='1. B2C'
 AND DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d'))=DATE('2022-02-01')
)

,FixedUsefulFields AS(
SELECT DISTINCT DT,DATE_TRUNC('MONTH', DATE_ADD('MONTH', -1, DATE(DT))) AS FixedMonth
,ACT_ACCT_CD AS FixedAccount,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(CAST(CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart, FI_TOT_MRC_AMT AS Fixed_MRC
,CASE WHEN (PD_BB_ACCS_MEDIA='FTTH' OR PD_TV_ACCS_MEDIA ='FTTH' OR PD_VO_ACCS_MEDIA='FTTH') THEN 'FTTH'
      WHEN (PD_BB_ACCS_MEDIA='HFC' OR PD_TV_ACCS_MEDIA ='HFC' OR PD_VO_ACCS_MEDIA='HFC') THEN 'HFC'
      WHEN (PD_BB_ACCS_MEDIA='VDSL' OR PD_TV_ACCS_MEDIA ='VDSL' OR PD_VO_ACCS_MEDIA='VDSL' OR
            PD_BB_ACCS_MEDIA='COPPER' OR PD_TV_ACCS_MEDIA ='COPPER' OR PD_VO_ACCS_MEDIA='COPPER') THEN 'COPPER'
      ELSE 'Other' END AS TechFlag
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numVO,
PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm
FROM "db-analytics-prod"."fixed_cwp"
WHERE PD_MIX_CD<>'0P'
GROUP BY DT,2,3,4,FI_OUTST_AGE,8, 7, 9, 10, 11,PD_BB_PROD_CD,pd_tv_prod_cd,PD_VO_PROD_CD,pd_mix_nm
)
,HardBundleFlag AS(
SELECT DISTINCT DT, ACT_ACCT_CD
FROM "db-analytics-prod"."fixed_cwp"
WHERE PD_MIX_CD<>'0P'
AND ((PD_VO_PROD_CD = 1719 AND PD_BB_PROD_CD = 1743) OR
(PD_VO_PROD_CD = 1719 AND PD_BB_PROD_CD = 1744) OR
(PD_VO_PROD_CD = 1718 AND PD_BB_PROD_CD = 1645))
)

,FixedActive_BOM AS(
SELECT f.DT as Fix_B_Date, DATE_TRUNC('MONTH', DATE(f.dt)) AS FixedMonth,
FixedAccount as FixedAccount_BOM, Contacto AS Fixed_B_Phone, FI_OUTST_AGE as B_Overdue, MaxStart as Fixed_B_MaxStart
,CASE WHEN DATE_DIFF('DAY', DATE(f.DT), MaxStart)<=180 THEN 'Early-Tenure'
      WHEN DATE_DIFF('DAY', DATE(f.DT), MaxStart)>180 THEN 'Late-Tenure' END AS B_FixedTenure
,Fixed_MRC as B_Fixed_MRC, TechFlag as B_TechFlag, (numBB+numTV+numVO) as B_NumRGUs
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BO+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BO+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BO+VO+TV'
    END AS B_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN '1P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN '2P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN '3P' END AS B_MixCode_Adj
    ,PD_BB_PROD_CD AS B_bbCode, PD_TV_PROD_CD AS B_tvCode, PD_VO_PROD_CD AS B_voCode,
    CASE WHEN h.ACT_ACCT_CD IS NOT NULL THEN 'Hard FMC'
    ELSE 'TBD' END AS B_Hard_FMC_Flag
    FROM FixedUsefulFields f LEFT JOIN HardBundleFlag h ON f.fixedaccount = h.ACT_ACCT_CD AND f.DT = h.DT
    WHERE f.dt = '2022-02-02' AND (CAST(FI_OUTST_AGE AS INTEGER)<=90 OR FI_OUTST_AGE IS NULL)
)

,FixedActive_EOM AS(
SELECT f.DT as Fix_E_Date, DATE_TRUNC('MONTH', DATE_ADD('MONTH', -1, DATE(f.dt))) AS FixedMonth,
FixedAccount as FixedAccount_EOM, Contacto AS Fixed_E_Phone, FI_OUTST_AGE as E_Overdue, MaxStart as Fixed_E_MaxStart
,CASE WHEN DATE_DIFF('DAY', DATE(f.DT), DATE(MaxStart))<=180 THEN 'Early-Tenure'
      WHEN DATE_DIFF('DAY', DATE(f.DT), DATE(MaxStart))>180 THEN 'Late-Tenure' END AS E_FixedTenure
,Fixed_MRC as E_Fixed_MRC, TechFlag as E_TechFlag, (numBB+numTV+numVO) as E_NumRGUs
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BO+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BO+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BO+VO+TV'
    END AS E_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN '1P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN '2P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN '3P' END AS E_MixCode_Adj
    ,PD_BB_PROD_CD AS E_bbCode, PD_TV_PROD_CD AS E_tvCode, PD_VO_PROD_CD AS E_voCode,
    CASE WHEN h.ACT_ACCT_CD IS NOT NULL THEN 'Hard FMC'
    ELSE 'TBD' END AS E_Hard_FMC_Flag
    FROM FixedUsefulFields f LEFT JOIN HardBundleFlag h ON f.fixedaccount = h.ACT_ACCT_CD AND f.DT = h.DT
    WHERE f.dt= '2022-03-02' AND (CAST(FI_OUTST_AGE AS INTEGER)<=90 OR FI_OUTST_AGE IS NULL)
)

,CustomerStatus AS(
  SELECT DISTINCT
  CASE WHEN (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NOT NULL) OR (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NULL) THEN b.FixedMonth
      WHEN (FixedAccount_BOM IS NULL AND FixedAccount_EOM IS NOT NULL) THEN e.FixedMonth
  END AS FixedMonth,
      CASE WHEN (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NOT NULL) OR (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NULL) THEN FixedAccount_BOM
      WHEN (FixedAccount_BOM IS NULL AND FixedAccount_EOM IS NOT NULL) THEN FixedAccount_EOM
  END AS FixedAccount
  ,CASE WHEN FixedAccount_BOM IS NOT NULL THEN 1 ELSE 0 END AS F_ActiveBOM
  ,CASE WHEN FixedAccount_EOM IS NOT NULL THEN 1 ELSE 0 END AS F_ActiveEOM,
  b.* , e.* 
  FROM FixedActive_BOM b FULL OUTER JOIN FixedActive_EOM e
  ON b.FixedAccount_BOM = e.FixedAccount_EOM AND b.FixedMonth = e.FixedMonth
)

,MainMovementBase AS(
SELECT a.*,
CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN '1.SameRGUs'
WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN '2.Upsell'
WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN '3.Downsell'
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', Fixed_E_MaxStart) = DATE('2022-02-01')) THEN '4.New Customer'
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', Fixed_E_MaxStart) <> DATE('2022-02-01')) THEN '5.Come Back to Life'
WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN '6.Null last day'
WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN '7.Always null'
END AS FixedMainMovement
FROM CustomerStatus a
)
,SpinMovementBase AS(
  SELECT b.*,
  CASE WHEN FixedMainMovement = '1.SameRGUs' AND (E_Fixed_MRC - B_Fixed_MRC) > 0 THEN '1. Up-spin'
  WHEN FixedMainMovement = '1.SameRGUs' AND (E_Fixed_MRC - B_Fixed_MRC) < 0 THEN '2. Down-spin'
  ELSE '3. No Spin' END AS FixedSpinMovement
  FROM MainMovementBase b
)

-- ################ Voluntary Churn ###############################################

,MaxDateVolChurners AS(
SELECT DISTINCT DATE_PARSE(CAST(max(ClosingDate) AS VARCHAR(10)), '%Y%m%d') AS MaxDate,COD_CUENTA
FROM "lla_cco_int_ext"."cwp_fix_ext_voluntarychurn"
GROUP BY COD_CUENTA
)
,ChurnedFixedRGUs AS(
SELECT DISTINCT DATE_TRUNC('MONTH', MaxDate) AS ChurnMonth,CAST(v.COD_CUENTA AS VARCHAR) AS COD_CUENTA,count(*) AS NumChurnedRGUs
FROM "lla_cco_int_ext"."cwp_fix_ext_voluntarychurn" v
 INNER JOIN MaxDateVolChurners m ON v.COD_CUENTA=m.COD_CUENTA AND DATE_PARSE(CAST(ClosingDate AS VARCHAR(10)), '%Y%m%d')=MaxDate
WHERE prod IN('TV','TEL','BB')
GROUP BY 1,v.COD_CUENTA
)
,RGUSLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH', DATE(DT)) AS Month, FixedAccount
,CASE WHEN last_value(pd_mix_nm) over(partition by FixedAccount order by dt) IN ('VO', 'BB', 'TV') THEN 1
      WHEN last_value(pd_mix_nm) over(partition by FixedAccount order by dt) IN ('VO+BB', 'BB+TV', 'VO+TV') THEN 2
      WHEN last_value(pd_mix_nm) over(partition by FixedAccount order by dt) IN ('VO+BB+TV') THEN 3
      ELSE 0 END AS NumRGUsDNA
FROM FixedUsefulFields
WHERE (CAST(fi_outst_age AS INTEGER) <= 90 OR fi_outst_age IS NULL) 
)
,LastRecordDateDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS Month, FixedAccount,max(dt) as LastDate
FROM FixedUsefulFields
GROUP BY 1,FixedAccount
)
,OverdueLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS Month, t.FixedAccount, CAST(fi_outst_age as INT) as LastOverdueRecord
FROM FixedUsefulFields t
INNER JOIN LastRecordDateDNA d ON t.FixedAccount = d.FixedAccount AND t.dt = d.LastDate
)
,VoluntaryChurners AS(
SELECT DISTINCT l.Month, l.FixedAccount AS ChurnAccount, d.LastDate
,CASE WHEN (DATE(d.LastDate) = date_trunc('MONTH', DATE(d.LastDate)) or DATE(d.LastDate) = DATE_TRUNC('MONTH', DATE(d.LastDate)) + interval '1' MONTH - interval '1' day) THEN '1. First/Last Day Churner'
      ELSE '2. Other Date Churner' END AS ChurnDateType
,CASE WHEN LastOverdueRecord >= 90 THEN '2. MixedChurner'
      ELSE '1. Fixed Voluntary Churner' END AS FixedChurnerType
FROM ChurnedFixedRGUs f INNER JOIN RGUSLastRecordDNA l ON f.cod_cuenta = l.FixedAccount
  AND f.NumChurnedRGUs = l.NumRGUsDNA
  AND f.ChurnMonth = l.Month
INNER JOIN LastRecordDateDNA d on f.cod_cuenta = d.FixedAccount AND f.ChurnMonth = d.Month
INNER JOIN OverdueLastRecordDNA o ON f.cod_cuenta = o.FixedAccount AND f.ChurnMonth = o.Month
)
,FinalVoluntaryChurners AS(
    SELECT DISTINCT MONTH, ChurnAccount, FixedChurnerType
    FROM VoluntaryChurners
    WHERE FixedChurnerType = '1. Fixed Voluntary Churner'
)

-- ######################################## Involuntary Churn #########################################################

,FIRSTCUSTRECORD AS (
    SELECT DATE_TRUNC('MONTH', DATE(dt)) AS MES, FixedAccount AS Account, dt AS FirstCustRecord
    FROM FixedUsefulFields 
    WHERE dt='2022-02-02'
)
,LastCustRecord as(
    SELECT  DATE_TRUNC('MONTH', DATE(dt)) AS MES, FixedAccount AS Account, dt as LastCustRecord
    FROM FixedUsefulFields 
    WHERE dt='2022-03-02'
)
 ,NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS MES, FixedAccount AS Account, fi_outst_age
 FROM FixedUsefulFields t
 INNER JOIN FIRSTCUSTRECORD  r ON t.dt = r.FirstCustRecord and r.account = t.FixedAccount
 WHERE CAST(fi_outst_age as INT) <= 90
 GROUP BY 1,2,3
)
 ,OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', DATE_ADD('MONTH', -1, DATE(DT))) AS MES, FixedAccount AS Account, fi_outst_age,
 (date_diff('DAY', DATE(dt), MaxStart)) as ChurnTenureDays
 FROM FixedUsefulFields t
 INNER JOIN LastCustRecord r ON t.dt = r.LastCustRecord and r.account = t.FixedAccount
 WHERE  CAST(fi_outst_age AS INTEGER) >= 90
 GROUP BY 1,2,3,(date_diff('DAY', DATE(dt), MaxStart))
 )
 ,INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,InvoluntaryChurners AS(
SELECT DISTINCT Month, Account AS ChurnAccount, ChurnTenureDays
,CASE WHEN Account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS FixedChurnerType
FROM INVOLUNTARYNETCHURNERS 
GROUP BY 1, Account,ChurnTenureDays
)

,FinalInvoluntaryChurners AS(
    SELECT DISTINCT MONTH, ChurnAccount, FixedChurnerType
    FROM InvoluntaryChurners
    WHERE FixedChurnerType = '2. Fixed Involuntary Churner'
)

-- ####################### All Churners #####################################################################################
,AllChurners AS(
SELECT DISTINCT Month,ChurnAccount,FixedChurnerType
from (SELECT Month,ChurnAccount,FixedChurnerType from FinalVoluntaryChurners a 
      UNION ALL
      SELECT Month,ChurnAccount,FixedChurnerType  from FinalInvoluntaryChurners b)
)
,FixedTable_ChurnFlag AS(
SELECT s.*,
CASE WHEN c.ChurnAccount IS NOT NULL THEN '1. Fixed Churner'
WHEN c.ChurnAccount IS NULL THEN '2. Fixed NonChurner'
END AS FixedChurnFlag,
CASE WHEN c.ChurnAccount is not null then FixedChurnerType END AS FixedChurnType
FROM SpinMovementBase s LEFT JOIN AllChurners c ON s.FixedAccount = c.ChurnAccount and S.FixedMonth = c.Month
)
,Fixed_Convergency AS(
SELECT DISTINCT d.*, c.household_id
 ,CASE WHEN Tipo='1. Inscrito a Paquete completo' OR Tipo='2. Beneficio manual' THEN '1.Soft FMC'
       WHEN Tipo='2. Match_ID' OR Tipo='3. Contact number' THEN '2.Near FMC'
       WHEN E_Hard_FMC_Flag = 'Hard FMC' or B_Hard_FMC_Flag = 'Hard FMC' THEN '3. Hard FMC'
       ELSE '4.Fixed Only' END AS FMCFlagFix
FROM Convergente c RIGHT JOIN FixedTable_ChurnFlag d ON CAST(c.household_id AS TEXT)= CAST(d.FixedAccount AS TEXT)
  AND c.Mes=d.FixedMonth
)

SELECT DISTINCT *
From Fixed_Convergency LIMIT 100
