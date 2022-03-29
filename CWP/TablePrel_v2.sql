WITH
UsefulFields AS(
SELECT ACT_ACCT_CD, DT,DATE_TRUNC(DT,Month) AS Month, MAX(SAFE_CAST(SAFE_CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart
, FI_TOT_MRC_AMT AS mrc_amt
, PD_BB_PROD_CD, PD_TV_PROD_CD, PD_VO_PROD_CD
    ,CASE WHEN pd_bb_prod_cd IS NOT NULL AND pd_bb_prod_cd <> "" THEN 1 ELSE 0 END AS numBB
    ,CASE WHEN pd_tv_prod_cd IS NOT NULL AND pd_tv_prod_cd <> "" THEN 1 ELSE 0 END AS numTV
    ,CASE WHEN pd_vo_prod_cd IS NOT NULL AND pd_vo_prod_cd <> "" THEN 1 ELSE 0 END AS numVO
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE ACT_CUST_TYP_NM="Residencial" AND PD_MIX_CD<>"0P"
GROUP BY ACT_ACCT_CD, DT,numBB,numTV,numVO,mrc_amt, PD_BB_PROD_CD, PD_TV_PROD_CD, PD_VO_PROD_CD
)
,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, act_acct_cd, avg(mrc_amt) AS AvgMRC
  FROM UsefulFields 
  WHERE mrc_amt IS NOT NULL AND mrc_amt <> 0
  GROUP BY Month, act_acct_cd
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(DT),Month) AS Month, u.ACT_ACCT_CD AS accountBOM,dt,(NumBB+NumTV+NumVO) as B_NumRGUs
  ,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN "BO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN "TV"
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN "VO"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN "BO+TV"
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN "BO+VO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN "VO+TV"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN "BO+VO+TV"
    END AS B_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS B_MixCode_Adj
,MaxStart as B_MaxStart, mrc_amt AS B_MRC, AvgMRC AS B_Avg_MRC
,PD_BB_PROD_CD AS B_bbCode, PD_TV_PROD_CD AS B_tvCode, PD_VO_PROD_CD AS B_voCode
FROM UsefulFields u LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
WHERE dt="2022-02-02"
GROUP BY 1,2,3,B_MixName_Adj,B_MixCode_Adj,B_NumRGUs,B_MaxStart,B_MRC,B_Avg_MRC,B_bbCode,B_tvCode,B_voCode
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(DT),MONTH) AS Month, u.ACT_ACCT_CD AS accountEOM,dt,(NumBB+NumTV+NumVO) as E_NumRGUs
  ,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN "BO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN "TV"
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN "VO"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN "BO+TV"
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN "BO+VO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN "VO+TV"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN "BO+VO+TV"
    END AS E_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS E_MixCode_Adj
,MaxStart as E_MaxStart, MRC_AMT AS E_MRC, AvgMRC as E_Avg_MRC
,PD_BB_PROD_CD AS E_bbCode, PD_TV_PROD_CD AS E_tvCode, PD_VO_PROD_CD AS E_voCode
FROM UsefulFields u LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
WHERE dt=LAST_DAY(dt,Month)
GROUP BY 1,2,3,E_MixName_Adj,E_MixCode_Adj,E_NumRGUs,E_MaxStart,E_MRC,E_Avg_MRC,E_bbCode,E_tvCode,E_voCode
)
,CustomerStatus AS(
  SELECT DISTINCT
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS account,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  B_MixName_Adj,B_MixCode_Adj,E_MixName_Adj,E_MixCode_Adj,B_NumRGUs,E_NumRGUs,B_MaxStart,E_MaxStart,B_MRC,E_MRC,B_Avg_MRC,E_Avg_MRC
  ,B_bbCode,B_tvCode,B_voCode,E_bbCode,E_tvCode,E_voCode
  FROM ActiveUsersBOM b FULL OUTER JOIN ActiveUsersEOM e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH
)
,MainMovementBase AS(
SELECT a.*,
CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN "1.SameRGUs" 
WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN "2.Upsell"
WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN "3.Downsell"
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (E_MaxStart, MONTH) = '2022-02-01') THEN "4.New Customer"
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (E_MaxStart, MONTH) <> '2022-02-01') THEN "5.Come Back to Life"
WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN "6.Null last day"
WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN "7.Always null"
END AS MainMovement
FROM CustomerStatus a
)
,SpinMovementBase AS(
  SELECT b.*,
  CASE WHEN MainMovement = "1.SameRGUs" AND (E_MRC - B_MRC) > 0 THEN "1. Up-spin"
  WHEN MainMovement = "1.SameRGUs" AND (E_MRC - B_MRC) < 0 THEN "2. Down-spin"
  ELSE "3. No Spin" END AS SpinMovement
  FROM MainMovementBase b
)

--SELECT * FROM SpinMovementBase

--/*
SELECT DISTINCT Month, MainMovement,SpinMovement
,COUNT(DISTINCT Account) AS Records
,ROUND(SUM(B_Avg_MRC),2) AS RevenueBOM
,ROUND(SUM(E_Avg_MRC),2) AS RevenueEOM
,ROUND(SUM(B_Avg_MRC)/COUNT(DISTINCT Account),2) AS ARPU_BOM
,ROUND(SUM(E_Avg_MRC)/COUNT(DISTINCT Account),2) AS ARPU_EOM
FROM SpinMovementBase
GROUP BY Month,2,3
ORDER BY Month,MainMovement,SpinMovement
--*/

/*
SELECT Month, MainMovement--,SpinMovement
,COUNT(E_bbCode) AS BB, COUNT(E_tvCode) AS TV, COUNT(E_voCode) AS VO
FROM SpinMovementBase 
GROUP BY Month, MainMovement--,SpinMovement
ORDER BY Month,MainMovement
*/
