WITH
UsefulFields AS(
SELECT ACT_ACCT_CD, DT,DATE_TRUNC(DT,Month) AS Month, MAX(SAFE_CAST(SAFE_CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart
, FI_TOT_MRC_AMT AS mrc_amt
, PD_BB_PROD_CD, PD_TV_PROD_CD, PD_VO_PROD_CD
    ,CASE WHEN pd_bb_prod_cd IS NOT NULL AND pd_bb_prod_cd <> "" THEN 1 ELSE 0 END AS numBB
    ,CASE WHEN pd_tv_prod_cd IS NOT NULL AND pd_tv_prod_cd <> "" THEN 1 ELSE 0 END AS numTV
    ,CASE WHEN pd_vo_prod_cd IS NOT NULL AND pd_vo_prod_cd <> "" THEN 1 ELSE 0 END AS numVO
    ,CASE WHEN (PD_BB_ACCS_MEDIA="FTTH" OR PD_TV_ACCS_MEDIA ="FTTH" OR PD_VO_ACCS_MEDIA="FTTH") THEN "FTTH"
      WHEN (PD_BB_ACCS_MEDIA="HFC" OR PD_TV_ACCS_MEDIA ="HFC" OR PD_VO_ACCS_MEDIA="HFC") THEN "HFC"
      WHEN (PD_BB_ACCS_MEDIA="VDSL" OR PD_TV_ACCS_MEDIA ="VDSL" OR PD_VO_ACCS_MEDIA="VDSL" OR 
            PD_BB_ACCS_MEDIA="COPPER" OR PD_TV_ACCS_MEDIA ="COPPER" OR PD_VO_ACCS_MEDIA="COPPER") THEN "COPPER"
      ELSE "Other" END AS TechFlag
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
WHERE PD_MIX_CD<>"0P" AND (SAFE_CAST(FI_OUTST_AGE AS INT64)<=90 OR FI_OUTST_AGE IS NULL)
GROUP BY ACT_ACCT_CD, DT,numBB,numTV,numVO,mrc_amt, PD_BB_PROD_CD, PD_TV_PROD_CD, PD_VO_PROD_CD,TechFlag
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
,MaxStart as B_MaxStart, mrc_amt AS B_MRC, AvgMRC AS B_Avg_MRC, TechFlag AS B_TechFlag
,PD_BB_PROD_CD AS B_bbCode, PD_TV_PROD_CD AS B_tvCode, PD_VO_PROD_CD AS B_voCode
FROM UsefulFields u LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
WHERE dt="2022-02-02"
GROUP BY 1,2,3,B_MixName_Adj,B_MixCode_Adj,B_NumRGUs,B_MaxStart,B_MRC,B_Avg_MRC,B_bbCode,B_tvCode,B_voCode,B_TechFlag
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_SUB(DATE(DT),INTERVAL 1 MONTH),MONTH) AS Month, u.ACT_ACCT_CD AS accountEOM,dt,(NumBB+NumTV+NumVO) as E_NumRGUs
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
,MaxStart as E_MaxStart, MRC_AMT AS E_MRC, AvgMRC as E_Avg_MRC, TechFlag AS E_TechFlag
,PD_BB_PROD_CD AS E_bbCode, PD_TV_PROD_CD AS E_tvCode, PD_VO_PROD_CD AS E_voCode
FROM UsefulFields u LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
WHERE dt='2022-03-02'
GROUP BY 1,2,3,E_MixName_Adj,E_MixCode_Adj,E_NumRGUs,E_MaxStart,E_MRC,E_Avg_MRC,E_bbCode,E_tvCode,E_voCode,E_TechFlag
)
,CustomerStatus AS(
  SELECT DISTINCT
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS account
  ,CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM
  ,CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM
  ,B_MixName_Adj,B_MixCode_Adj,B_NumRGUs,B_MaxStart,B_MRC,B_Avg_MRC,B_bbCode,B_tvCode,B_voCode,B_TechFlag
  ,E_MixName_Adj,E_MixCode_Adj,E_NumRGUs,E_MaxStart,E_MRC,E_Avg_MRC,E_bbCode,E_tvCode,E_voCode,E_TechFlag
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
,FlagFMCBase AS(
SELECT s.*,f.FlagFMC
FROM SpinMovementBase s LEFT JOIN `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-01_CWP_FlagFMC` f 
 ON s.ACCOUNT=f.ACT_ACCT_CD AND s.Month=f.Month
)
,SegmentFMCBase AS(
SELECT s.*
,CASE WHEN FlagFMC=1 AND B_MixCode_Adj="1P" THEN "P2"
      WHEN FlagFMC=1 AND B_MixCode_Adj="2P" THEN "P3"
      WHEN FlagFMC=1 AND B_MixCode_Adj="3P" THEN "P4"
      WHEN FlagFMC=0 THEN "P1Fixed"
      ELSE "Null" END AS B_SegmentFlag
,CASE WHEN FlagFMC=1 AND E_MixCode_Adj="1P" THEN "P2"
      WHEN FlagFMC=1 AND E_MixCode_Adj="2P" THEN "P3"
      WHEN FlagFMC=1 AND E_MixCode_Adj="3P" THEN "P4"
      WHEN FlagFMC=0 THEN "P1Fixed"
      ELSE "Null" END AS E_SegmentFlag
FROM FlagFMCBase s
)

SELECT Month,E_SegmentFlag,COUNT(DISTINCT Account) AS Records
FROM SegmentFMCBase
WHERE ActiveEOM=1
GROUP BY Month,E_SegmentFlag

