CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cwc_fix_stg_weekly_churn_input_mayjune_prueba" AS

WITH weekly_dates as(
SELECT distinct date_trunc('Month', DATE(dt)) as Month, 
case when date_trunc('Month', date_trunc('week', DATE(dt))) = date_trunc('Month', DATE(dt))
and date(dt) <= date_trunc('Month', DATE(dt)) + interval '1' month - interval '1' day
then  date_trunc('week', DATE(dt)) 
else date_trunc('Month', DATE(dt)) - interval '1' day end as BOW,
case when date_trunc('Month', date_trunc('Week', DATE(dt))+ interval '7' day) = date_trunc('Month', DATE(dt)) then  date_trunc('Week', DATE(dt))+ interval '7' day
else date_trunc('Month', DATE(dt)) + interval '1' month - interval '1' day end as EOW
FROM "db-analytics-prod"."tbl_fixed_cwc" 
WHERE DATE_TRUNC('YEAR', DATE(DT)) = date('2022-01-01')
--order by 1,2,3
)

,weeks_per_month as(
select *,
row_number() over (partition by month order by EOW asc) as month_week,
(Month - interval '1' day) as BOM, (Month + interval '1' month - interval '1' day) as EOM
from weekly_dates
where bow <> eow
--order by month, 4, bow, eow
)

,UsefulFields AS(
  SELECT
    DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, Max(dt) as MaxDateMonth,
    dt,act_acct_cd, act_contact_phone_1, act_contact_phone_2, act_contact_phone_3,
    pd_mix_cd,pd_mix_nm,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,
   CASE WHEN IS_NAN (cast(fi_tot_mrc_amt AS double)) THEN 0
    WHEN NOT IS_NAN (cast(fi_tot_mrc_amt AS double)) THEN ROUND((cast(fi_tot_mrc_amt AS double)),0)
    END AS mrc_amt,
    CASE WHEN fi_outst_age IS NULL THEN -1 ELSE cast(fi_outst_age as double) end as fi_outst_age
    , fi_tot_srv_chrg_amt, ROUND(cast(fi_bb_mrc_amt as double),0) as fi_bb_mrc_amt, ROUND(cast(fi_tv_mrc_amt as double),0) as fi_tv_mrc_amt, ROUND(cast(fi_vo_mrc_amt as double),0) as fi_vo_mrc_amt,
    MAX(act_cust_strt_dt) AS MaxStart, bundle_code, bundle_name,
    CASE WHEN (pd_mix_nm like '%BO%') THEN 1 ELSE 0 END AS numBB,
   CASE WHEN (pd_mix_nm like '%TV%') THEN 1 ELSE 0 END AS numTV,
   CASE WHEN (pd_mix_nm like '%VO%') THEN 1 ELSE 0 END AS numVO,
  CASE WHEN length(cast(act_acct_cd as varchar))=8 then 'HFC' 
            WHEN NR_FDP<>'' and NR_FDP<>' ' and NR_FDP is not null THEN 'FTTH' 
            WHEN pd_vo_tech='FIBER' THEN 'FTTH' 
            WHEN (pd_bb_prod_nm like '%GPON%'  OR pd_bb_prod_nm like '%FTT%') and 
            (pd_bb_prod_nm not like '%ADSL%' and pd_bb_prod_nm not like '%VDSL%') THEN 'FTTH' 
            ELSE 'COPPER' END AS Techonology_type,
  cst_cust_cd
  FROM "db-analytics-prod"."tbl_fixed_cwc" 
  WHERE
    org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard')
    AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
    AND DATE_TRUNC('YEAR', DATE(DT)) = date('2022-01-01')
    -- El mes toca variabilizarlo (mes de reporte)
  GROUP BY
    1,dt,act_acct_cd,
    pd_mix_cd,pd_mix_nm,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,
    13 ,fi_outst_age, fi_tot_srv_chrg_amt, 14, 15, 16, 17,
     18, 20, 21, 22, 23, 24,25, act_contact_phone_1, act_contact_phone_2, act_contact_phone_3, cst_cust_cd
)

,LastDayRGUs AS(
  SELECT date_trunc('Month', date(dt)) as Month, act_acct_cd, first_value (numBB + numTV + numVO) over (partition by act_acct_cd, date_trunc('Month', date(dt)) order by dt desc) as last_rgus, first_value(dt) over (partition by act_acct_cd order by dt desc) as last_date
  FROM UsefulFields
  )

, ActiveUsersBOW as(

 SELECT DISTINCT w.Month, w.month_week, w.BOW, u.act_acct_cd AS accountBOM, act_contact_phone_1 as PhoneBOM1, act_contact_phone_2 as PhoneBOM2, act_contact_phone_3 as PhoneBOM3,
     u.dt as B_Date,pd_mix_cd as B_MixCode ,pd_mix_nm as B_MixName ,pd_bb_prod_nm as B_ProdBBName,pd_tv_prod_nm as B_ProdTVName,pd_vo_prod_nm as B_ProdVoName,
    (NumBB+NumTV+NumVO) as B_NumRGUs, 
    CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BO+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BO+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BO+VO+TV'
    END AS B_MixName_Adj,
    CASE WHEN NumBB = 1 THEN u.act_acct_cd ELSE NULL END As BB_RGU_BOM,
    CASE WHEN NumTV = 1 THEN u.act_acct_cd ELSE NULL END As TV_RGU_BOM,
    CASE WHEN NumVO = 1 THEN u.act_acct_cd ELSE NULL END As VO_RGU_BOM,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN '1P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN '2P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN '3P' END AS B_MixCode_Adj,
    mrc_amt as B_MRC ,fi_outst_age  as B_OutstAge, fi_tot_srv_chrg_amt as B_MRCAdj, fi_bb_mrc_amt as B_MRCBB, fi_tv_mrc_amt as B_MRCTV, fi_vo_mrc_amt as B_MRCVO,
    MaxStart as B_MaxStart, Techonology_type as B_Tech_Type, bundle_code as B_bundlecode, bundle_name as B_bundlename,
    --AvgMRC as B_Avg_MRC,min(last_rgus) as last_rgus, last_date,
    case when cast(fi_outst_age as double) <= 90 or fi_outst_age is null then 'Active' 
    else 'DRC' end as b_active_flag,
    cst_cust_cd as b_cust_cd, last_rgus
    
 FROM
    UsefulFields u inner join weeks_per_month w on date(u.dt) = w.BOW
    LEFT JOIN LASTDAYRGUs l ON u.act_acct_cd = l.act_acct_cd and u.Month = l.Month
  WHERE
    DATE(u.dt) = w.BOW
    --DATE(u.dt) = LAST_DAY(u.dt, MONTH)
    --AND ((AvgMRC IS NOT NULL AND AvgMRC <> 0 AND date_diff('day', DATE(MaxStart), DATE(MaxDateMonth))>60) OR  (date_diff('day',DATE(MaxStart), DATE(MaxDateMonth)) <=60))
  GROUP BY
    1, 2, 3, 4, 5,6,7 ,8, 9,10,11, 
    15 ,16, 17, 18, 19, 20, 21, 22, 12, 21, 22, 23, 13, 14, 24, 25,26,27,28,29,30,31,32
)

, ActiveUsersEOW as(

SELECT
    DISTINCT w.Month, w.month_week, w.EOW, u.act_acct_cd AS accountEOM, act_contact_phone_1 as PhoneEOM1, act_contact_phone_2 as PhoneEOM2, act_contact_phone_3 as PhoneEOM3,
    u.dt as E_Date,pd_mix_cd as E_MixCode ,pd_mix_nm as E_MixName ,pd_bb_prod_nm as E_ProdBBName,pd_tv_prod_nm as E_ProdTVName,pd_vo_prod_nm as E_ProdVoName,
     (NumBB+NumTV+NumVO) as E_NumRGUs,
     CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BO+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BO+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BO+VO+TV'
    END AS E_MixName_Adj,
        CASE WHEN NumBB = 1 THEN u.act_acct_cd ELSE NULL END As BB_RGU_EOM,
    CASE WHEN NumTV = 1 THEN u.act_acct_cd ELSE NULL END As TV_RGU_EOM,
    CASE WHEN NumVO = 1 THEN u.act_acct_cd ELSE NULL END As VO_RGU_EOM,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN '1P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN '2P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN '3P' END AS E_MixCode_Adj,
     mrc_amt as E_MRC ,fi_outst_age  as E_OutstAge, fi_tot_srv_chrg_amt as E_MRCAdj, fi_bb_mrc_amt as E_MRCBB, fi_tv_mrc_amt as E_MRCTV, fi_vo_mrc_amt as E_MRCVO,
    MaxStart as E_MaxStart, Techonology_type as E_Tech_TypE, bundle_code as E_bundlecode, bundle_name as E_bundlename
    --, AvgMRC as E_Avg_MRC
    , case when cast(fi_outst_age as double) <= 90 or fi_outst_age is null then 'Active' 
    else 'DRC' end as e_active_flag,
    cst_cust_cd as e_cust_cd
 FROM
    UsefulFields u inner join weeks_per_month w on date(u.dt) = w.EOW
  WHERE
   -- (cast(fi_outst_age AS double) <= 90
     -- OR fi_outst_age IS NULL)
    --AND 
    DATE(u.dt) = w.EOW
    --AND ((AvgMRC IS NOT NULL AND AvgMRC <> 0 AND date_diff('day',  DATE(MaxStart), DATE(MaxDateMonth))>60) OR  (date_diff('day',  DATE(MaxStart), DATE(MaxDateMonth))<=60))
  GROUP BY
    1, 2, 3, 4, 5, 6,7 ,8, 9,10,11, 
    15 ,16, 17,18, 19, 20, 21, 22, 12, 22, 23, 24, 13, 14,25,26,27,28,29,30,31
)

, CUSTOMERBASE AS(
  SELECT DISTINCT
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Fixed_Month,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.month_week
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.month_week
  END AS Fixed_MonthWeek,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS Fixed_Account,
    CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN B_cust_cd
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN E_cust_cd
  END AS Fixed_Cust_Cd,
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM1
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM1
  END AS f_contactphone1,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM2
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM2
  END AS f_contactphone2,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM3
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM3
  END AS f_contactphone3,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  b_active_flag, e_active_flag,
  BOW,
  B_Date,B_Tech_Type, B_MixCode, B_MixCode_Adj, B_MixName, B_MixName_Adj,  B_ProdBBName,B_ProdTVName,B_ProdVoName, BB_RGU_BOM, TV_RGU_BOM, VO_RGU_BOM,B_NumRGUs,B_bundlecode, B_bundlename,
  B_MRC ,B_OutstAge, B_MRCAdj, B_MRCBB, B_MRCTV, B_MRCVO
  --, B_Avg_MRC
  , B_MaxStart, DATE_DIFF('day', DATE(B_MaxStart),DATE(B_Date)) as B_TenureDays,
  CASE WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) <= 180 Then 'Early-Tenure'
  WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) > 180 THEN 'Late-Tenure' END AS B_FixedTenureSegment,
  EOW,E_Date,E_Tech_Type, E_MixCode, E_MixCode_Adj ,E_MixName, E_MixName_Adj ,E_ProdBBName,E_ProdTVName,E_ProdVoName,BB_RGU_EOM, TV_RGU_EOM, VO_RGU_EOM, E_NumRGUs, E_bundlecode, E_bundlename,
  E_MRC ,E_OutstAge, E_MRCAdj, E_MRCBB, E_MRCTV, E_MRCVO
  --, E_Avg_MRC
  , E_MaxStart, DATE_DIFF('day', DATE(E_MaxStart),  DATE(E_Date)) as E_TenureDays,
  CASE WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) <= 180 Then 'Early-Tenure'
  WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) > 180 THEN 'Late-Tenure' END AS E_FixedTenureSegment, 
  (E_MRC - B_MRC) as MRCDiff,last_rgus--, last_date
  , (coalesce(B_NumRGUs,0) - coalesce(E_NumRGUs,0)) as Dif_RGUs
  FROM ActiveUsersBOW b FULL OUTER JOIN ActiveUsersEOW e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH and b.month_week = e.month_week
 -- ORDER BY Fixed_Account
)

------------------------- Gross ads, upsells and downsells ----------------------------------------
,MAINMOVEMENTBASE AS(
SELECT a.*,
CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN '1.SameRGUs' 
WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN '2.Upsell'
WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN '3.Downsell'
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', DATE(E_MaxStart)) = DATE('2022-06-01')) THEN '4.New Customer'
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', DATE(E_MaxStart)) <> DATE('2022-06-01')) THEN '5.Come Back to Life'
WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN '6.Null last day'
WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN '7.Always null'
END AS MainMovement
FROM CUSTOMERBASE a
)

--################################# FIXED CHURN FLAGS --###############################################################

,panel_so as (
    select account_id, order_id,
    case when max(lob_vo_count)> 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_vo_count, 
    case when max(lob_bb_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_bb_count, 
    case when max(lob_tv_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_tv_count, 
   -- case when max(lob_other_count) > 0 then 1 else 0 end  as vol_lob_other_count,
    --DATE_TRUNC('month',  order_start_date) as completed_month,
    DATE_TRUNC('month', completed_date) as completed_month, completed_date,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type,
    lob_VO_count, lob_BB_count, lob_TV_count, customer_id
    from (
        select * FROM "db-stage-dev"."so_hdr_cwc"
    WHERE org_cntry = 'Jamaica'
        AND (cease_reason_group in ('Voluntary', 'Customer Service Transaction', 'Involuntary') or cease_reason_group is null)
        AND (network_type NOT IN ('LTE','MOBILE') or network_type is null)
        AND order_status = 'COMPLETED' AND account_type = 'Residential')
        --AND order_type = 'DEACTIVATION'
        --AND DATE_TRUNC('month', completed_date) = ( select month_analysis from parameters))
    group by account_id, order_id, lob_vo_count, lob_bb_count, lob_tv_count, DATE_TRUNC('month', completed_date), completed_date, customer_id,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type
    --order by completed_month, account_id, order_id
    )

------------------------ Main  Churn Flags ---------------------------------------------------

-- Voluntary churners base
,VOLCHURNERS_SO AS (
SELECT *,
CASE WHEN lob_vo_count > 0 THEN 1 ELSE 0 END AS VO_Churn,
CASE WHEN lob_BB_count > 0 THEN 1 ELSE 0 END AS BB_Churn,
CASE WHEN lob_TV_count > 0 THEN 1 ELSE 0 END AS TV_Churn
FROM panel_so
WHERE
    org_cntry = 'Jamaica' 
    AND cease_reason_group in ('Voluntary')
    AND network_type NOT IN ('LTE','MOBILE') AND order_status = 'COMPLETED'
    AND account_type = 'Residential'
)
-- Number of churned RGUs on the maximum date - it doesn't consider mobile
,ChurnedRGUS_SO AS(
 SELECT *,
 (VO_CHURN + BB_CHURN + TV_CHURN) AS ChurnedRGUs
 FROM VOLCHURNERS_SO
)
-- Number of RGUs a customer has on the last record of the month
,RGUSLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, act_acct_cd, cst_cust_cd,
CASE WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC('MONTH',DATE(dt)) order by dt) IN ('VO', 'BO', 'TV') THEN 1
WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC('MONTH',DATE(dt)) order by dt) IN ('BO+VO', 'BO+TV', 'VO+TV') THEN 2
WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC('MONTH',DATE(dt)) order by dt) IN ('BO+VO+TV') THEN 3
ELSE 0 END AS NumRgusLastRecord
FROM Usefulfields
WHERE (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
 --ORDER BY act_acct_cd
),
-- Date of the last record of the month per customer
LastRecordDateDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, act_acct_cd,max(dt) as LastDate, cst_cust_cd
FROM Usefulfields
WHERE  (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
 GROUP BY 1, act_acct_cd, cst_cust_cd
 --ORDER BY act_acct_cd
),
-- Number of outstanding days on the last record date
OverdueLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, t.act_acct_cd, fi_outst_age as LastOverdueRecord, t.cst_cust_cd,
(date_diff('day', DATE(MaxStart), DATE(dt))) as ChurnTenureDays
FROM Usefulfields t 
INNER JOIN LastRecordDateDNA d ON t.act_acct_cd = d.act_acct_cd AND t.dt = d.LastDate
),
-- Total Voluntary Churners considering number of churned RGUs, outstanding age and churn date
VoluntaryTotalChurners AS(
SELECT distinct l.Month, l.act_acct_cd, d.LastDate, o.ChurnTenureDays, v.completed_date as churndate
,CASE WHEN length(cast(l.act_acct_cd AS varchar)) = 12 THEN '1. Liberate'
ELSE '2. Cerilion' END AS BillingSystem,
CASE WHEN (DATE(d.LastDate) = date_trunc('Month', DATE(d.LastDate)) or DATE(d.LastDate) = date_trunc('MONTH', DATE(d.LastDate)) + interval '1' MONTH - interval '1' day) THEN '1. First/Last Day Churner'
ELSE '2. Other Date Churner' END AS ChurnDateType,
CASE WHEN cast(LastOverdueRecord as double) >= 90 THEN '2.Fixed Mixed Churner'
ELSE '1.Fixed Voluntary Churner' END AS ChurnerType
FROM CHURNEDRGUS_SO v INNER JOIN RGUSLastRecordDNA l ON cast(v.customer_id as double) = cast(l.cst_cust_cd as double)
AND v.ChurnedRGUs >= l.NumRgusLastRecord 
AND DATE_TRUNC('month', v.completed_date) = l.Month
INNER JOIN LastRecordDateDNA d on cast(l.act_acct_cd as double)= cast(d.act_acct_cd as double) AND l.Month = d.Month
INNER JOIN OverdueLastRecordDNA o ON cast(l.act_acct_cd as double) = cast(o.act_acct_cd as double) AND l.month = o.Month
WHERE cease_reason_group = 'Voluntary'
)

,VoluntaryChurners AS(
SELECT Month, cast(act_acct_cd AS varchar) AS Account, ChurnerType, ChurnTenureDays, ChurnDate
FROM VoluntaryTotalChurners 
WHERE ChurnerType='1.Fixed Voluntary Churner'
GROUP BY Month, act_acct_cd, ChurnerType, ChurnTenureDays, ChurnDate
)

,WeeklyVoluntaryChurners AS(

SELECT v.Month, f.Fixed_MonthWeek, f.BOW, f.EOW, v.account, v.churnertype, date(v.Churndate) as churndate
FROM voluntarychurners v
INNER JOIN customerbase f on v.account = f.fixed_account and v.Month = f.Fixed_Month
where date(v.ChurnDate) >= f.BOW and  (date(v.churnDate) <f.EOW or f.EOW is null)
)

,OverdueCustomers AS(
SELECT month, act_acct_cd, fi_outst_age, dt
FROM UsefulFields
WHERE fi_outst_age = 90
)

,WeeklyInvoluntaryChurners as(

SELECT f.Fixed_month as Month, f.fixed_monthweek, f.BOW, f.EOW, f.fixed_account as account,
'2.Fixed Involuntary Churner' as churnertype, date(o.dt) as churndate
FROM CustomerBase f INNER JOIN  overduecustomers o on f.fixed_account = o.act_acct_cd 
AND f.fixed_month = o.month 
where b_active_flag = 'Active' and date(dt) > f.BOW and (date(dt) <= f.EOW or EOW is null)
)

,AllChurners as(

Select Month, fixed_monthweek, account, churnertype
From 
( SELECT * FROM WeeklyVoluntaryChurners
UNION ALL 
SELECT * from WeeklyInvoluntaryChurners)
)

,WeeklyCustomerBaseChurners AS(

SELECT m.*,
churnertype as Fixed_ChurnType
FROM mainmovementbase m LEFT JOIN AllChurners a on m.fixed_account = a.account
and m.Fixed_Month = a.month and m.fixed_monthweek = a.fixed_monthweek

)

,WeeklyEarlyRejoiners as(

SELECT f.Fixed_month as Month, f.fixed_monthweek, f.BOW, f.EOW, f.fixed_account as account
FROM CustomerBase f 
where b_active_flag = 'DRC' and e_active_flag = 'Active'
)

,WeeklyCustomerBaseRejoiners AS(

SELECT w.*,
case when r.account is not null and fixed_churntype is null then 'Early Rejoiner' else null end as RejoinerFlag
FROM WeeklyCustomerBaseChurners w LEFT JOIN WeeklyEarlyRejoiners r on w.fixed_account = r.account
and w.Fixed_Month = r.month and w.fixed_monthweek = r.fixed_monthweek

)

,WeeklyBase_MainFlags as(
Select *,
case when fixed_churntype = '1.Fixed Voluntary Churner' then 'Voluntary'
when fixed_churntype = '2.Fixed Involuntary Churner' then 'Involuntary'
when rejoinerflag = 'Early Rejoiner' then 'Early Rejoiner'
else null end as MainChurnFlag
from weeklycustomerbaserejoiners
)


Select *
FROM WeeklyBase_MainFlags
where fixed_month in(date('2022-05-01'),date('2022-06-01'))
