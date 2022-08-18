CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cwp_fix_stg_weekly_churn_input_mayjune" AS
WITH weekly_dates as(
SELECT distinct date_trunc('Month', DATE(dt)) as Month, 
case when date_trunc('Month', date_trunc('week', DATE(dt))) = date_trunc('Month', DATE(dt))
and date(dt) <= date_trunc('Month', DATE(dt)) + interval '1' month - interval '1' day
then  date_trunc('week', DATE(dt)) 
else date_trunc('Month', DATE(dt)) - interval '1' day end as BOW,
case when date_trunc('Month', date_trunc('Week', DATE(dt))+ interval '7' day) = date_trunc('Month', DATE(dt)) then  date_trunc('Week', DATE(dt))+ interval '7' day
else date_trunc('Month', DATE(dt)) + interval '1' month - interval '1' day end as EOW
from "db-analytics-prod"."fixed_cwp" 
WHERE DATE_TRUNC('YEAR', DATE(DT)) = date('2022-01-01')
order by 1,2,3
)

,weeks_per_month as(
select *,
row_number() over (partition by month order by EOW asc) as month_week,
(Month - interval '1' day) as BOM, (Month + interval '1' month - interval '1' day) as EOM
from weekly_dates
where bow <> eow
order by month, 4, bow, eow
)

,lag_dna as (
select *, date_trunc('month', date(dt)) month_dt, 
 case when (fi_outst_age is null and (next1_fi_outst_age>90 or next2_fi_outst_age>91) or fi_outst_age>90) then 1 else 0 end as exclude,--
 case when fi_outst_age = (90) or 
        (next1_fi_outst_age = (90) + 1 and date_trunc('month',date(next1_dt) - interval '1' day) = date_trunc('month',date(dt))) or 
        (next2_fi_outst_age = (90) + 2 and date_trunc('month',date(next2_dt) - interval '2' day) = date_trunc('month',date(dt))) or 
        (next3_fi_outst_age = (90) + 3 and date_trunc('month',date(next3_dt) - interval '3' day) = date_trunc('month',date(dt))) or
        (next4_fi_outst_age = (90) + 4 and date_trunc('month',date(next4_dt) - interval '4' day) = date_trunc('month',date(dt)))then 1 else 0 end as inv_churn_flg

from (select *,
        lag(fi_outst_age) over (partition by act_acct_cd order by dt desc) as next1_fi_outst_age,
        lag(fi_outst_age,2) over (partition by act_acct_cd order by dt desc) as next2_fi_outst_age,
        lag(fi_outst_age,3) over (partition by act_acct_cd order by dt desc) as next3_fi_outst_age,
        lag(fi_outst_age,4) over (partition by act_acct_cd order by dt desc) as next4_fi_outst_age,
        lag(dt) over (partition by act_acct_cd order by dt desc) as next1_dt,
        lag(dt,2) over (partition by act_acct_cd order by dt desc) as next2_dt,
        lag(dt,3) over (partition by act_acct_cd order by dt desc) as next3_dt,
        lag(dt,4) over (partition by act_acct_cd order by dt desc) as next4_dt
        from "db-analytics-prod"."fixed_cwp"
        WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
        and date(dt) between (DATE('2022-06-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  (DATE('2022-06-01') + interval '1' MONTH - interval '1' DAY) )
)
,UsefulFields AS(
SELECT DISTINCT date_trunc('month',date(dt)) as month
,ACT_ACCT_CD,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(CAST(CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart, round(FI_TOT_MRC_AMT,0) AS MRC_amt
,Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
    ELSE 'COPPER' end as TechFlag
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numVO
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS BB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS TV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS VO,
CASE WHEN evt_frst_sale_chnl = 'CALL CENTER' THEN 'Tele Sales'
        WHEN evt_frst_sale_chnl = 'Negocios Regionales' or evt_frst_sale_chnl ='AM REGIONAL'THEN 'Regionales'
        WHEN evt_frst_sale_chnl = 'Dealers' THEN 'Agencias'
        WHEN evt_frst_sale_chnl = 'TIENDAS' or evt_frst_sale_chnl = 'Tiendas'  THEN 'Stores'
        WHEN evt_frst_sale_chnl = 'D2D' or evt_frst_sale_chnl = 'Door 2 Door B2C'  THEN 'D2D'
        WHEN evt_frst_sale_chnl in ( 'Alianzas', 'Promotores', 'Ventas Corporativas') THEN 'Other'
        WHEN evt_frst_sale_chnl = 'Ventas Web' then 'WEB'
         WHEN evt_frst_sale_chnl is null THEN 'No Channel'
         Else NULL 
         END AS FIRST_SALES_CHNL,
CASE WHEN evt_lst_sale_chnl = 'CALL CENTER' THEN 'Tele Sales'
        WHEN evt_lst_sale_chnl = 'Negocios Regionales'  or evt_lst_sale_chnl ='AM REGIONAL'THEN 'Regionales'
        WHEN evt_lst_sale_chnl = 'Dealers' THEN 'Agencias'
        WHEN evt_lst_sale_chnl = 'TIENDAS' or evt_lst_sale_chnl = 'Tiendas'  THEN 'Stores'
        WHEN evt_lst_sale_chnl = 'D2D' or evt_lst_sale_chnl = 'Door 2 Door B2C'  THEN 'D2D'
        WHEN evt_lst_sale_chnl in ( 'Alianzas', 'Promotores', 'Ventas Corporativas') THEN 'Other'
        WHEN evt_lst_sale_chnl = 'Ventas Web' then 'WEB'
         WHEN evt_lst_sale_chnl is null THEN 'No Channel'
         Else NULL 
         END AS LAST_SALES_CHNL,
PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm,pd_mix_cd,date(dt) as dt,exclude,inv_churn_flg,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,fi_bb_mrc_amt,fi_tv_mrc_amt,fi_vo_mrc_amt
FROM lag_dna
WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
and date(dt) between (DATE('2022-05-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  (DATE('2022-05-01') + interval '1' MONTH - interval '1' DAY + interval '2' MONTH)
GROUP BY LOAD_DT,dt,2,3,FI_OUTST_AGE,6, 7,8, 9, 10, 11,12,13,14,15,PD_BB_PROD_CD,pd_tv_prod_cd,PD_VO_PROD_CD,pd_mix_nm,pd_mix_cd,exclude,inv_churn_flg,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,fi_bb_mrc_amt,fi_tv_mrc_amt,fi_vo_mrc_amt
)

,LastDayRGUs AS(
  SELECT date_trunc('Month', date(dt)) as Month, act_acct_cd, first_value (numBB + numTV + numVO) over (partition by act_acct_cd, date_trunc('Month', date(dt)) order by dt desc) as last_rgus, first_value(dt) over (partition by act_acct_cd order by dt desc) as last_date
  FROM UsefulFields
  )

, ActiveUsersBOW as(
 SELECT DISTINCT w.Month, w.month_week, w.BOW, u.act_acct_cd AS accountBOM,contacto as PhoneBOM3,
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
    mrc_amt as B_MRC ,fi_outst_age  as B_OutstAge, fi_bb_mrc_amt as B_MRCBB, fi_tv_mrc_amt as B_MRCTV, fi_vo_mrc_amt as B_MRCVO,
    MaxStart as B_MaxStart, Techflag as B_Tech_Type, pd_mix_cd as B_bundlecode, pd_mix_nm as B_bundlename,
    case when cast(fi_outst_age as double) <= 90 or fi_outst_age is null then 'Active' 
    else 'DRC' end as b_active_flag, last_rgus
 FROM
    UsefulFields u inner join weeks_per_month w on date(u.dt) = w.BOW
    LEFT JOIN LASTDAYRGUs l ON u.act_acct_cd = l.act_acct_cd and u.Month = l.Month
  WHERE
    DATE(u.dt) = w.BOW
  GROUP BY
    1, 2, 3, 4, 5,6,7 ,8, 9,10,11, 15 ,16, 17, 18, 19, 20, 21, 22, 12, 21, 22, 23, 13, 14, 24, 25,26,27,28
)

, ActiveUsersEOW as(
SELECT
    DISTINCT w.Month, w.month_week, w.EOW, u.act_acct_cd AS accountEOM, contacto as PhoneEOM3,
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
     mrc_amt as E_MRC ,fi_outst_age  as E_OutstAge, fi_bb_mrc_amt as E_MRCBB, fi_tv_mrc_amt as E_MRCTV, fi_vo_mrc_amt as E_MRCVO,
    MaxStart as E_MaxStart, Techflag as E_Tech_TypE, pd_mix_cd as E_bundlecode, pd_mix_cd as E_bundlename
    , case when cast(fi_outst_age as double) <= 90 or fi_outst_age is null then 'Active' 
    else 'DRC' end as e_active_flag
 FROM
    UsefulFields u inner join weeks_per_month w on date(u.dt) = w.EOW
  WHERE
    DATE(u.dt) = w.EOW
  GROUP BY
    1, 2, 3, 4, 5, 6,7 ,8, 9,10,11, 15 ,16, 17,18, 19, 20, 21, 22, 12, 22, 23, 24, 13, 14,25,26,27
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
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM3
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM3
  END AS f_contactphone3,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  b_active_flag, e_active_flag,
  BOW,
  B_Date,B_Tech_Type, B_MixCode, B_MixCode_Adj, B_MixName, B_MixName_Adj,  B_ProdBBName,B_ProdTVName,B_ProdVoName, BB_RGU_BOM, TV_RGU_BOM, VO_RGU_BOM,B_NumRGUs,B_bundlecode, B_bundlename,
  B_MRC ,B_OutstAge, B_MRCBB, B_MRCTV, B_MRCVO
  , B_MaxStart, DATE_DIFF('day', DATE(B_MaxStart),DATE(B_Date)) as B_TenureDays,
  CASE WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) <= 180 Then 'Early-Tenure'
  WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) > 180 THEN 'Late-Tenure' END AS B_FixedTenureSegment,
  EOW,E_Date,E_Tech_Type, E_MixCode, E_MixCode_Adj ,E_MixName, E_MixName_Adj ,E_ProdBBName,E_ProdTVName,E_ProdVoName,BB_RGU_EOM, TV_RGU_EOM, VO_RGU_EOM, E_NumRGUs, E_bundlecode, E_bundlename,
  E_MRC ,E_OutstAge, E_MRCBB, E_MRCTV, E_MRCVO
  , E_MaxStart, DATE_DIFF('day', DATE(E_MaxStart),  DATE(E_Date)) as E_TenureDays,
  CASE WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) <= 180 Then 'Early-Tenure'
  WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) > 180 THEN 'Late-Tenure' END AS E_FixedTenureSegment, 
  (E_MRC - B_MRC) as MRCDiff,last_rgus
  , (coalesce(B_NumRGUs,0) - coalesce(E_NumRGUs,0)) as Dif_RGUs
  FROM ActiveUsersBOW b FULL OUTER JOIN ActiveUsersEOW e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH and b.month_week = e.month_week
)

------------------------- Gross ads, upsells and downsells ----------------------------------------
,MAINMOVEMENTBASE AS(
SELECT a.*,
CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 and b_active_flag = 'Active' and e_active_flag = 'Active' THEN '1.SameRGUs' 
WHEN (E_NumRGUs - B_NumRGUs) > 0  and b_active_flag = 'Active' and e_active_flag = 'Active' THEN '2.Upsell'
WHEN (E_NumRGUs - B_NumRGUs) < 0  and b_active_flag = 'Active' and e_active_flag = 'Active' THEN '3.Downsell'
WHEN (B_NumRGUs IS NULL or b_active_flag is null) AND (E_NumRGUs > 0 AND e_active_flag = 'Active') AND DATE_TRUNC ('MONTH', DATE(E_MaxStart)) = DATE('2022-06-01')  THEN '4.New Customer'
WHEN (B_NumRGUs IS NULL or b_active_flag = 'DRC' or b_active_flag is null) AND (E_NumRGUs > 0 AND e_active_flag = 'Active') AND DATE_TRUNC ('MONTH', DATE(E_MaxStart)) <> DATE('2022-06-01') THEN '5.Come Back to Life'
WHEN (B_NumRGUs > 0 AND  b_active_flag ='Active') AND (E_NumRGUs IS NULL or e_active_flag = 'DRC' or e_active_flag is null) THEN '6.Null last day'
WHEN (B_NumRGUs IS NULL or b_active_flag = 'DRC' or b_active_flag is null) AND (E_NumRGUs IS NULL or e_active_flag = 'DRC' or e_active_flag is null) THEN '7.Always null'
END AS MainMovement
FROM CUSTOMERBASE a
)
-- ################ Voluntary Churn ###############################################
,SO_flag AS(
Select distinct 
date_trunc('Month', date(completed_date)) as month,date(completed_date) as EndDate,date(order_start_date) as StartDate
,cease_reason_code, cease_reason_desc,cease_reason_group
,CASE 
 WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
 WHEN cease_reason_code IN('2','74') THEN 'Involuntario'
 WHEN (cease_reason_code = '9' AND cease_reason_desc='CAMBIO DE TECNOLOGIA') OR (cease_reason_code IN('32','44','55','72')) THEN 'Migracion'
 WHEN cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA' THEN 'Voluntario'
ELSE NULL END AS DxType
,account_id
,lob_vo_count,lob_bb_count,lob_tv_count
,first_value(date(completed_date)) over(partition by account_id,date_trunc('Month', date(completed_date)) order by completed_date desc) as date
from "db-stage-dev"."so_hdr_cwp" 
where order_type = 'DEACTIVATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
)
,RGUsFlag_SO AS(
SELECT Month,StartDate,account_id,DxType,date
,CASE WHEN lob_vo_count>0 THEN 1 ELSE 0 END AS VO_Churn
,CASE WHEN lob_bb_count>0 THEN 1 ELSE 0 END AS BB_Churn
,CASE WHEN lob_tv_count>0 THEN 1 ELSE 0 END AS TV_Churn
FROM SO_FLAG
)
,ChurnedRGUs_SO_Prel AS(
SELECT DISTINCT *
,(VO_CHURN + BB_CHURN + TV_CHURN) AS RGUs_Prel
FROM RGUsFlag_SO
WHERE DxType='Voluntario'
)
,ChurnedRGUs_SO AS (
SELECT DISTINCT Month,Account_id,dxtype,date
,SUM(RGUs_Prel) AS ChurnedRGUs
FROM ChurnedRGUs_SO_Prel
GROUP BY 1,2,3,4
)
,RGUS_MixLastDay AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month,date(dt) as dt,act_acct_Cd as FixedAccount,fi_outst_age
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
    WHEN PD_MIX_CD='0P' THEN '0P'
    END AS MixName_Adj
FROM UsefulFields
)
,RGUSLastRecordDNA AS(
SELECT DISTINCT Month, FixedAccount
,first_value(mixname_adj) over(partition by FixedAccount,DATE_TRUNC('Month',date(dt)) order by date(dt) desc) as LastRGU
FROM RGUS_MixLastDay
WHERE (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
)
,RGUSLastRecordDNA_Adj AS(
SELECT DISTINCT Month,FixedAccount,LastRGU
,CASE WHEN LastRGU IN ('VO', 'BB', 'TV') THEN 1
WHEN LastRGU IN ('BB+VO', 'BB+TV', 'VO+TV') THEN 2
WHEN lastRGU IN ('BB+VO+TV') THEN 3
WHEN lastRGU IN ('0P') THEN -1
ELSE 0 END AS NumRgusLastRecord
FROM RGUSLastRecordDNA
)
,LastRecordDateDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, 
act_acct_cd as FixedAccount,max(date(dt)) as LastDate
FROM Usefulfields
WHERE (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
GROUP BY 1,2
)
,OverdueLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, t.act_acct_cd as FixedAccount, fi_outst_age as LastOverdueRecord,(date_diff('day', DATE(MaxStart),DATE(dt))) as ChurnTenureDays
FROM Usefulfields t 
INNER JOIN LastRecordDateDNA d ON t.act_acct_cd = d.FixedAccount AND date(t.dt) = d.LastDate
)
,VoluntaryFlag AS(
SELECT DISTINCT l.month,l.fixedaccount,dxtype,l.LastRGU,NumRgusLastRecord,date
,ChurnedRGUs
,CASE WHEN v.ChurnedRGUs >= l.NumRgusLastRecord THEN 1 ELSE 0 END AS Vol_Flag
FROM CHURNEDRGUS_SO v INNER JOIN RGUSLastRecordDNA_Adj l ON CAST(v.account_id AS VARCHAR)=l.fixedaccount AND v.month = l.Month
INNER JOIN LastRecordDateDNA d on l.fixedaccount=d.fixedaccount AND l.Month = date_trunc('month',d.lastdate)
INNER JOIN OverdueLastRecordDNA o ON l.fixedaccount = o.fixedaccount AND l.month = o.Month
)
,VoluntaryChurners AS(
SELECT DISTINCT s.* --s.Fixed_Month,Fixed_Account,F_ActiveBOM,F_ActiveEOM,B_Overdue,B_TechFlag,B_NumRGUs,B_MixName_Adj,B_MixCode_Adj,E_Overdue,E_TechFlag,E_NumRGUs,E_MixName_Adj,E_MixCode_Adj,FixedMainMovement,FixedSpinMovement,LastRGU
,ChurnedRGUs,date,NumRgusLastRecord
,CASE WHEN v.FixedAccount IS NOT NULL and vol_flag=1  THEN '1.Fixed Voluntary Churner' END AS ChurnType
FROM MAINMOVEMENTBASE s LEFT JOIN VoluntaryFlag v ON s.fixed_account=v.FixedAccount AND s.Fixed_Month=v.Month
)
,VoluntaryChurners_Adj AS(
SELECT DISTINCT Fixed_Month AS Month,Fixed_Account AS ChurnAccount,ChurnType,date
,CASE WHEN ChurnType IS NOT NULL AND ActiveEOM=1 AND B_NumRGUs>NumRgusLastRecord THEN 1 ELSE 0 END AS PartialChurn
FROM VoluntaryChurners
)
,FinalVoluntaryChurners AS(
SELECT DISTINCT MONTH, ChurnAccount,date as churndate,churntype
, CASE WHEN ChurnAccount IS NOT NULL THEN '1.Fixed Voluntary Churner' END AS FixedChurnerType
FROM VoluntaryChurners_Adj
WHERE ChurnType IS NOT NULL AND PartialChurn=0 
)
,WeeklyVoluntaryChurners AS(
SELECT v.Month, f.Fixed_MonthWeek, f.BOW, f.EOW, v.churnaccount as account, v.churntype, date(v.Churndate) as churndate
FROM finalvoluntarychurners v
INNER JOIN customerbase f on v.churnaccount = f.fixed_account and v.Month = f.Fixed_Month
where date(v.ChurnDate) >= f.BOW and  (date(v.churnDate) <f.EOW or f.EOW is null)
)
,OverdueCustomers AS(
SELECT month, act_acct_cd, fi_outst_age, dt
FROM UsefulFields
WHERE fi_outst_age = 90
)
,WeeklyInvoluntaryChurners as(
SELECT f.Fixed_month as Month, f.fixed_monthweek, f.BOW, f.EOW, f.fixed_account as account,
'2.Fixed Involuntary Churner' as churntype, date(o.dt) as churndate
FROM CustomerBase f INNER JOIN  overduecustomers o on f.fixed_account = o.act_acct_cd 
AND f.fixed_month = o.month 
where b_active_flag = 'Active' and date(dt) > f.BOW and (date(dt) <= f.EOW or EOW is null)
)
,AllChurners as(
Select Month, fixed_monthweek, account, churntype
From 
( SELECT Month, fixed_monthweek, account, churntype FROM WeeklyVoluntaryChurners
UNION ALL 
SELECT  Month, fixed_monthweek, account, churntype from WeeklyInvoluntaryChurners)
)

,WeeklyCustomerBaseChurners AS(
SELECT m.*,
churntype as Fixed_ChurnType
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
--distinct fixed_month--,fixed_MonthWeek,MainChurnFlag,count(distinct fixed_account)
FROM WeeklyBase_MainFlags
where fixed_month >= date('2022-02-01') --or fixed_month = date('2022-04-01')
--and rejoinerflag = 'Early Rejoiner'
--and fixed_account in('113013460000','125016040000','133042760000','148015160000')
--group by 1,2--,3
--order by 3,1,2
