WITH weekly_dates AS (
SELECT DISTINCT date_trunc('Week', DATE(dt))  AS BOW, date_trunc('Week', DATE(dt)) + interval '7' day AS EOW
FROM "db-analytics-prod"."fixed_cwp" 
WHERE DATE_TRUNC('YEAR', DATE(DT)) = DATE('2022-01-01')
)
,calendar_weeks AS (
 SELECT extract(year FROM bow) AS year, extract(week FROM BOW) AS calendar_week,*
 FROM weekly_dates
)
,UsefulFields AS(
SELECT DISTINCT date_trunc('month',date(dt)) as month,ACT_ACCT_CD,ACT_CONTACT_PHONE_3,FI_OUTST_AGE
,first_value(dt) over(partition by act_acct_cd, date_trunc('Month', date(dt)) order by dt desc) as MaxDateMonth
,first_value(act_cust_strt_dt) over(partition by act_acct_cd order by dt desc) AS MaxStart
,round(FI_TOT_MRC_AMT,0) AS MRC_amt
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
PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm,pd_mix_cd,date(dt) as dt,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,fi_bb_mrc_amt,fi_tv_mrc_amt,fi_vo_mrc_amt
FROM "db-analytics-prod"."fixed_cwp"  WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
    AND DATE_TRUNC('YEAR', DATE(dt)) = date('2022-01-01') 
    AND date(dt) between date('2022-07-04') and date('2022-07-11')
    and (FI_OUTST_AGE<=100 or FI_OUTST_AGE is null)
)
,LastDayRGUs AS(
  SELECT date_trunc('Month', date(dt)) as Month, act_acct_cd, first_value (numBB + numTV + numVO) over (partition by act_acct_cd, date_trunc('Month', date(dt)) order by dt desc) as last_rgus, first_value(dt) over (partition by act_acct_cd order by dt desc) as last_date
  FROM UsefulFields
)
, ActiveUsersBOW as(
 SELECT w.year, w.calendar_week as b_calendarweek, u.Month as B_Month, w.BOW, w.EOW, u.act_acct_cd AS accountBOM,act_contact_phone_3 as PhoneBOM3,u.dt as B_Date,pd_mix_cd as B_MixCode ,pd_mix_nm as B_MixName ,pd_bb_prod_nm as B_ProdBBName,pd_tv_prod_nm as B_ProdTVName,pd_vo_prod_nm as B_ProdVoName,(NumBB+NumTV+NumVO) as B_NumRGUs, 
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
    mrc_amt as B_MRC ,fi_outst_age  as B_OutstAge, fi_bb_mrc_amt as B_MRCBB, fi_tv_mrc_amt as B_MRCTV, fi_vo_mrc_amt as B_MRCVO, MaxStart as B_MaxStart, techflag as B_Tech_Type
    ,case when cast(fi_outst_age as double) <= 90 or fi_outst_age is null then 'Active' 
    else 'DRC' end as b_active_flag,  last_rgus
 FROM UsefulFields u 
    inner join calendar_weeks w on  DATE(u.dt) = w.BOW
    LEFT JOIN LASTDAYRGUs l ON u.act_acct_cd = l.act_acct_cd and u.Month = l.Month
  WHERE DATE(u.dt) = w.BOW
)
, ActiveUsersEOW as(
SELECT w.year, w.calendar_week as e_calendarweek, u.Month as E_Month, w.BOW, w.EOW, u.act_acct_cd AS accountEOM, act_contact_phone_3 as PhoneEOM3,u.dt as E_Date,pd_mix_cd as E_MixCode ,pd_mix_nm as E_MixName ,pd_bb_prod_nm as E_ProdBBName,pd_tv_prod_nm as E_ProdTVName,pd_vo_prod_nm as E_ProdVoName,(NumBB+NumTV+NumVO) as E_NumRGUs,
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
     mrc_amt as E_MRC ,fi_outst_age  as E_OutstAge,fi_bb_mrc_amt as E_MRCBB, fi_tv_mrc_amt as E_MRCTV, fi_vo_mrc_amt as E_MRCVO,MaxStart as E_MaxStart, techflag as E_Tech_TypE
    , case when cast(fi_outst_age as double) <= 90 or fi_outst_age is null then 'Active' 
    else 'DRC' end as e_active_flag
 FROM  UsefulFields u inner join calendar_weeks w on DATE(u.dt) = w.EOW
  WHERE DATE(u.dt) = w.EOW
)
, CUSTOMERBASE AS(
  SELECT
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Year
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Year
  END AS Fixed_Year,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.b_calendarweek
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.e_calendarweek
  END AS Fixed_CalendarWeek,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS Fixed_Account,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM3
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM3
  END AS f_contactphone3,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOW,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOW,
  b_active_flag, e_active_flag, b.b_Month, b.BOW,B_Date,B_Tech_Type, B_MixCode, B_MixCode_Adj, B_MixName, B_MixName_Adj,  B_ProdBBName,B_ProdTVName,B_ProdVoName, BB_RGU_BOM, TV_RGU_BOM, VO_RGU_BOM,B_NumRGUs,B_MRC ,B_OutstAge, B_MRCBB, B_MRCTV, B_MRCVO, B_MaxStart, DATE_DIFF('day', DATE(B_MaxStart),DATE(B_Date)) as B_TenureDays,
  CASE WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) <= 180 Then 'Early-Tenure'
  WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) > 180 THEN 'Late-Tenure' END AS B_FixedTenureSegment,
  e.e_Month, e.EOW,E_Date,E_Tech_Type, E_MixCode, E_MixCode_Adj ,E_MixName, E_MixName_Adj ,E_ProdBBName,E_ProdTVName,E_ProdVoName,BB_RGU_EOM, TV_RGU_EOM, VO_RGU_EOM, E_NumRGUs, E_MRC ,E_OutstAge, E_MRCBB, E_MRCTV, E_MRCVO
  , E_MaxStart, DATE_DIFF('day', DATE(E_MaxStart),  DATE(E_Date)) as E_TenureDays,
  CASE WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) <= 180 Then 'Early-Tenure'
  WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) > 180 THEN 'Late-Tenure' END AS E_FixedTenureSegment, 
  (E_MRC - B_MRC) as MRCDiff,last_rgus, (coalesce(B_NumRGUs,0) - coalesce(E_NumRGUs,0)) as Dif_RGUs
  FROM ActiveUsersBOW b FULL OUTER JOIN ActiveUsersEOW e
  ON b.accountBOM = e.accountEOM AND b.year = e.year and b.b_calendarweek = e.e_calendarweek
)
------------------------- Gross ads, upsells and downsells ----------------------------------------
,MAINMOVEMENTBASE AS(
SELECT a.*,
CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 and b_active_flag = 'Active' and e_active_flag = 'Active' THEN '1.SameRGUs' 
WHEN (E_NumRGUs - B_NumRGUs) > 0  and b_active_flag = 'Active' and e_active_flag = 'Active' THEN '2.Upsell'
WHEN (E_NumRGUs - B_NumRGUs) < 0  and b_active_flag = 'Active' and e_active_flag = 'Active' THEN '3.Downsell'
WHEN (B_NumRGUs IS NULL or b_active_flag is null) AND (E_NumRGUs > 0 AND e_active_flag = 'Active') AND(DATE_TRUNC ('MONTH', DATE(E_MaxStart)) = b_month or DATE_TRUNC ('MONTH', DATE(E_MaxStart)) =e_month)  THEN '4.New Customer'
WHEN (B_NumRGUs IS NULL or b_active_flag = 'DRC' or b_active_flag is null) AND (E_NumRGUs > 0 AND e_active_flag = 'Active') AND (DATE_TRUNC ('MONTH', DATE(E_MaxStart)) <> e_month) THEN '5.Come Back to Life'
WHEN (B_NumRGUs > 0 AND  b_active_flag ='Active') AND (E_NumRGUs IS NULL or e_active_flag = 'DRC' or e_active_flag is null) THEN '6.Null last day'
WHEN (B_NumRGUs IS NULL or b_active_flag = 'DRC' or b_active_flag is null) AND (E_NumRGUs IS NULL or e_active_flag = 'DRC' or e_active_flag is null) THEN '7.Always null'
END AS MainMovement
FROM CUSTOMERBASE a
)
--################################# FIXED CHURN FLAGS --###############################################################
,SO_flag AS(
select *
   ,case when max(lob_vo_count)> 0 and max(dxtype) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_vo_count, 
    case when max(lob_bb_count) > 0 and max(dxtype) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_bb_count, 
    case when max(lob_tv_count) > 0 and max(dxtype) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_tv_count
from(Select distinct date_trunc('Month', date(completed_date)) as month,date(completed_date) as EndDate,date(order_start_date) as StartDate
,cease_reason_code, cease_reason_desc,cease_reason_group,DATE_TRUNC('month', completed_date) as completed_month, extract(year from completed_date) as completed_year, extract(week from completed_date) as completed_week,order_type
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
where order_type = 'DEACTIVATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
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
SELECT DISTINCT s.* ,ChurnedRGUs,date,NumRgusLastRecord
,CASE WHEN v.FixedAccount IS NOT NULL and vol_flag=1  THEN '1.Fixed Voluntary Churner' END AS ChurnType
FROM MAINMOVEMENTBASE s LEFT JOIN VoluntaryFlag v ON s.fixed_account=v.FixedAccount AND s.b_Month=v.Month
)
,VoluntaryChurners_Adj AS(
SELECT DISTINCT b_Month AS Month,Fixed_Account AS ChurnAccount,ChurnType,date
,CASE WHEN ChurnType IS NOT NULL AND ActiveEOw=1 AND B_NumRGUs>NumRgusLastRecord THEN 1 ELSE 0 END AS PartialChurn
FROM VoluntaryChurners
)
,FinalVoluntaryChurners AS(
SELECT DISTINCT MONTH, ChurnAccount,date as churndate,churntype,extract(year from date(date)) as churnyear,extract(week from date(date)) as churnweek
, CASE WHEN ChurnAccount IS NOT NULL THEN '1.Fixed Voluntary Churner' END AS FixedChurnerType
FROM VoluntaryChurners_Adj
WHERE ChurnType IS NOT NULL AND PartialChurn=0 
)
,WeeklyVoluntaryChurners AS(
SELECT v.churnyear as Year, f.Fixed_calendarweek, f.BOW, f.EOW, v.churnAccount, v.churntype, date(v.Churndate) as churndate
FROM FinalVoluntaryChurners v
INNER JOIN customerbase f on v.ChurnAccount = f.fixed_account and churnyear = f.Fixed_Year 
and v.ChurnWeek = f.Fixed_calendarweek
where date(v.ChurnDate) >= f.BOW and  (date(v.churnDate) <f.EOW or f.EOW is null)
)
,OverdueCustomers AS(
SELECT month,  extract(year from date(dt)) as churnyear, extract(week from date(dt)) as churnweek,act_acct_cd, fi_outst_age, dt
FROM UsefulFields
WHERE fi_outst_age >= 90
)
,WeeklyInvoluntaryChurners as(
SELECT f.Fixed_year as Year, f.fixed_calendarweek, f.BOW, f.EOW, f.fixed_account as churnaccount,
'2.Fixed Involuntary Churner' as churntype, date(o.dt) as churndate
FROM CustomerBase f INNER JOIN  overduecustomers o on f.fixed_account = o.act_acct_cd 
AND f.fixed_year = o.churnyear and f.fixed_calendarweek = o.churnweek
where b_active_flag = 'Active' and date(dt) > f.BOW and (date(dt) <= f.EOW or EOW is null)
)
,AllChurners as(
Select Year, fixed_calendarweek, churnaccount, churntype
From 
( SELECT * FROM WeeklyVoluntaryChurners
UNION ALL 
SELECT * from WeeklyInvoluntaryChurners)
)
,WeeklyCustomerBaseChurners AS(
SELECT m.*,
churntype as Fixed_ChurnType
FROM mainmovementbase m LEFT JOIN AllChurners a on m.fixed_account = a.churnaccount
and m.Fixed_year = a.year and  m.fixed_calendarweek = a.fixed_calendarweek
)
,WeeklyEarlyRejoiners as(
SELECT f.Fixed_year as year, f.fixed_calendarweek, f.BOW, f.EOW, f.fixed_account as account
FROM CustomerBase f 
where b_active_flag = 'DRC' and e_active_flag = 'Active'
)
,WeeklyCustomerBaseRejoiners AS(
SELECT w.*,
case when r.account is not null and fixed_churntype is null then 'Early Rejoiner' else null end as RejoinerFlag
FROM WeeklyCustomerBaseChurners w LEFT JOIN WeeklyEarlyRejoiners r on w.fixed_account = r.account
and w.Fixed_year = r.year and w.fixed_calendarweek = r.fixed_calendarweek
)
,WeeklyBase_MainFlags as(
Select *,
case when fixed_churntype = '1.Fixed Voluntary Churner' then 'Voluntary'
when fixed_churntype = '2.Fixed Involuntary Churner' then 'Involuntary'
when rejoinerflag = 'Early Rejoiner' then 'Early Rejoiner'
else null end as MainChurnFlag
from weeklycustomerbaserejoiners
)
-----------------------------Rejoiners-------------------------------------------------------------
,Potential_rejoiners_week as(
Select distinct Fixed_year, fixed_calendarweek, Fixed_Account
from customerbase
where activeBOW = 0 and activeEOW = 1
)
,ActiveUsers_PreviousMonth as(
select distinct t.act_acct_cd, date(t.dt) as date
FROM "db-analytics-prod"."fixed_cwp" t
inner join usefulfields u on t.act_acct_cd = u.act_acct_cd and date(t.dt) = date(u.dt) - interval '1' month
inner join calendar_weeks c on date(t.dt) = c.BOW - interval '1' month
 WHERE t.PD_MIX_CD<>'0P'AND t.act_cust_typ_nm = 'Residencial' 
    and cast(t.fi_outst_age as double) <= 90 or t.fi_outst_age is null
)
,Rejoiners_Week AS(
SELECT fixed_year, fixed_calendarweek, fixed_account
FROM Potential_rejoiners_week p inner join activeusers_previousmonth a
on p.fixed_account = a.act_acct_cd
)
, WeeklyBase_Rejoiners AS(
select w.*,
case when r.fixed_account is not null then 1 else 0 END AS W_rejoiner
from weeklybase_mainflags w left join rejoiners_week r on w.fixed_account = r.fixed_account 
and w.fixed_calendarweek = r.fixed_calendarweek and w.fixed_year = r.fixed_year
)
, WeeklyTable AS(
SELECT *, 
first_value(b_active_flag_adj) over(partition by fixed_account,b_month order by fixed_calendarweek asc) as b_active_temp,first_value(e_active_flag_adj) over(partition by fixed_account,b_month order by fixed_calendarweek desc) as e_active_temp
,first_value(b_numrgus) over(partition by fixed_account,b_month order by fixed_calendarweek asc) as first_rgus_month,first_value(e_numrgus) over(partition by fixed_account,b_month order by fixed_calendarweek desc) as last_rgus_month
,first_value(activebow) over(partition by fixed_account,b_month order by fixed_calendarweek asc) as activebow_month,first_value(activeeow) over(partition by fixed_account,b_month order by fixed_calendarweek desc) as activeeow_month
FROM
(select *,case when b_active_flag is null then 'Inactive' when b_active_flag='DRC' then 'Inactive' else b_active_flag end as b_active_flag_adj,case when e_active_flag is null then 'Inactive' when e_active_flag='DRC' then 'Inactive' else e_active_flag end as e_active_flag_adj
FROM weeklybase_rejoiners)
)
,weeklytable_rgus AS(
 SELECT DISTINCT *, 
 COALESCE(first_rgus_month,0) - COALESCE(last_rgus_month,0) as Dif_RGUsMonth
 FROM weeklytable
)
,SO_LLAFlags AS(
 select completed_month, account_id, fixed_calendarweek as week,completed_year,
   sum(vol_lob_vo_count) + sum(vol_lob_bb_count) + sum(vol_lob_tv_count) as vol_churn_rgu,
    case when sum(case when dxtype = 'Migracion'  then 1 else 0 end) > 0 then 1 else 0 end as cst_churn_flag,
    case when sum(case when dxtype = 'Involuntary' then 1 else 0 end) > 0 then 1 else 0 end as non_pay_so_flag
    from SO_flag p inner join WeeklyTable w on cast(p.account_id as varchar) = cast(w.fixed_account as varchar) and p.completed_year = w.fixed_year and p.completed_week = w.fixed_calendarweek
    where enddate > w.BOW and (enddate <= w.EOW or EOW is null)
    group by account_id, completed_month, 3,completed_year
)
,join_so_fixedbase as (
    select a.*, b.cst_churn_flag,
    case when a.MainChurnFlag ='Voluntary' and coalesce(last_rgus_month,0) < coalesce(first_rgus_month,0) and b_active_flag = 'Active' and MainMovement <> '3.Downsell' then 'Voluntary' 
     when a.MainChurnFlag is null and activeeow = 0 and activeeow_month = 0 and cast(a.B_OutstAge as integer) <90 and (b.cst_churn_flag = 0 or b.cst_churn_flag is null) and non_pay_so_flag=1 then 'Early Dx'
    when a.MainChurnFlag ='Involuntary' and b_active_flag = 'Active' then 'Involuntary'
    when a.MainChurnFlag = 'Early Rejoiner' then 'Early Rejoiner'
    when ((b.cst_churn_flag = 1 and ((a.MainChurnFlag <>'Involuntary' and a.MainChurnFlag <> 'Voluntary') or a.MainChurnFlag is null))) and b_active_flag = 'Active' and (b_numrgus > e_numrgus or (e_active_temp = 'Inactive' and activeeow = 0)) then 'Incomplete CST'
    when a.mainmovement = '2.Upsell' and e_numrgus > b_numrgus and b_active_flag = 'Active' then 'Upsell'
    when a.mainmovement='5.Come Back to Life' and W_rejoiner=1 and (a.MainChurnFlag<>'Early Rejoiner' or a.MainChurnFlag is null) then 'Rejoiner'
    when a.mainmovement = '4.New Customer' or (a.mainmovement = '5.Come Back to Life' and (a.mainchurnflag <> 'Early Rejoiner' or a.mainchurnflag is null) and W_Rejoiner =0 )
    then 'Gross Add' 
    end as FinalFixedChurnFlag
    from weeklytable_RGUS  a left join SO_LLAFlags b
    on cast(a.fixed_account as varchar) = cast(b.account_id as varchar)
    and a.fixed_year = b.completed_year and a.fixed_calendarweek = b.week
)
, finalfixedbase_final as(
select a.*,
finalfixedchurnflag as finalfixedchurnflag_adj
,first_value(b_active_flag) over(partition by fixed_account order by fixed_calendarweek asc) as first_active_flag
, case when mainmovement = '3.Downsell' and finalfixedchurnflag = ' CST Churner' then 'Partial Churner'
when mainmovement = '2.Upsell' or  mainmovement = '4.New Customer' or  mainmovement ='5.Come Back to Life' then 'Adds'
when finalfixedchurnflag is null then null
else 'Total Churner' end as fixed_partial_total_churnflag
, case when finalfixedchurnflag = 'Gross Add' or  finalfixedchurnflag ='Rejoiner' or finalfixedchurnflag = 'Upsell' then 'Adds'
when finalfixedchurnflag is null then null
else 'Churn' end as Overall_Movement
,case when b_active_flag_adj='Inactive' and e_active_flag_adj='Inactive' then 1 else 0 end as exclude
from join_so_fixedbase a
) 

Select 
Fixed_year, fixed_calendarweek, activebow,activeeow,b_active_flag_adj,e_active_flag_adj,Overall_Movement,fixed_partial_total_churnflag,FinalFixedChurnFlag_adj, 
case when FinalFixedChurnFlag_adj = 'Early Rejoiner' then count(distinct fixed_account)*-1
else count(distinct fixed_account) end as Customers,
case when FinalFixedChurnFlag_adj= 'Early Rejoiner' then sum(b_numrgus*-1) 
when FinalFixedChurnFlag_adj = 'Involuntary' or FinalFixedChurnFlag_adj = 'Incomplete CST' then sum (b_numrgus)
when FinalFixedChurnFlag_adj = 'Voluntary' then sum (dif_rgus)
when FinalFixedChurnFlag_adj in('Gross Add','Rejoiner') then sum(e_numrgus)
when FinalFixedChurnFlag_adj = 'Upsell' then sum(dif_rgus)*-1
when finalfixedchurnflag_adj is null then sum(b_numrgus)
else sum(Dif_RGus) end as RGUs 
from finalfixedbase_final
where fixed_calendarweek = 27 
and exclude=0 
group by 1,2,3,4,5,6,7,8,9 order by 1,2,3,4,5,6,7,8,9
