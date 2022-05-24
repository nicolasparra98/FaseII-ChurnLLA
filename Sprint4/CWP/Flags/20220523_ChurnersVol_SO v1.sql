WITH
FixedUsefulFields AS(
SELECT DISTINCT LOAD_DT
,ACT_ACCT_CD AS FixedAccount,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(CAST(CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart, round(FI_TOT_MRC_AMT,0) AS Fixed_MRC
,Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
    ELSE 'COPPER' end as TechFlag
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numVO,
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
PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm, PD_MIX_CD
FROM "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE --PD_MIX_CD<>'0P'
act_cust_typ_nm = 'Residencial' --AND (CAST(FI_OUTST_AGE AS INTEGER)<=90 OR FI_OUTST_AGE IS NULL)
GROUP BY LOAD_DT,2,3,FI_OUTST_AGE,6, 7,8, 9, 10, 11,12,PD_BB_PROD_CD,pd_tv_prod_cd,PD_VO_PROD_CD,pd_mix_nm,PD_MIX_CD
)
,FixedUsefulFields_Adj AS(
SELECT DISTINCT * --load_dt,fixedaccount,pd_mix_nm,pd_mix_cd
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
    WHEN PD_MIX_CD='0P' THEN '0P'
    END AS MixName_Adj
FROM FixedUsefulFields
--ORDER BY Load_dt
)
,SO_flag AS(
Select distinct 
date_trunc('Month', date(completed_date)) as month
,cease_reason_code, cease_reason_desc,cease_reason_group
,CASE 
 WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
 WHEN cease_reason_code IN('2','74') THEN 'Involuntario'
 WHEN (cease_reason_code = '9' AND cease_reason_desc='CAMBIO DE TECNOLOGIA') OR (cease_reason_code IN('32','44','55','72')) THEN 'Migracion'
 WHEN cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA' THEN 'Voluntario'
ELSE NULL END AS DxType
,account_id
,lob_vo_count,lob_bb_count,lob_tv_count
from "db-analytics-prod"."so_hdr_cwp" 
where order_type = 'DEACTIVATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
)
,RGUsFlag_SO AS(
SELECT Month,account_id,DxType
,CASE WHEN lob_vo_count>0 THEN 1 ELSE 0 END AS VO_Churn
,CASE WHEN lob_bb_count>0 THEN 1 ELSE 0 END AS BB_Churn
,CASE WHEN lob_tv_count>0 THEN 1 ELSE 0 END AS TV_Churn
FROM SO_FLAG
)
,ChurnedRGUs_SO AS(
SELECT DISTINCT *
,(VO_CHURN + BB_CHURN + TV_CHURN) AS ChurnedRGUs
FROM RGUsFlag_SO
WHERE DxType='Voluntario'
)
,RGUSLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(load_dt)) AS Month, FixedAccount
,last_value(mixname_adj) over(partition by FixedAccount, DATE_TRUNC('MONTH',DATE(load_dt)) order by DATE_TRUNC('MONTH',DATE(load_dt))) AS LastRGU
FROM FixedUsefulfields_Adj
WHERE (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
)
,RGUSLastRecordDNA_Adj AS(
SELECT DISTINCT Month,FixedAccount
,CASE WHEN LastRGU IN ('VO', 'BB', 'TV') THEN 1
WHEN LastRGU IN ('BB+VO', 'BB+TV', 'VO+TV') THEN 2
WHEN lastRGU IN ('BB+VO+TV') THEN 3
WHEN lastRGU IN ('0P') THEN -1
ELSE 0 END AS NumRgusLastRecord
FROM RGUSLastRecordDNA
)
,LastRecordDateDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(load_dt)) AS Month, FixedAccount,date(max(load_dt)) as LastDate
FROM FixedUsefulfields_Adj
WHERE  (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
 GROUP BY 1, FixedAccount
 ORDER BY FixedAccount
)
,OverdueLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(load_dt)) AS Month, t.FixedAccount, fi_outst_age as LastOverdueRecord,(date_diff('day', DATE(MaxStart),DATE(load_dt))) as ChurnTenureDays
FROM FixedUsefulfields_Adj t 
INNER JOIN LastRecordDateDNA d ON t.FixedAccount = d.FixedAccount AND t.load_dt = d.LastDate
)
,VoluntaryFlag AS(
SELECT DISTINCT l.month,l.fixedaccount,dxtype,NumRgusLastRecord,ChurnedRGUs
,CASE WHEN v.ChurnedRGUs >= l.NumRgusLastRecord THEN 1 ELSE 0 END AS Vol_Flag
FROM CHURNEDRGUS_SO v INNER JOIN RGUSLastRecordDNA_Adj l ON CAST(v.account_id AS VARCHAR)=l.fixedaccount
AND v.month = l.Month
INNER JOIN LastRecordDateDNA d on l.fixedaccount=d.fixedaccount AND l.Month = d.Month
INNER JOIN OverdueLastRecordDNA o ON l.fixedaccount = o.fixedaccount AND l.month = o.Month
)
SELECT DISTINCT Month,fixedaccount,dxtype,vol_flag,NumRgusLastRecord,ChurnedRGUs
--Month, DxType, Vol_Flag,count(distinct fixedaccount) as Records
FROM VoluntaryFlag
WHERE month=date('2022-02-01')
--GROUP BY 1,2,3
ORDER BY 1,2

