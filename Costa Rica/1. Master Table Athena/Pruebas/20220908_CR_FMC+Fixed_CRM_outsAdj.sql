WITH 
------------------------Fixed Useful Fields -------------------------------------------------------------
UsefulFields AS(
SELECT DISTINCT DATE_TRUNC('month',date(FECHA_EXTRACCION)) AS Month,date(FECHA_EXTRACCION) as fecha_extraccion, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, first_value(ACT_ACCT_INST_DT) over(partition by act_acct_cd order by fecha_extraccion) as MinInst, CST_CHRN_DT AS ChurnDate

, DATE_DIFF('day',date(OLDEST_UNPAID_BILL_DT_new2),date(FECHA_EXTRACCION)) AS MORA
, ACT_CONTACT_MAIL_1,round(VO_FI_TOT_MRC_AMT,0) AS mrcVO, round(BB_FI_TOT_MRC_AMT,0) AS mrcBB, round(TV_FI_TOT_MRC_AMT,0) AS mrcTV,round((VO_FI_TOT_MRC_AMT + BB_FI_TOT_MRC_AMT + TV_FI_TOT_MRC_AMT),0) as avgmrc, round(TOT_BILL_AMT,0) AS Bill, ACT_ACCT_SIGN_DT
  ,CASE WHEN pd_vo_prod_id IS NOT NULL and pd_vo_prod_id<>'' THEN 1 ELSE 0 END AS RGU_VO
  ,CASE WHEN pd_tv_prod_cd IS NOT NULL and pd_tv_prod_id<>'' THEN 1 ELSE 0 END AS RGU_TV
  ,CASE WHEN pd_bb_prod_id IS NOT NULL and pd_bb_prod_id<>'' THEN 1 ELSE 0 END AS RGU_BB
  ,CASE WHEN PD_VO_PROD_ID IS NOT NULL and pd_vo_prod_id<>'' AND PD_BB_PROD_ID IS NOT NULL and pd_bb_prod_id<>'' AND PD_TV_PROD_ID IS NOT NULL and pd_tv_prod_id<>'' THEN '3P'
        WHEN (PD_VO_PROD_ID IS NULL or pd_vo_prod_id='')  AND PD_BB_PROD_ID IS NOT NULL and pd_bb_prod_id<>'' AND PD_TV_PROD_ID IS NOT NULL and pd_tv_prod_id<>'' THEN '2P'
        WHEN PD_VO_PROD_ID IS NOT NULL and pd_vo_prod_id<>'' AND (PD_BB_PROD_ID IS NULL or pd_bb_prod_id='') AND PD_TV_PROD_ID IS NOT NULL and pd_tv_prod_id<>'' THEN '2P'
        WHEN PD_VO_PROD_ID IS NOT NULL and pd_vo_prod_id<>'' AND PD_BB_PROD_ID IS NOT NULL and pd_bb_prod_id<>'' AND (PD_TV_PROD_ID IS NULL or pd_tv_prod_id='') THEN '2P'
ELSE '1P' END AS MIX
from "lla_cco_int_san"."dna_fixed_historic_cr_billfix"  
where date(fecha_extraccion) between (DATE('2022-04-01') + interval '1' MONTH - interval '2' MONTH) AND  (DATE('2022-04-01') + interval '1' MONTH)
)
,CustomerBase_BOM AS(
SELECT *
 ,CASE WHEN B_Tech_Type IS NOT NULL THEN B_Tech_Type
       WHEN B_Tech_Type IS NULL AND cast(B_RGU_TV AS varchar)='NEXTGEN TV' THEN 'FTTH'
 ELSE 'HFC' END AS B_TechAdj
 ,CASE WHEN B_Tenure <=6 THEN 'Early Tenure'
       WHEN B_Tenure >6 THEN 'Late Tenure'
 ELSE NULL END AS B_FixedTenureSegment
from(SELECT DISTINCT Month, Fecha_Extraccion AS B_DATE, c.act_acct_cd AS AccountBOM, pd_vo_prod_id as B_VO_id, pd_vo_prod_nm as B_VO_nm, pd_tv_prod_id AS B_TV_id, pd_tv_prod_cd as B_TV_nm, pd_bb_prod_id as B_BB_id, pd_bb_prod_nm as B_BB_nm, RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MinInst as B_MinInst, MIX AS B_MIX,RGU_VO + RGU_TV + RGU_BB AS B_NumRGUs,Tipo_Tecnologia AS B_Tech_Type, MORA AS B_MORA, mrcVO as B_VO_MRC, mrcBB as B_BB_MRC, mrcTV as B_TV_MRC, avgmrc as B_AVG_MRC,BILL AS B_BILL_AMT,ACT_ACCT_SIGN_DT AS B_ACT_ACCT_SIGN_DT
  ,CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN '1P'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN '2P'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN '3P' END AS B_Bundle_Type
  ,CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS B_BundleName
  ,CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_BOM
  ,CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_BOM
  ,CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_BOM
  ,CASE WHEN (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 0) OR  (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 0 AND RGU_TV = 0 AND RGU_VO = 1)  THEN '1P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 1) OR (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 1) THEN '2P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 1) THEN '3P' END AS B_MixCode_Adj
FROM UsefulFields c LEFT JOIN "lla_cco_int_san"."catalogue_tv_internet_cr"  ON PD_BB_PROD_nm=ActivoInternet
WHERE FECHA_EXTRACCION=DATE_TRUNC('month',FECHA_EXTRACCION) )
)
,CustomerBase_EOM AS(
select *
 ,CASE WHEN E_Tech_Type IS NOT NULL THEN E_Tech_Type
       WHEN E_Tech_Type IS NULL AND cast(E_RGU_TV AS varchar)='NEXTGEN TV' THEN 'FTTH'
 ELSE 'HFC' END AS E_TechAdj
 ,CASE WHEN E_Tenure <=6 THEN 'Early Tenure'
       WHEN E_Tenure >6 THEN 'Late Tenure'
 ELSE NULL END AS E_FixedTenureSegment
from(SELECT DISTINCT date_add('month',-1,Month) as Month, Fecha_Extraccion as E_Date, c.act_acct_cd as AccountEOM, pd_vo_prod_id as E_VO_id, pd_vo_prod_nm as E_VO_nm, pd_tv_prod_cd AS E_TV_id, pd_tv_prod_cd as E_TV_nm, pd_bb_prod_id as E_BB_id, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, C_CUST_AGE as E_Tenure, MinInst as E_MinInst, MIX AS E_MIX,RGU_VO + RGU_TV + RGU_BB AS E_NumRGUs,Tipo_Tecnologia AS E_Tech_Type, MORA AS E_MORA, mrcVO AS E_VO_MRC, mrcBB as E_BB_MRC, mrcTV as E_TV_MRC, avgmrc as E_AVG_MRC, BILL AS E_BILL_AMT,ACT_ACCT_SIGN_DT AS E_ACT_ACCT_SIGN_DT
  ,CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN '1P'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN '2P'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN '3P' END AS E_Bundle_Type,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS E_BundleName
  ,CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_EOM
  ,CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_EOM
  ,CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_EOM
  ,CASE WHEN (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 0) OR  (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 0 AND RGU_TV = 0 AND RGU_VO = 1)  THEN '1P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 1) OR (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 1) THEN '2P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 1) THEN '3P' END AS E_MixCode_Adj
FROM UsefulFields c LEFT JOIN "lla_cco_int_san"."catalogue_tv_internet_cr" ON PD_BB_PROD_nm=ActivoInternet
WHERE FECHA_EXTRACCION=DATE_TRUNC('month',FECHA_EXTRACCION) )
)
,FixedCustomerBase AS(
    SELECT DISTINCT
    CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
   END AS Fixed_Month,
     CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS Fixed_Account,
   CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
   CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
   B_Date, B_VO_id, B_VO_nm, B_TV_id, B_TV_nm, B_BB_id, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MinInst, B_Bundle_Type, B_BundleName,B_MIX, B_TechAdj,B_FixedTenureSegment, B_MORA, B_VO_MRC, B_BB_MRC, B_TV_MRC, B_AVG_MRC, B_BILL_AMT,B_ACT_ACCT_SIGN_DT,BB_RGU_BOM,TV_RGU_BOM,VO_RGU_BOM,B_MixCode_Adj,
   E_Date, E_VO_id, E_VO_nm, E_TV_id, E_TV_nm, E_BB_id, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MinInst, E_Bundle_Type, E_BundleName,E_MIX, E_TechAdj,E_FixedTenureSegment, E_MORA, E_VO_MRC, E_BB_MRC, E_TV_MRC, E_AVG_MRC, E_BILL_AMT,E_ACT_ACCT_SIGN_DT,BB_RGU_EOM,TV_RGU_EOM,VO_RGU_EOM,E_MixCode_Adj
  FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)
-----------------------------Main Movements------------------------------------------------------------
,MAINMOVEMENTBASE AS(
 SELECT f.*
 ,CASE WHEN (E_NumRGUs - B_NumRGUs)=0 THEN 'Same RGUs'
       WHEN (E_NumRGUs - B_NumRGUs)>0 THEN 'Upsell'
       WHEN (E_NumRGUs - B_NumRGUs)<0 then 'Downsell'
       WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC('month',E_ACT_ACCT_SIGN_DT) <> Fixed_Month) THEN 'Come Back to Life'
       WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC('month',E_ACT_ACCT_SIGN_DT) = Fixed_Month) THEN 'New Customer'
       WHEN ActiveBOM = 1 AND ActiveEOM = 0 THEN 'Loss'
 END AS MainMovement
 ,CASE WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC('month',E_MinInst) = date('2022-04-01') THEN 'June Gross-Ads'
       WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC('month',E_MinInst) <> date('2022-04-01') THEN 'ComeBackToLife/Rejoiners Gross-Ads'
 ELSE NULL END AS GainMovement
 ,coalesce(E_RGU_BB - B_RGU_BB,0) as DIF_RGU_BB ,coalesce(E_RGU_TV - B_RGU_TV,0) as DIF_RGU_TV ,coalesce(E_RGU_VO - B_RGU_VO,0) as DIF_RGU_VO,(E_NumRGUs - B_NumRGUs) as DIF_TOTAL_RGU
FROM FixedCustomerBase f
)
,SPINMOVEMENTBASE AS (
SELECT b.*,
 CASE WHEN MainMovement='Same RGUs' AND (E_BILL_AMT - B_BILL_AMT) > 0 THEN '1. Up-spin' 
      WHEN MainMovement='Same RGUs' AND (E_BILL_AMT - B_BILL_AMT) < 0 THEN '2. Down-spin' 
 ELSE '3. No Spin' END AS SpinMovement
FROM MAINMOVEMENTBASE b
)
------------------------------------Fixed Churn Flags----------------------------------------------
,ServiceOrders AS (
SELECT * FROM "lla_cco_int_san"."so_fixed_historic_cr"
)
,MAX_SO_CHURN AS(
SELECT DISTINCT reverse(rpad(substr(reverse(nombre_contrato),1,10),10,'0')) as contratoso
, DATE_TRUNC('month',MAX(FECHA_APERTURA)) as DeinstallationMonth, MAX(FECHA_APERTURA) AS FECHA_CHURN
FROM ServiceOrders WHERE TIPO_ORDEN = 'DESINSTALACION' AND (ESTADO <> 'CANCELADA' OR ESTADO <> 'ANULADA') AND FECHA_APERTURA IS NOT NULL
 --and cast(nombre_contrato as varchar) NOT LIKE '%E%' -- temporal
GROUP BY 1
)
,CHURNERSSO AS(
SELECT DISTINCT reverse(rpad(substr(reverse(nombre_contrato),1,10),10,'0')) as contratoso,DATE_TRUNC('month',FECHA_APERTURA) as DeinstallationMonth,Fecha_apertura as DeinstallationDate,CASE WHEN submotivo='MOROSIDAD' THEN 'Involuntary'
       WHEN submotivo <> 'MOROSIDAD' THEN 'Voluntary' END AS Submotivo
FROM ServiceOrders t INNER JOIN MAX_SO_CHURN m on reverse(rpad(substr(reverse(nombre_contrato),1,10),10,'0'))= m.contratoso and fecha_apertura = fecha_churn
WHERE TIPO_ORDEN = 'DESINSTALACION' AND (ESTADO <> 'CANCELADA' OR ESTADO <> 'ANULADA') AND FECHA_APERTURA IS NOT NULL
--and cast(nombre_contrato as varchar) NOT LIKE '%E%' --temporal
)
,MaximaFecha as(
select distinct reverse(rpad(substr(reverse(cast(act_acct_cd as varchar)),1,10),10,'0')) as act_acct_cd, max(fecha_extraccion) as MaxFecha 
FROM "lla_cco_int_san"."dna_fixed_historic_cr"  group by 1
)
,ChurnersJoin as(
select Distinct f.Fecha_Extraccion,f.act_acct_cd,Submotivo,DeinstallationMonth,DeinstallationDate,MaxFecha 
FROM "lla_cco_int_san"."dna_fixed_historic_cr" f
left join churnersso c on contratoso=reverse(rpad(substr(reverse(cast(f.act_acct_cd as varchar)),1,10),10,'0'))
and date_trunc('month',fecha_extraccion)=DeinstallationMonth
left join MaximaFecha m on reverse(rpad(substr(reverse(cast(f.act_acct_cd as varchar)),1,10),10,'0'))=reverse(rpad(substr(reverse(cast(m.act_acct_cd as varchar)),1,10),10,'0'))
)
,MaxFechaJoin as(
select Fecha_extraccion,DeinstallationMonth as DxMonth,reverse(rpad(substr(reverse(cast(act_acct_cd as varchar)),1,10),10,'0')) as act_acct_cd
,CASE WHEN date_diff('month',DeinstallationMonth,MaxFecha)<=1 THEN Submotivo
ELSE NULL END AS FixedChurnTypeFlag
FROM Churnersjoin WHERE Submotivo IS NOT NULL
)
,ChurnersFixedTable as(
select f.*,FixedChurnTypeFlag,reverse(rpad(substr(reverse(cast(fixed_account as varchar)),1,10),10,'0'))
FROM SPINMOVEMENTBASE f left join MaxFechaJoin b
on Fixed_Month=date_trunc('month',b.DxMonth) and reverse(rpad(substr(reverse(cast(fixed_account as varchar)),1,10),10,'0'))= b.act_acct_cd
)
------------------------------------Rejoiners--------------------------------------------------------------
,InactiveUsersMonth AS (
SELECT DISTINCT Fixed_Month AS ExitMonth, Fixed_Account,DATE_ADD('MONTH',1,Fixed_Month) AS RejoinerMonth
FROM FixedCustomerBase 
WHERE ActiveBOM=1 AND ActiveEOM=0
)
,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Fixed_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
-- Variabilizar
,CASE WHEN RejoinerMonth>=date('2022-04-01') AND RejoinerMonth<=DATE_ADD('month',1,date('2022-04-01')) THEN 1 ELSE 0 END AS Fixed_PR
FROM FixedCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Fixed_Account=i.Fixed_Account AND Fixed_Month=ExitMonth
)
,FixedRejoinerFebPopulation AS(
SELECT DISTINCT Fixed_Month,RejoinerPopFlag,Fixed_PR,Fixed_Account,date('2022-04-01') AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1 AND Fixed_PR=1 AND Fixed_Month<>date('2022-04-01')
GROUP BY 1,2,3,4
)
,FullFixedBase_Rejoiners AS(
SELECT DISTINCT f.*,Fixed_PR
,CASE WHEN Fixed_PR=1 AND MainMovement='Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_Rejoiner
FROM ChurnersFixedTable f LEFT JOIN FixedRejoinerFebPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=date(r.Month)
)
,FinalTable as(
SELECT *,CASE
WHEN FixedChurnTypeFlag is not null THEN b_NumRGUs
WHEN MainMovement='Downsell' THEN (B_NumRGUs - coalesce(E_NumRGUs,0))
ELSE NULL END AS RGU_Churn,
CONCAT(coalesce(B_VO_nm,''),coalesce(B_TV_nm,''),coalesce(B_BB_nm,'')) AS B_PLAN,CONCAT(coalesce(E_VO_nm,''),coalesce(E_TV_nm,''),coalesce(E_BB_nm,'')) AS E_PLAN
FROM FullFixedBase_Rejoiners
)
,fixed_base as(
Select * from FinalTable
)
,Mobile_Base AS(
SELECT DISTINCT * FROM "lla_cco_int_stg"."cr_mob_stg_dashboardinput_sprint12_apr"
)
,EMAIL_BOM AS (
SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,'.','') as ID_ABONADO, act_acct_cd, NOM_EMAIL AS B_EMAIL
FROM "lla_cco_int_san"."dna_mobile_historic_cr" INNER JOIN "lla_cco_int_san"."dna_fixed_historic_cr" 
    ON cast(FECHA_EXTRACCION as varchar)=FECHA_PARQUE AND NOM_EMAIL=ACT_CONTACT_MAIL_1
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes' AND NOM_EMAIL <>'NOTIENE@GMAIL.COM' AND NOM_EMAIL<> 'NOREPORTA@CABLETICA.COM' AND NOM_EMAIL<>'NOREPORTACORREO@CABLETICA.COM'
)
,NEARFMC_MOBILE_BOM AS (
SELECT DISTINCT a.*,b.act_acct_cd AS B_CONTR, b.B_EMAIL
FROM Mobile_Base a LEFT JOIN EMAIL_BOM b ON ID_ABONADO=Mobile_Account AND cast(FECHA_PARQUE as varchar)=cast(Mobile_Month as varchar)
)
,EMAIL_EOM AS (
SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,'.','') as ID_ABONADO, act_acct_cd, NOM_EMAIL AS E_EMAIL
FROM "lla_cco_int_san"."dna_mobile_historic_cr" INNER JOIN "lla_cco_int_san"."dna_fixed_historic_cr" 
    ON cast(FECHA_EXTRACCION as varchar)=FECHA_PARQUE AND NOM_EMAIL=ACT_CONTACT_MAIL_1
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes' AND NOM_EMAIL <>'NOTIENE@GMAIL.COM' AND NOM_EMAIL<> 'NOREPORTA@CABLETICA.COM' AND NOM_EMAIL<>'NOREPORTACORREO@CABLETICA.COM'
)
,NEARFMC_MOBILE_EOM AS (
SELECT DISTINCT a.*,b.act_acct_cd AS E_CONTR, E_EMAIL
FROM NEARFMC_MOBILE_BOM a LEFT JOIN EMAIL_EOM b ON ID_ABONADO=Mobile_Account AND DATE_ADD('month',-1,date(FECHA_PARQUE))=Mobile_Month
)
,CONTRATO_ADJ AS (
SELECT a.*
,CASE WHEN B_FMCAccount IS NOT NULL THEN cast(B_FMCAccount as varchar)
    WHEN B_CONTR IS NOT NULL THEN cast(B_CONTR as varchar)
 ELSE NULL END AS B_Mobile_Contrato_Adj,
 CASE WHEN E_FMCAccount IS NOT NULL THEN cast(E_FMCAccount as varchar)
    WHEN E_CONTR IS NOT NULL THEN cast(E_CONTR as varchar)
 ELSE NULL END AS E_Mobile_Contrato_Adj
FROM NEARFMC_MOBILE_EOM a
)
,MobilePreliminaryBase AS (
SELECT a.*
 ,CASE WHEN B_Mobile_Contrato_Adj IS NOT NULL THEN B_Mobile_Contrato_Adj
      WHEN E_Mobile_Contrato_Adj IS NOT NULL THEN E_Mobile_Contrato_Adj
  END AS Mobile_Contrato_Adj
FROM CONTRATO_ADJ a
)
,AccountFix AS (
SELECT DISTINCT Mobile_Contrato_Adj, Mobile_Month, COUNT(*) as FixedCount
FROM MobilePreliminaryBase
GROUP BY Mobile_Contrato_Adj, Mobile_Month
)
,JoinAccountFix AS (
SELECT m.*, b.FixedCount
FROM MobilePreliminaryBase m LEFT JOIN AccountFix b ON m.Mobile_Contrato_Adj=b.Mobile_Contrato_Adj AND m.Mobile_Month=b.Mobile_month
)
-----------------------------------------Join Fixed Mobile-------------------------------------------------
,FullCustomerBase AS(
select CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) OR (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN cast(Fixed_Month as varchar)
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN cast(Mobile_Month as varchar)
  END AS Month
,CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) OR (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN cast(Fixed_Account as varchar)
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN cast(Mobile_Account as varchar)
  END AS Final_Account
,CASE WHEN (ActiveBOM =1 AND Mobile_ActiveBOM=1) or (ActiveBOM=1 AND (Mobile_ActiveBOM=0 or Mobile_ActiveBOM IS NULL)) or ((ActiveBOM=0 OR ActiveBOM IS NULL) AND Mobile_ActiveBOM=1) THEN 1
ELSE 0 END AS Final_BOM_ActiveFlag
,CASE WHEN (ActiveEOM =1 AND Mobile_ActiveEOM=1) or (ActiveEOM=1 AND (Mobile_ActiveEOM=0 or Mobile_ActiveEOM IS NULL)) or ((ActiveEOM=0 OR ActiveEOM IS NULL) AND Mobile_ActiveEOM=1) THEN 1
ELSE 0 END AS Final_EOM_ActiveFlag
,CASE WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveBOM = 1 and Mobile_ActiveBOM = 1 AND B_FMCAccount IS NOT NULL ) THEN 'Soft FMC'
      WHEN (B_EMAIL IS NOT NULL AND B_FMCAccount IS NULL AND ActiveBOM=1) OR (ActiveBOM = 1 and Mobile_ActiveBOM = 1) THEN 'Near FMC'
      WHEN (Fixed_Account IS NOT NULL AND ActiveBOM=1 AND (Mobile_ActiveBOM = 0 OR Mobile_ActiveBOM IS NULL))  THEN 'Fixed Only'
      WHEN ((Mobile_Account IS NOT NULL AND Mobile_ActiveBOM=1 AND (ActiveBOM = 0 OR ActiveBOM IS NULL)))  THEN 'Mobile Only'
 END AS B_FMC_Status
,CASE WHEN FixedCount IS NULL THEN 1
      WHEN FixedCount IS NOT NULL THEN FixedCount
End as ContractsFix
,CASE WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveEOM = 1 and Mobile_ActiveEOM = 1 AND E_FMCAccount IS NOT NULL ) THEN 'Soft FMC'
      WHEN (E_EMAIL IS NOT NULL AND E_FMCAccount IS NULL) OR (ActiveEOM = 1 and Mobile_ActiveEOM = 1 ) THEN 'Near FMC'
      WHEN (Fixed_Account IS NOT NULL AND ActiveEOM=1 AND (Mobile_ActiveEOM = 0 OR Mobile_ActiveEOM IS NULL))  THEN 'Fixed Only'
      WHEN (Mobile_Account IS NOT NULL AND Mobile_ActiveEOM=1 AND (ActiveEOM = 0 OR ActiveEOM IS NULL )) AND MobileChurntype IS NULL THEN 'Mobile Only'
 END AS E_FMC_Status 
,fixed_month,fixed_account,activebom,activeeom,b_date,b_vo_id,b_vo_nm,b_tv_id,b_tv_nm,b_bb_id,b_bb_nm,b_rgu_vo,b_rgu_tv,b_rgu_bb,b_numrgus,b_overdue,b_tenure,b_mininst,b_bundle_type,b_bundlename,b_mix,b_techadj,b_fixedtenuresegment,b_mora,b_vo_mrc,b_bb_mrc,b_tv_mrc,b_avg_mrc,b_bill_amt,b_act_acct_sign_dt,bb_rgu_bom,tv_rgu_bom,vo_rgu_bom,b_mixcode_adj,e_date,e_vo_id,e_vo_nm,e_tv_id,e_tv_nm,e_bb_id,e_bb_nm,e_rgu_vo,e_rgu_tv,e_rgu_bb,e_numrgus,e_overdue,e_tenure,e_mininst,e_bundle_type,e_bundlename,e_mix,e_techadj,e_fixedtenuresegment,e_mora,e_vo_mrc,e_bb_mrc,e_tv_mrc,e_avg_mrc,e_bill_amt,e_act_acct_sign_dt,bb_rgu_eom,tv_rgu_eom,vo_rgu_eom,e_mixcode_adj,mainmovement,gainmovement,dif_rgu_bb,dif_rgu_tv,dif_rgu_vo,dif_total_rgu,spinmovement,fixedchurntypeflag,fixed_pr,fixed_rejoiner,rgu_churn,b_plan,e_plan,mobile_month,mobile_account,mobile_activebom,mobile_activeeom,b_fmcaccount,e_fmcaccount,mobile_mrc_bom,mobile_mrc_eom,b_mobile_maxstart,e_mobile_maxstart,mobile_b_tenuredays,b_mobiletenuresegment,mobile_e_tenuredays,e_mobiletenuresegment,mobilemovementflag,mobile_mrc_diff,mobilespinflag,mobilechurnflag,mobilechurntype,mobile_prmonth,mobile_rejoinermonth,Mobile_Contrato_Adj,FixedCount
FROM Fixed_Base f FULL OUTER JOIN JoinAccountFix m ON cast(Fixed_Account as varchar)=Mobile_Contrato_Adj AND Fixed_Month=Mobile_Month
)
,CustomerBase_FMC_Tech_Flags AS(
SELECT t.*,round(round(coalesce(B_BILL_AMT/ContractsFix,0)) + coalesce(Mobile_MRC_BOM,0),0) AS TOTAL_B_MRC,round(round(coalesce(E_BILL_AMT/ContractsFix,0)) + coalesce(Mobile_MRC_EOM,0),0) AS TOTAL_E_MRC
,CASE WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '1P' THEN 'Fixed 1P'
 WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '2P' THEN 'Fixed 2P'
 WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '3P' THEN 'Fixed 3P'
 WHEN (B_FMC_Status = 'Soft FMC' ) AND (ActiveBOM = 0 OR ActiveBOM is null) then 'Mobile Only'
 WHEN B_FMC_Status = 'Mobile Only' THEN B_FMC_Status
 WHEN (B_FMC_Status='Near FMC' OR  B_FMC_Status='Soft FMC') THEN B_FMC_Status
 END AS B_FMCType
 ,CASE WHEN Final_EOM_ActiveFlag = 0 AND ((ActiveEOM = 0 AND FixedChurnTypeFlag IS NULL) OR (Mobile_ActiveEOM = 0 AND MobileChurntype is null)) THEN 'Customer Gap'
 WHEN E_FMC_Status = 'Fixed Only' AND FixedChurnTypeFlag IS NOT NULL THEN NULL
 WHEN E_FMC_Status = 'Mobile Only' AND MobileChurntype IS NOT NULL THEN NULL
 WHEN (E_FMC_Status = 'Fixed Only'  )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurntype IS NOT NULL))  AND E_MIX = '1P' THEN 'Fixed 1P'
 WHEN (E_FMC_Status = 'Fixed Only' )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurntype IS NOT NULL)) AND E_MIX = '2P' THEN 'Fixed 2P'
 WHEN (E_FMC_Status = 'Fixed Only' )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurntype IS NOT NULL)) AND E_MIX = '3P' THEN 'Fixed 3P'
 WHEN (E_FMC_Status = 'Soft FMC' OR E_FMC_Status = 'Near FMC' OR E_FMC_Status = 'Undefined FMC'  ) AND (ActiveEOM = 0 OR ActiveEOM is null OR (ActiveEOM = 1 AND FixedChurnTypeFlag IS NOT NULL)) then 'Mobile Only'
 WHEN E_FMC_Status = 'Mobile Only' OR((ActiveEOM is null or activeeom=0) and(Mobile_ActiveEOM=1)) THEN 'Mobile Only'
 WHEN E_FMC_Status='Soft FMC' AND (FixedChurnTypeFlag IS NULL AND MobileChurntype IS NULL AND Fixed_Account IS NOT NULL  AND ActiveEOM=1 AND Mobile_ActiveEOM=1 ) THEN E_FMC_Status
 WHEN E_FMC_Status='Near FMC' AND (FixedChurnTypeFlag IS NULL AND MobileChurntype IS NULL  AND Fixed_Account IS NOT NULL  AND ActiveEOM=1 AND Mobile_ActiveEOM=1) THEN E_FMC_Status
 WHEN E_FMC_Status='Undefined FMC' AND (FixedChurnTypeFlag IS NULL AND MobileChurntype IS NULL  AND Fixed_Account IS NOT NULL AND  ActiveEOM=1 AND Mobile_ActiveEOM=1) THEN E_FMC_Status
 END AS E_FMCType
,case when Mobile_ActiveBOM=1 then 1 else 0 end as B_MobileRGUs
,case when Mobile_ActiveEOM=1 then 1 else 0 end as E_MobileRGUs
 FROM FullCustomerBase  t
)
,CustomerBase_FMCSegments_ChurnFlag AS(
SELECT c.*
,CASE WHEN (B_FMC_Status='Fixed Only') OR ((B_FMC_Status='Soft FMC' OR B_FMC_Status='Near FMC' OR B_FMC_Status='Undefined FMC') AND ACTIVEBOM = 1 AND Mobile_ActiveBOM = 1) THEN B_TechAdj
 WHEN B_FMC_Status = 'Mobile Only' OR ((B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' OR B_FMC_Status='Undefined FMC') AND (ACTIVEBOM = 0 or ACTIVEBOM IS NULL)) THEN 'Wireless'
 END AS B_FinalTechFlag
,CASE WHEN (E_FMC_Status = 'Fixed Only' AND FixedChurnTypeFlag is null) OR ((E_FMC_Status = 'Soft FMC' OR E_FMC_Status='Near FMC' OR E_FMC_Status='Undefined FMC' ) AND ACTIVEEOM = 1 AND Mobile_ActiveEOM = 1 AND FixedChurnTypeFlag is null) THEN E_TechAdj
 WHEN E_FMC_Status='Mobile Only' OR ((E_FMC_Status='Soft FMC' OR E_FMC_Status='Near FMC' OR E_FMC_Status='Undefined FMC') AND (ACTIVEEOM = 0 OR ActiveEOM IS NULL)) THEN 'Wireless'
 END AS E_FinalTechFlag
,CASE WHEN (B_FixedTenureSegment='Late Tenure' and B_MobileTenureSegment='Late Tenure') OR (B_FixedTenureSegment='Late Tenure' and B_MobileTenureSegment is null) or (B_FixedTenureSegment IS NULL and B_MobileTenureSegment='Late Tenure') THEN 'Late Tenure'
 WHEN (B_FixedTenureSegment='Early Tenure' OR B_MobileTenureSegment='Early Tenure') THEN 'Early Tenure'
 END AS B_TenureFinalFlag
,CASE WHEN (E_FixedTenureSegment='Late Tenure' and E_MobileTenureSegment='Late Tenure') OR (E_FixedTenureSegment='Late Tenure' and E_MobileTenureSegment is null) or (E_FixedTenureSegment IS NULL and E_MobileTenureSegment='Late Tenure') THEN 'Late Tenure'
 WHEN (E_FixedTenureSegment='Early Tenure' OR E_MobileTenureSegment =  'Early Tenure') THEN 'Early Tenure'
 END AS E_TenureFinalFlag
,CASE WHEN (B_FMCType='Soft FMC' OR B_FMCType = 'Near FMC' OR B_FMCType = 'Undefined FMC') AND B_MIX = '1P'  THEN 'P2'
 WHEN (B_FMCType = 'Soft FMC' OR B_FMCType = 'Near FMC' OR B_FMCType = 'Undefined FMC') AND B_MIX = '2P' THEN 'P3'
 WHEN (B_FMCType = 'Soft FMC' OR B_FMCType = 'Near FMC' OR B_FMCType = 'Undefined FMC') AND B_MIX = '3P' THEN 'P4'
 WHEN (B_FMCType = 'Fixed 1P' OR B_FMCType = 'Fixed 2P' OR B_FMCType = 'Fixed 3P') OR ((B_FMCType = 'Soft FMC' OR B_FMCType='Near FMC' OR B_FMCType='Undefined FMC') AND(Mobile_ActiveBOM= 0 OR Mobile_ActiveBOM IS NULL)) AND ActiveBOM = 1 THEN 'P1_Fixed'
WHEN (B_FMCType = 'Mobile Only')  OR (B_FMCType = 'Soft FMC' AND(ActiveBOM= 0 OR ActiveBOM IS NULL)) AND Mobile_ActiveBOM = 1 THEN 'P1_Mobile'
END AS B_FMC_Segment
,CASE WHEN E_FMCType='Customer Gap' THEN 'Customer Gap' 
WHEN (E_FMCType = 'Soft FMC' OR E_FMCType='Near FMC' OR E_FMCType='Undefined FMC') AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = '1P' AND (FixedChurnTypeFlag IS NULL and MobileChurntype IS NULL) THEN 'P2'
WHEN (E_FMCType = 'Soft FMC' OR E_FMCType='Near FMC' OR E_FMCType='Undefined FMC') AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = '2P' AND (FixedChurnTypeFlag IS NULL and MobileChurntype IS NULL) THEN 'P3'
WHEN (E_FMCType = 'Soft FMC' OR E_FMCType='Near FMC' OR E_FMCType='Undefined FMC') AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = '3P' AND (FixedChurnTypeFlag IS NULL and MobileChurntype IS NULL) THEN 'P4'
WHEN ((E_FMCType = 'Fixed 1P' OR E_FMCType = 'Fixed 2P' OR E_FMCType = 'Fixed 3P') OR ((E_FMCType = 'Soft FMC' OR E_FMCType='Near FMC' OR E_FMCType='Undefined FMC') AND(Mobile_ActiveEOM= 0 OR Mobile_ActiveEOM IS NULL))) AND (ActiveEOM = 1 AND FixedChurnTypeFlag IS NULL) THEN 'P1_Fixed'
WHEN ((E_FMCType = 'Mobile Only')  OR (E_FMCType  = 'Soft FMC' AND(ActiveEOM= 0 OR ActiveEOM IS NULL))) AND (Mobile_ActiveEOM = 1 and MobileChurntype IS NULL) THEN 'P1_Mobile'
END AS E_FMC_Segment
,CASE WHEN (FixedChurnTypeFlag is not null  AND (ActiveBOM IS NULL OR ACTIVEBOM = 0)) OR (MobileChurntype is not null and (Mobile_ActiveBOM = 0 or Mobile_ActiveBOM IS NULL)) THEN 'Churn Exception'
WHEN (FixedChurnTypeFlag is not null and MobileChurntype is not null) then 'Churner'
WHEN (FixedChurnTypeFlag is not null and MobileChurntype is null) then 'Fixed Churner'
WHEN FixedChurnTypeFlag is null and activebom=1 and mobile_activebom=1 AND (activeeom=0 or activeeom is null) and (Mobile_ActiveEOM=0 or mobile_activeeom Is null) THEN 'Full Churner'
WHEN (FixedChurnTypeFlag is null and MobileChurntype is NOT null) then 'Mobile Churner'
WHEN ActiveBom=1 AND ActiveEOM=0 AND Mobile_Month IS NULL THEN 'Fixed churner - Customer Gap'
ELSE 'Non Churner' END AS FinalChurnFlag
,((coalesce(B_NumRGUs,0) + coalesce(B_MobileRGUs,0))/ContractsFix) as B_TotalRGUs
,((coalesce(E_NumRGUs,0) + coalesce(E_MobileRGUs,0))/ContractsFix) AS E_TotalRGUs
,round(coalesce(TOTAL_E_MRC,0) - coalesce(TOTAL_B_MRC,0),0) AS MRC_Change
FROM CustomerBase_FMC_Tech_Flags c
)
,RejoinerColumn AS (
SELECT DISTINCT  f.*,CASE WHEN Fixed_Rejoiner = 1 AND E_FMC_Segment = 'P1_Fixed' THEN 'Fixed Rejoiner'
WHEN (Fixed_Rejoiner = 1) OR ((Fixed_Rejoiner = 1) and  (E_FMCType = 'Soft FMC' OR E_FMCType = 'Near FMC')) THEN 'FMC Rejoiner'
WHEN Mobile_Rejoinermonth IS NOT NULL AND E_FMC_Segment = 'P1_Mobile' THEN 'Mobile Rejoiner'
END AS Rejoiner_FinalFlag
FROM CustomerBase_FMCSegments_ChurnFlag f
)
--------------------------------------Waterfall------------------------------------
,FullCustomersBase_Flags_Waterfall AS(
SELECT DISTINCT 
Month,Final_Account,Final_BOM_ActiveFlag,Final_EOM_ActiveFlag,B_FMC_Status,ContractsFix,E_FMC_Status,fixed_month,fixed_account,activebom,activeeom,b_date,b_vo_id,b_vo_nm,b_tv_id,b_tv_nm,b_bb_id,b_bb_nm,b_rgu_vo,b_rgu_tv,b_rgu_bb,b_numrgus,b_overdue,b_tenure,b_mininst,b_bundle_type,b_bundlename,b_mix,b_techadj,b_fixedtenuresegment,b_mora,b_vo_mrc,b_bb_mrc,b_tv_mrc,b_avg_mrc,b_bill_amt,b_act_acct_sign_dt,bb_rgu_bom,tv_rgu_bom,vo_rgu_bom,b_mixcode_adj,e_date,e_vo_id,e_vo_nm,e_tv_id,e_tv_nm,e_bb_id,e_bb_nm,e_rgu_vo,e_rgu_tv,e_rgu_bb,e_numrgus,e_overdue,e_tenure,e_mininst,e_bundle_type,e_bundlename,e_mix,e_techadj,e_fixedtenuresegment,e_mora,e_vo_mrc,e_bb_mrc,e_tv_mrc,e_avg_mrc,e_bill_amt,e_act_acct_sign_dt,bb_rgu_eom,tv_rgu_eom,vo_rgu_eom,e_mixcode_adj,mainmovement,gainmovement,dif_rgu_bb,dif_rgu_tv,dif_rgu_vo,dif_total_rgu,spinmovement,fixedchurntypeflag,fixed_pr,fixed_rejoiner,rgu_churn,b_plan,e_plan,mobile_month,mobile_account,mobile_activebom,mobile_activeeom,b_fmcaccount,e_fmcaccount,mobile_mrc_bom,mobile_mrc_eom,b_mobile_maxstart,e_mobile_maxstart,mobile_b_tenuredays,b_mobiletenuresegment,mobile_e_tenuredays,e_mobiletenuresegment,mobilemovementflag,mobile_mrc_diff,mobilespinflag,mobilechurnflag,mobilechurntype,mobile_prmonth,mobile_rejoinermonth,Mobile_Contrato_Adj,FixedCount,TOTAL_B_MRC,TOTAL_E_MRC,B_FMCType,E_FMCType,B_MobileRGUs,E_MobileRGUs,B_FinalTechFlag,E_FinalTechFlag,B_TenureFinalFlag,E_TenureFinalFlag,B_FMC_Segment,FinalChurnFlag,B_TotalRGUs,E_TotalRGUs,MRC_Change,Rejoiner_FinalFlag
,CASE WHEN (FinalChurnFlag = 'Churner') OR ((FinalChurnFlag = 'Fixed Churner' OR FinalChurnFlag = 'Fixed churner - Customer Gap') and B_FMC_Segment = 'P1_Fixed' and (E_FMC_Segment is null or E_FMC_Segment='Customer Gap')) OR (FinalChurnFlag = 'Mobile Churner' and B_FMC_Segment = 'P1_Mobile' and E_FMC_Segment is null) then 'Total Churner'
WHEN FinalChurnFlag = 'Non Churner' then null
ELSE 'Partial Churner' end as Partial_Total_ChurnFlag
--Arreglar cuando se tenga churn split
,CASE WHEN ((FinalChurnFlag='Full Churner' OR FinalChurnFlag='Fixed Churner' OR FinalChurnFlag='Fixed churner - Customer Gap' AND (Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL)) AND FixedChurnTypeFlag='Voluntario' AND MobileChurnFlag IS NULL) OR (MobileChurnFlag='BAJA VOLUNTARIA' AND(ActiveBOM=0 OR ActiveEOM IS NULL)) Then 'Voluntary'
WHEN ((FinalChurnFlag='Full Churner' OR FinalChurnFlag='Fixed Churner' OR FinalChurnFlag='Fixed churner - Customer Gap' AND (Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL)) AND FixedChurnTypeFlag='Involuntario' AND MobileChurnFlag IS NULL) OR ((MobileChurnFlag='BAJA INVOLUNTARIA' OR MobileChurnFlag='ALTA/MIGRACION') AND(ActiveBOM=0 OR ActiveEOM IS NULL)) Then 'Involuntary'
--WHEN (ActiveEOM=0 OR ActiveEOM IS NULL) AND MobileChurnFlag IS NOT NULL THEN "TBD"
End as churntypefinalflag
,CASE WHEN (B_FMCTYPE='Fixed 1P' OR B_FMCType='Fixed 2P' OR B_FMCType='Fixed 3P') AND E_FMCType='Mobile Only' AND FinalChurnFlag<>'Churn Exception' AND FinalChurnFlag<>'Customer Gap' AND FinalChurnFlag<>'Fixed Churner' THEN 'Customer Gap'
ELSE E_FMC_Segment END AS E_FMC_Segment
,CASE WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs < E_NumRGUs) AND ((Mobile_ActiveBOM=Mobile_ActiveEOM) OR (Mobile_ActiveBOM is null and Mobile_ActiveEOM is null) ) THEN 'Upsell'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs > E_NumRGUs) AND ((Mobile_ActiveBOM=Mobile_ActiveEOM) OR (Mobile_ActiveBOM is null and Mobile_ActiveEOM is null) ) THEN 'Downsell'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (TOTAL_B_MRC = TOTAL_E_MRC) THEN 'Maintain'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND ((MainMovement = 'New Customer' AND MobileMovementFlag = '3.Gross Add/ rejoiner') OR (MainMovement = 'New Customer' AND MobileMovementFlag IS NULL) OR (MainMovement IS NULL AND MobileMovementFlag = '3.Gross Add/ rejoiner')) THEN 'Fixed Gross Add or Mobile Gross Add/ Rejoiner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (TOTAL_B_MRC> TOTAL_E_MRC ) THEN 'Downspin'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (TOTAL_B_MRC < TOTAL_E_MRC) THEN 'Upspin'
WHEN Rejoiner_FinalFlag IS NOT NULL THEN Rejoiner_FinalFlag
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (ACTIVEBOM IS NULL AND ACTIVEEOM IS NULL) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) THEN 'Maintain'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (FinalChurnFlag='Fixed Churner') AND ((Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) OR (Mobile_ActiveBOM IS NULL AND Mobile_ActiveEOM IS NULL) ) THEN 'Fixed Churner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag =0) AND (FinalChurnFlag='Fixed Churner') AND ((Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) OR (Mobile_ActiveBOM IS NULL AND Mobile_ActiveEOM IS NULL) ) THEN 'Fixed Churner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag =1) AND (ActiveBOM=0 AND ActiveEOM=1) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0) THEN 'Fixed Gross Add or Mobile Gross Add/ Rejoiner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (FinalChurnFlag='Fixed Churner') AND (Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1) THEN 'Fixed to Mobile Customer Gap'
WHEN FinalChurnFlag='Full Churner' Then 'Full Churner'
WHEN FinalChurnFlag='Mobile Churner' Then 'Mobile Churner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag =1) AND (ActiveBOM=1 AND ActiveEOM=0) AND FinalChurnFlag='Non Churner' THEN 'Churn Gap'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1) AND (ActiveBOM=1 AND ActiveEOM=1) THEN 'Mobile Gross Adds'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (ActiveBOM=1 AND ActiveEOM=1) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0) THEN 'Mobile Churner'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND E_FMC_Segment='P1_Fixed' AND Rejoiner_FinalFlag is null then 'Fixed Gross Add or Mobile Gross Add/ Rejoiner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (ActiveBOM=0 AND ActiveEOM=1) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) THEN 'FMC Packing'
WHEN B_FMC_Status='Undefined FMC' OR E_FMC_Status='Undefined FMC' THEN 'Gap Undefined FMC'
WHEN FinalChurnFlag='Customer Gap' THEN 'Customer Gap'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1)  AND (ActiveBOM=0 AND ActiveEOM=1) AND (Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1) THEN 'FMC Gross Add'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1)  AND (ActiveBOM=0 AND ActiveEOM=1) AND FixedChurnTypeFlag IS NOT NULL THEN 'Customer Gap'
END AS Waterfall_Flag
FROM RejoinerColumn f
)
,Last_Flags as(
select *
,Case when waterfall_flag='Downsell' and MainMovement='Downsell' then 'Voluntary'
      when waterfall_flag='Downsell' and FinalChurnFlag <> 'Non Churner' then ChurnTypeFinalFlag
      when waterfall_flag='Downsell' and mainmovement='Loss' then 'Undefined'
else null end as Downsell_Split
,case when waterfall_flag='Downspin' then 'Voluntary' else null end as Downspin_Split
from FullCustomersBase_Flags_Waterfall
)
select distinct --month,e_fmc_segment,count(distinct final_account)
month,FixedChurnTypeFlag,count(distinct fixed_account)
from last_flags
--limit 1000
--*/
--where --date(month)=date('2022-04-01') and e_fmc_segment is null and (Partial_Total_ChurnFlag<>'Total Churner'or Partial_Total_ChurnFlag is null)
--and Final_EOM_ActiveFlag=1
where activebom=1
group by 1,2 order by 1,2
