WITH
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d')) as Mes

FROM "lla_cco_int_ext"."cwp_con_ext_fmc" 

WHERE telefonia='Pospago' AND "unidad de negocio"='1. B2C' 
 AND DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d'))=DATE('2022-02-01') or DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d'))=DATE('2022-01-01')
)

,MobileUsefulFields AS(
SELECT DATE(dt) AS DT, DATE_TRUNC('MONTH', DATE(dt)) AS MobileMonth
,ACCOUNTNO AS MobileAccount
, CAST(SERVICENO AS INT) AS PhoneNumber
,MAX(CAST(DATE_PARSE(STARTDATE_ACCOUNTNO, '%Y.%m.%d %T') AS DATE)) AS MaxStart
,ACCOUNTNAME AS Mob_AccountName,NUMERO_IDENTIFICACION as Mobile_Id
,CAST(TOTAL_MRC_D AS DECIMAL) AS Mobile_MRC
,CAST(DATE_PARSE(INV_EXP_DT, '%Y.%m.%d %T') AS DATE) AS MobilePay_Dt
FROM "db-analytics-dev"."tbl_postpaid_cwp"
WHERE "biz_unit_d"='B2C' AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN', 'RESTRICTED') AND INV_EXP_DT<>'nan' 
 --AND date(dt) between (DATE('2022-02-01') + interval '1' MONTH - interval '1' DAY - interval '3' MONTH) AND  (DATE('2022-02-01') + interval '1' MONTH - interval '1' DAY + interval '3' MONTH)
GROUP BY DT,2,3,
4,ACCOUNTNAME,7,8,9
)
--Select *from mobileusefulfields where Mobileaccount = '1762634' and MobileMonth = date('2022-02-01')
,NumberRGUsPerUser AS(
SELECT DISTINCT MobileMonth,dt,MobileAccount,count(distinct PHONENUMBER) AS NumRGUs
FROM MobileUsefulFields
GROUP BY MobileMonth,dt,MobileAccount
)
--Select sum (Numrgus) from numberrgusperuser where MobileMonth = date('2022-02-01') --4.751.968
--Select count(*) from numberrgusperuser where MobileMonth = date('2022-02-01') -- 5.125.245
--Select * from NumberRGUsPerUser  where MobileMonth = date('2022-02-01') order by NumRgus desc
,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS Month, MobileAccount, Round(avg(Mobile_MRC),0) AS AvgMRC_Mobile
  FROM MobileUsefulFields 
  WHERE Mobile_MRC IS NOT NULL AND Mobile_MRC <> 0
  GROUP BY 1, MobileAccount
)
,MobileActive_BOM AS(
SELECT m.DT AS B_Date, DATE_TRUNC('MONTH', DATE_ADD('MONTH', 1, DATE(m.dt))) AS MobileMonth,
m.MobileAccount as MobileBOM, PhoneNumber as Phone_BOM, MaxStart as Mobile_B_MaxStart
, Mob_AccountName as B_Mob_Acc_Name, Mobile_Id as B_Mobile_ID
, round(Mobile_MRC,0) as B_MobileMRC
, NumRGUs AS B_MobileRGUs, round(AvgMRC_Mobile,0) as B_AvgMobileMRC
,CASE WHEN DATE_DIFF('DAY',MaxStart, m.dt)<=180 THEN 'Early-Tenure'
      WHEN DATE_DIFF('DAY',MaxStart, m.dt)>180 THEN 'Late-Tenure' END AS B_MobileTenure
FROM MobileUsefulFields m INNER JOIN NumberRGUsPerUser r ON m.MobileAccount = r.MobileAccount AND m.dt = r.dt
LEFT JOIN AverageMRC_User a ON m.MobileAccount = a.MobileAccount AND  m.MobileMonth = a.Month
WHERE DATE(m.dt)= DATE_TRUNC('MONTH', DATE(m.dt)) + interval '1' MONTH - interval '1' day
)

,MobileActive_EOM AS(
SELECT m.DT AS E_Date, DATE_TRUNC('MONTH', DATE(m.dt)) AS MobileMonth,
m.MobileAccount as MobileEOM, PhoneNumber as Phone_EOM, MaxStart as Mobile_E_MaxStart
, Mob_AccountName as E_Mob_Acc_Name, Mobile_Id as E_Mobile_ID
, round(Mobile_MRC,0) as E_MobileMRC
, NumRGUs AS E_MobileRGUs, round(AvgMRC_Mobile,0) as E_AvgMobileMRC
,CASE WHEN DATE_DIFF('DAY',MaxStart, m.dt)<=180 THEN 'Early-Tenure'
      WHEN DATE_DIFF('DAY', MaxStart, m.dt)>180 THEN 'Late-Tenure' END AS E_MobileTenure
FROM MobileUsefulFields m INNER JOIN NumberRGUsPerUser r ON m.MobileAccount = r.MobileAccount AND m.dt = r.dt
LEFT JOIN AverageMRC_User a ON m.MobileAccount = a.MobileAccount AND  m.MobileMonth = a.Month
WHERE DATE(m.dt) = DATE_TRUNC('MONTH', DATE(m.dt)) + interval '1' MONTH - interval '1' day
)
,MobileCustomerStatus AS(

  SELECT DISTINCT
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN b.MobileMonth
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN e.MobileMonth
  END AS Mobile_Month,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN mobileBOM
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN mobileEOM
  END AS Mobile_Account,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN Phone_BOM
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN Phone_EOM
  END AS PhoneNumber,
  CASE WHEN mobileBOM IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveBOM,
  CASE WHEN mobileEOM IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveEOM,
  b.*, e.* 
  FROM MobileActive_BOM as b FULL OUTER JOIN MobileActive_EOM as e on b.MobileBOM = e.MobileEOM and b.MobileMonth = e.MobileMonth

)
,MainMovementBase AS(
SELECT DISTINCT *
,CASE WHEN (E_MobileRGUs - B_MobileRGUs) = 0 THEN '1.SameRGUs' 
      WHEN (E_MobileRGUs - B_MobileRGUs) > 0 THEN '2.Upsell'
      WHEN (E_MobileRGUs - B_MobileRGUs) < 0 THEN '3.Downsell'
      WHEN (B_MobileRGUs IS NULL AND E_MobileRGUs > 0 AND DATE_TRUNC('MONTH', Mobile_E_MaxStart) = DATE('2022-02-01')) THEN '4.New Customer'
      WHEN (B_MobileRGUs IS NULL AND E_MobileRGUs > 0 AND DATE_TRUNC('MONTH', Mobile_E_MaxStart) <> DATE('2022-02-01')) THEN '5.Come Back to Life'
      WHEN (B_MobileRGUs > 0 AND E_MobileRGUs IS NULL) THEN '6.Null last day'
      WHEN B_MobileRGUs IS NULL AND E_MobileRGUs IS NULL THEN '7.Always null'
 END AS MobileMainMovement
FROM MobileCustomerStatus
)
,SpinClass AS(
SELECT DISTINCT *, ROUND((E_MobileMRC - B_MobileMRC),0) AS Mobile_MRC_Diff,
      CASE WHEN MobileMainMovement ='1.SameRGUs' AND (E_MobileMRC - B_MobileMRC)=0 THEN '1.Same'
      WHEN MobileMainMovement ='1.SameRGUs' AND (E_MobileMRC - B_MobileMRC)>0 THEN '2.Upspin'
      WHEN MobileMainMovement ='1.SameRGUs' AND (E_MobileMRC - B_MobileMRC)<0 THEN '3.Downspin'
      ELSE '4.NoSpin' END AS MobileSpinFlag
FROM MainMovementBase 
)

-- ################################CONVERGENCY#################################################################

,MobileConvergency AS(
SELECT DISTINCT m.*, c.household_id as Mobile_household_id
 ,CASE WHEN Tipo='1. Inscrito a Paquete completo' OR Tipo='2. Beneficio manual' THEN '1.Soft FMC'
       WHEN Tipo='2. Match_ID' OR Tipo='3. Contact number' THEN '2.Near FMC'
       WHEN household_id IS NULL THEN '4. MobileOnly'
       ELSE '3.Mobile-HardBundle' END AS FmcFlagMob
FROM SpinClass m LEFT JOIN Convergente c ON m.PhoneNumber=c.SERVICE_ID AND m.Mobile_Month=c.Mes
)

-- #################################CHURNERS################################################################
,calculated_fields as (    
SELECT *,
date_add('day', -fi_outs_age_raw, dt) as OLDEST_UNPAID_BILL_CORRECTED_raw
,case when date_add('day',-1,date(concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle,2,'0')))) >  date_add('day', -fi_outs_age_raw, dt) then  date_add('day',-1,date(concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle,2,'0')))) - interval '1' month 
else date_add('day',-1,date(concat(substring(cast(date_add('day', -fi_outs_age_raw, dt) as varchar),1,7),'-',lpad(billcycle,2,'0')))) end as OLDEST_UNPAID_BILL_CORRECTED_corrected
FROM (select date(dt) as dt, accountno, billcycle , cast(cast(billcycle as int)-1 as varchar) as billcycle_m1
         ,CASE WHEN cast(cast(flg_outstd_d as double) as int) = 0 THEN 0 ELSE (SUM (cast(cast(flg_outstd_d as double) as int)) OVER (PARTITION BY accountno,inv_paymt_dt ORDER BY dt ASC)) END as fi_outs_age_raw
    FROM "db-analytics-dev"."tbl_postpaid_cwp"
    where date(dt) between  date('2022-02-01') and date('2022-07-25') and 
    billcycle IN ('1','2','7','15','21','28')
)order by dt desc
)
,corrected_age as (
select *,
date_diff('day',OLDEST_UNPAID_BILL_CORRECTED_corrected, dt) as fi_outs_age_corrected
from calculated_fields
)
,InvoluntaryChurners as (
SELECT date_trunc('month',dt) as invmonth,accountno
from corrected_age
where fi_outs_age_corrected=75
)
,ChurnerTypeFlag AS(
SELECT f.*
,CASE WHEN MobileMainMovement='6.Null last day' OR i.AccountNo IS NOT NULL THEN '1. Mobile Churner'
      ELSE '2. Mobile NonChurner' END AS MobileChurnFlag
,CASE WHEN MobileMainMovement='6.Null last day' AND i.AccountNo IS NOT NULL THEN '3. Mobile Mixed Churner'
      WHEN MobileMainMovement='6.Null last day' AND i.AccountNo IS NULL THEN '1. Mobile Voluntary Churner' 
      WHEN i.AccountNo IS NOT NULL AND MobileMainMovement<>'6.Null last day' THEN '2. Mobile Involuntary Churner'
END AS MobileChurnerType
FROM MobileConvergency f LEFT JOIN InvoluntaryChurners i ON f.Mobile_Account=i.AccountNo AND f.Mobile_Month=invmonth
)
,FullMobileBase AS(
SELECT DISTINCT Mobile_Month,Mobile_Account,PhoneNumber,Mobile_ActiveBOM
,CASE WHEN Mobile_ActiveEOM=1 AND MobileChurnFlag='2. Mobile NonChurner' THEN 1
      ELSE 0 END AS Mobile_ActiveEOM
,B_Date,Phone_BOM,Mobile_B_MaxStart,B_Mob_Acc_Name,B_Mobile_ID,B_MobileMRC,B_MobileRGUs,B_AvgMobileMRC,B_MobileTenure
,E_Date,Phone_EOM,Mobile_E_MaxStart,E_Mob_Acc_Name,E_Mobile_ID,E_MobileMRC
,CASE WHEN E_MobileRGUs=1 AND MobileChurnFlag='2. Mobile NonChurner' THEN 1
      ELSE 0 END AS E_MobileRGUs
,E_AvgMobileMRC,E_MobileTenure,Mobile_MRC_Diff
,CASE WHEN MobileChurnFlag='1. Mobile Churner' THEN '6.Null last day'
      ELSE MobileMainMovement END AS MobileMainMovement
,CASE WHEN MobileChurnFlag='1. Mobile Churner' THEN '4.NoSpin'
      ELSE MobileSpinFlag END AS MobileSpinFlag
,mobile_household_id,FmcFlagMob,MobileChurnFlag,MobileChurnerType
FROM ChurnerTypeFlag
)

,InactiveUsers AS (
SELECT DISTINCT Mobile_Month AS ExitMonth, Mobile_Account,DATE_ADD('MONTH',1,date(Mobile_Month)) AS RejoinerMonth
FROM MobileCustomerStatus
WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)
,RejoinerPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
,CASE WHEN RejoinerMonth>=date('2022-02-01') AND RejoinerMonth<=DATE_ADD('MONTH',1,date('2022-02-01')) THEN 1 ELSE 0 END AS Mobile_PRMonth
FROM FullMobileBase f LEFT JOIN InactiveUsers i ON f.Mobile_Account=i.Mobile_Account AND Mobile_Month=ExitMonth
)

,MobileRejoinerPopulation AS(
SELECT DISTINCT Mobile_Month,RejoinerPopFlag,Mobile_PRMonth,Mobile_Account,date('2022-02-01') AS Month
FROM RejoinerPopulation
WHERE RejoinerPopFlag=1
AND Mobile_PRMonth=1
AND Mobile_Month<>date('2022-02-01')
GROUP BY 1,2,3,4
)

,MonthMobileRejoiners AS(
SELECT f.*,Mobile_PRMonth
,CASE WHEN Mobile_PRMonth=1 AND MobileMainMovement='5.Come Back to Life'
THEN 1 ELSE 0 END AS Mobile_RejoinerMonth
FROM FullMobileBase f LEFT JOIN MobileRejoinerPopulation r ON f.Mobile_Account=r.Mobile_Account AND f.Mobile_Month=CAST(r.Month AS DATE)
)
,fmc_table as(
select distinct mobile_month,mobile_account,phonenumber,mobile_activebom,mobile_activeeom,b_date,phone_bom,mobile_b_maxstart,b_mob_acc_name,b_mobile_id,b_mobilemrc,b_mobilergus,b_avgmobilemrc,b_mobiletenure,e_date,phone_eom,mobile_e_maxstart,e_mob_acc_name,e_mobile_id,e_mobilemrc,e_mobilergus,e_avgmobilemrc,e_mobiletenure,mobile_mrc_diff,mobilemainmovement,mobilespinflag,mobile_household_id,fmcflagmob,drc,mobilechurnflag,mobilechurnertype,mobile_rejoinermonth
--mobile_month,MobileChurnerType,mobile_account
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where month=date(dt)
--and mobilechurnertype='2. Mobile Involuntary Churner'
--and mobile_account in('1440381','1732187','1783903','1728001','1709526')
order by 2,1
)
--,new_churners as(
SELECT distinct mobile_month,MobileChurnerType,count(distinct mobile_account)
from MonthMobileRejoiners
where mobile_month>=date('2022-01-01')
and mobilechurnertype='2. Mobile Involuntary Churner'
group by 1,2 order by 1,2
)
select distinct *
from fmc_table f left join new_churners c on f.mobile_account=c.mobile_account --and f.mobile_month=c.mobile_month
where f.mobile_account --in('1440381','1732187','1783903','1728001','1709526')
in('1890736','1708674','1874851','796060','1753025')

--and c.mobile_month>=date('2022-01-01')
--where f.mobile_account is null
and f.mobile_month=date('2022-06-01')
--group by 1,2 
--order by 1,2
