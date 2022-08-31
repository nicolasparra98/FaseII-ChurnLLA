with
----------------------------------Mobile Useful Fields---------------------------------------
MobileFix AS (
select *,case when length(fec_actcen)=19 then concat(substr(fec_actcen,4,2),'/',substr(fec_actcen,1,2),'/',substr(fec_actcen,7,4)) when length(fec_actcen)=9 then lpad(fec_actcen,10,'0') else fec_actcen end as fec_actcen_adj
from(SELECT date(FECHA_PARQUE) as fecha_parque, ID_ABONADO, NUM_DIAS_MOROSO, NUM_IDENT, numContrato, idCliente, CONVERGENTE,NUM_TELEFONO,ID_ESTATUS_OPERACION, ID_CLIENTE, ID_CICLO, ID_RANGO_MOROSIDAD, DES_RANGO_MOROSIDAD,BAN_SIN_SALDO_TOTAL, DEUDA_VENCIDA_ACTUAL, DIRECCION_CORREO,NUM_TELF_CONTACTO, NUM_VECES_MOROSO, DES_SEGMENTO_CLIENTE, DES_CATEGORIA_CLIENTE, DES_USO, ID_PLAN_TARIFARIO, DES_PLAN_TARIFARIO, NOM_EMAIL,DES_PRODUCTO, ID_PLAN_TARIFARIO_1, AGRUPACION_COMUNIDAD, CANAL, ID_RANGO_MOROSIDAD_1, CON_TERMINAL,DES_EQUIPO
  ,CASE WHEN fec_actcen='#N/D' THEN NULL ELSE fec_actcen END AS fec_actcen
  ,CASE WHEN renta IN('#N/D','NULL') THEN NULL ELSE renta END AS renta
FROM "lla_cco_int_san"."dna_mobile_historic_cr"
where date(fecha_parque) between (DATE('2022-06-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  (DATE('2022-06-01') + interval '1' MONTH - interval '1' DAY) )
)
,MobileUsefulFields AS (
SELECT DISTINCT DATE_TRUNC('month',FECHA_PARQUE) AS Month,numContrato AS Contrato, ID_CLIENTE,NUM_IDENT, replace(ID_ABONADO,'.','') as ID_ABONADO,cast(replace(Renta,',','.') as double) AS Renta ,AGRUPACION_COMUNIDAD,date(date_parse(fec_actcen_adj,'%d/%m/%Y')) as fec_actcen
FROM MobileFix WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes'
)
,CustomerBase_BOM AS(
SELECT DISTINCT date_add('Month',1,Month) AS B_Month, Contrato as B_Contrato, ID_CLIENTE as B_IdCliente, NUM_IDENT as B_NumIdent, ID_ABONADO as B_Mobile_Account,Renta AS Mobile_MRC_BOM, AGRUPACION_COMUNIDAD as B_Agrupacion, fec_actcen as B_Mobile_MaxStart
FROM MobileUsefulFields
)
,CustomerBase_EOM AS(
SELECT DISTINCT Month AS E_Month, Contrato as E_Contrato, ID_CLIENTE as E_IdCliente,NUM_IDENT as E_NumIdent,ID_ABONADO as E_Mobile_Account,Renta AS Mobile_MRC_EOM, AGRUPACION_COMUNIDAD as E_Agrupacion,fec_actcen as E_Mobile_MaxStart
FROM MobileUsefulFields
)
,BaseMovimientos AS(
SELECT *,date(date_parse(concat(mes,'01'),'%Y%m%d')) as Month 
FROM "lla_cco_int_san"."mobile_movements_cr"
)
,MobileCustomerBase AS (
SELECT DISTINCT
    CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND 
    E_Mobile_Account IS NULL) THEN B_Month
    WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN E_Month
    END AS Mobile_Month,
    CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND 
    E_Mobile_Account IS NULL) THEN B_Mobile_Account
    WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN E_Mobile_Account
    END AS Mobile_Account,
    CASE WHEN B_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveBOM,
    CASE WHEN E_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveEOM,
    B_Contrato as B_FMCAccount,E_Contrato as E_FMCAccount, Mobile_MRC_BOM, Mobile_MRC_EOM, B_Mobile_MaxStart,E_Mobile_MaxStart
FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON B_Mobile_Account=E_Mobile_Account AND B_Month=E_Month
)
,FlagTenureCustomerBase AS (
SELECT DISTINCT *, date_diff('month',date(B_Mobile_MaxStart),date(Mobile_Month)) AS Mobile_B_TenureDays
 ,CASE WHEN date_diff('month',date(B_Mobile_MaxStart),date(Mobile_Month)) <6 THEN 'Early Tenure'
       WHEN date_diff('month',date(B_Mobile_MaxStart),date(Mobile_Month)) >=6 THEN 'Late Tenure'
ELSE NULL END AS B_MobileTenureSegment
,date_diff('month',date(E_Mobile_MaxStart),date(Mobile_Month)) AS Mobile_E_TenureDays
,CASE WHEN date_diff('month',date(E_Mobile_MaxStart),date(Mobile_Month)) <6 THEN 'Early Tenure'
      WHEN date_diff('month',date(E_Mobile_MaxStart),date(Mobile_Month)) >=6 THEN 'Late Tenure'
ELSE NULL END AS E_MobileTenureSegment 
FROM MobileCustomerBase
)
----------------------------------- Main Movements--------------------------------------------------------
,MainMovements AS (
SELECT DISTINCT *,CASE 
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(Mobile_MRC_BOM=Mobile_MRC_EOM) THEN '01.Maintain'
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(Mobile_MRC_BOM>Mobile_MRC_EOM) THEN '02.Downspin'
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(Mobile_MRC_BOM<Mobile_MRC_EOM) THEN '03.Upspin'
    WHEN  Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =0 THEN '04.Loss'
    WHEN (Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL)  AND Mobile_ActiveEOM=1 AND E_Mobile_MaxStart <>date('2022-06-01') THEN '05.Come Back To Life'
    WHEN (Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL)  AND Mobile_ActiveEOM=1 AND E_Mobile_MaxStart =date('2022-06-01') THEN '07.New Customer'
    WHEN (Mobile_MRC_BOM IS NULL OR Mobile_MRC_EOM IS NULL) THEN '08.MRC Gap'
  ELSE NULL END AS MobileMovementFlag
,(coalesce(Mobile_MRC_EOM,0)-coalesce(Mobile_MRC_BOM,0)) as Mobile_MRC_Diff
,Case WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(Mobile_MRC_BOM=Mobile_MRC_EOM) THEN '01.NoSpin'
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(Mobile_MRC_BOM>Mobile_MRC_EOM) THEN '02.Downspin'
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(Mobile_MRC_BOM<Mobile_MRC_EOM) THEN '03.Upspin'
  end as MobileSpinFlag
FROM FlagTenureCustomerBase
)
-------------------------------------------- Churners -------------------------------------------------------
,MobileChurners AS (
SELECT *, '1. Mobile Churner' as MobileChurnFlag 
FROM MainMovements WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)
,ChurnersMovements AS (
SELECT M.*,TIPO_BAJA AS MobileChurnType 
FROM MobileChurners m LEFT JOIN BaseMovimientos ON Mobile_Account=ID_Abonado AND Mobile_Month=Month
)
,CustomerBaseWithChurn AS (
SELECT DISTINCT m.*,case when mobilechurnflag is not null then MobileChurnFlag else '2. Mobile NonChurner' end as MobileChurnFlag,c.MobileChurnType
FROM MainMovements m LEFT JOIN ChurnersMovements c ON m.Mobile_Account=c.Mobile_Account and c.Mobile_Month=m.Mobile_Month
)
----------------------------------------- Rejoiners --------------------------------------------------------------
,InactiveUsersMonth AS (
SELECT DISTINCT Mobile_Month AS ExitMonth, Mobile_Account,DATE_ADD('month',1,Mobile_Month) AS RejoinerMonth
FROM MobileCustomerBase 
WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)
,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
  -- Variabilizar
,CASE WHEN RejoinerMonth>=date('2022-06-01') AND RejoinerMonth<=DATE_ADD('month',1,date('2022-06-01')) THEN 1 ELSE 0 END AS Mobile_PRMonth
FROM MobileCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Mobile_Account=i.Mobile_Account AND Mobile_Month=ExitMonth
)
,FixedRejoinerFebPopulation AS(
SELECT DISTINCT Mobile_Month,RejoinerPopFlag,Mobile_PRMonth,Mobile_Account,date('2022-06-01') AS Month
FROM RejoinersPopulation WHERE RejoinerPopFlag=1 AND Mobile_PRMonth=1 AND Mobile_Month<>date('2022-06-01')
GROUP BY 1,2,3,4
)
,FullFixedBase_Rejoiners AS(
SELECT DISTINCT f.*,Mobile_PRMonth,CASE WHEN Mobile_PRMonth=1 AND MobileMovementFlag='05.Come Back To Life' THEN f.Mobile_Account ELSE NULL END AS Mobile_RejoinerMonth
FROM CustomerBaseWithChurn f 
 LEFT JOIN FixedRejoinerFebPopulation r ON f.Mobile_Account=r.Mobile_Account AND f.Mobile_Month=r.Month
)
select *
from fullfixedbase_rejoiners
