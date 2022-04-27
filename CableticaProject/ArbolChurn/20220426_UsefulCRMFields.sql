WITH 
TicketsUsefulFields AS (
SELECT DISTINCT CONTRATO, FECHA_APERTURA AS TicketDate,Motivo
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_TIQUETES_SERVICIO_2021-01_A_2021-11_D` 
WHERE CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL AND FECHA_APERTURA IS NOT NULL
)
,UsefulCRMFields AS(
SELECT DISTINCT Fecha_Extraccion, DATE_TRUNC(Fecha_Extraccion,Month) AS Month, act_acct_cd
,pd_vo_prod_id, pd_vo_prod_nm, pd_tv_prod_id,pd_tv_prod_cd, pd_bb_prod_id, pd_bb_prod_nm
,CASE WHEN pd_vo_prod_id IS NOT NULL THEN 1 ELSE 0 END AS numVO
,CASE WHEN pd_tv_prod_cd IS NOT NULL THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_bb_prod_id IS NOT NULL THEN 1 ELSE 0 END AS numBB
,vo_fi_tot_mrc_amt,vo_fi_tot_mrc_amt_desc,tv_fi_tot_mrc_amt,tv_fi_tot_mrc_amt_desc,bb_fi_tot_mrc_amt,bb_fi_tot_mrc_amt_desc
,tot_bill_amt,tot_desc_amt
,fi_outst_age, c_cust_age,Max(SAFE_CAST(act_acct_inst_dt AS DATE)) as MaxInst, SAFE_CAST(cst_chrn_dt AS DATE) AS ChurnDate
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` 
GROUP BY Fecha_Extraccion,Month,act_acct_cd,pd_vo_prod_id, pd_vo_prod_nm, pd_tv_prod_id,pd_tv_prod_cd, pd_bb_prod_id, pd_bb_prod_nm
,numVO,numTV,numBB,vo_fi_tot_mrc_amt,vo_fi_tot_mrc_amt_desc,tv_fi_tot_mrc_amt,tv_fi_tot_mrc_amt_desc,bb_fi_tot_mrc_amt,bb_fi_tot_mrc_amt_desc
,tot_bill_amt,tot_desc_amt,fi_outst_age,c_cust_age,ChurnDate
)
,DataBOM AS(
SELECT DISTINCT Fecha_Extraccion AS B_date, Month AS B_Month, act_acct_cd AS B_Account
,fi_outst_age AS B_Overdue, MaxInst AS B_MaxInst,DATE_DIFF(Fecha_Extraccion,MaxInst,DAY) AS B_TenureDays
,(numBB+numTV+numVO) as B_NumRGUs
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN "BO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN "TV"
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN "VO"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN "BO+TV"
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN "BO+VO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN "VO+TV"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN "BO+VO+TV"
    END AS B_MixName
,CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS B_MixCode
,ROUND((vo_fi_tot_mrc_amt-vo_fi_tot_mrc_amt_desc)+(tv_fi_tot_mrc_amt-tv_fi_tot_mrc_amt_desc)+(bb_fi_tot_mrc_amt-bb_fi_tot_mrc_amt_desc),2) AS B_MRC
,ROUND((tot_bill_amt-tot_desc_amt),2) AS B_Bill
,ChurnDate AS B_ChurnDate
FROM UsefulCRMFields
WHERE Fecha_Extraccion=Month
)
,dataEOM AS(
SELECT DISTINCT Fecha_Extraccion AS E_date, DATE_SUB(Month,INTERVAL 1 MONTH) AS E_Month, act_acct_cd AS E_Account
,fi_outst_age AS E_Overdue, MaxInst AS E_MaxInst,DATE_DIFF(Fecha_Extraccion,MaxInst,DAY) AS E_TenureDays
,(numBB+numTV+numVO) as E_NumRGUs
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN "BO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN "TV"
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN "VO"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN "BO+TV"
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN "BO+VO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN "VO+TV"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN "BO+VO+TV"
    END AS E_MixName
,CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS E_MixCode
,ROUND((vo_fi_tot_mrc_amt-vo_fi_tot_mrc_amt_desc)+(tv_fi_tot_mrc_amt-tv_fi_tot_mrc_amt_desc)+(bb_fi_tot_mrc_amt-bb_fi_tot_mrc_amt_desc),2) AS E_MRC
,ROUND((tot_bill_amt-tot_desc_amt),2) AS E_Bill
,ChurnDate AS E_ChurnDate
FROM UsefulCRMFields
WHERE Fecha_Extraccion=Month
)
,CustomerStatus AS(
  SELECT DISTINCT
  CASE WHEN (B_Account IS NOT NULL AND E_Account IS NOT NULL) OR (B_Account IS NOT NULL AND E_Account IS NULL) THEN b.B_Month
      WHEN (B_Account IS NULL AND E_Account IS NOT NULL) THEN e.E_Month
  END AS Month,
      CASE WHEN (B_Account IS NOT NULL AND E_Account IS NOT NULL) OR (B_Account IS NOT NULL AND E_Account IS NULL) THEN B_Account
      WHEN (B_Account IS NULL AND E_Account IS NOT NULL) THEN E_Account
  END AS Account
  ,CASE WHEN B_Account IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM
  ,CASE WHEN E_Account IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  b.* except (B_Account,B_Month), e.* except (E_Account, E_Month)
  FROM dataBOM b FULL OUTER JOIN dataEOM e
  ON b.B_Account = e.E_Account AND b.B_Month=e.E_Month
)

--Tecnologia sale de catalogo internet


SELECT * 
FROM CustomerStatus
