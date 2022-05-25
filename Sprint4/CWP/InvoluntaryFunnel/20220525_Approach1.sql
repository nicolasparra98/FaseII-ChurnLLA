WITH
InitialBase AS(
SELECT DISTINCT DATE(DATE_TRUNC('MONTH',LOAD_DT)) AS Month,ACT_ACCT_CD
FROM "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial' AND DATE(LOAD_DT)=DATE(DATE_TRUNC('MONTH',LOAD_DT)) AND (FI_OUTST_AGE<=90 OR FI_OUTST_AGE IS NULL)
)
,UsersAndDueDays AS(
SELECT DISTINCT DATE(DATE_TRUNC('MONTH',d.LOAD_DT)) AS Month,d.LOAD_DT,d.ACT_ACCT_CD,d.FI_OUTST_AGE AS DueDays
FROM "lla_cco_int_stg"."cwp_fix_union_dna" d INNER JOIN InitialBase i 
 ON d.ACT_ACCT_CD=i.ACT_ACCT_CD AND DATE(DATE_TRUNC('MONTH',d.LOAD_DT))=i.Month
WHERE act_cust_typ_nm = 'Residencial'
)
,BillingMonth AS(
SELECT DISTINCT *
FROM UsersAndDueDays
WHERE DueDays IS NULL OR DueDays=0
)
,OverdueMonth AS(
SELECT DISTINCT *
FROM UsersAndDueDays
WHERE DueDays=15
)
,SoftDxMonth AS(
SELECT DISTINCT *
FROM UsersAndDueDays
WHERE DueDays=46
)
,HardDxMonth AS(
SELECT DISTINCT DATE_TRUNC('MONTH', DATE_ADD('MONTH', 2, DATE(LOAD_dt))) AS Month,act_acct_cd
FROM UsersAndDueDays
WHERE DueDays=90
)
,Funnel AS (
SELECT DISTINCT a.Month AS InitialMonth, a.Act_acct_cd AS BillingAccount,i.act_acct_cd AS InitialAccount,b.Act_acct_cd AS OutstandingAccount,c.Act_acct_cd AS SoftDxAccount,d.Act_acct_cd AS HardDxAccount
FROM BillingMonth a 
 LEFT JOIN InitialBase i ON a.act_acct_cd=i.act_acct_cd and a.Month=i.Month
 LEFT JOIN OverdueMonth b ON a.act_acct_cd=b.act_acct_cd and a.Month=b.Month
 LEFT JOIN SoftDxMonth c ON a.act_acct_cd=c.act_acct_cd and a.Month=c.Month 
 LEFT JOIN HardDxMonth d ON a.act_acct_cd=d.act_acct_cd and a.Month=d.Month
)
SELECT DISTINCT InitialMonth, /*count(distinct BillingAccount),*/count(distinct InitialAccount) AS InitialBase, count(distinct OutstandingAccount) AS Dia_1_Mora, count(distinct SoftDxAccount) AS SoftDx, count(distinct HardDxAccount) AS HardDx
FROM Funnel
Group by 1
Order by 1
