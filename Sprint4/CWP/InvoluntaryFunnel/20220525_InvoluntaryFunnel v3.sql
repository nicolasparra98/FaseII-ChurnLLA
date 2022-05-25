WITH
NoOverdueFirstDay AS(
SELECT DISTINCT DATE(DATE_TRUNC('MONTH',LOAD_DT)) AS Month,act_acct_cd
,Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
    ELSE 'COPPER' end as TechFlag
FROM "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial' AND DATE(LOAD_DT)=DATE(DATE_TRUNC('MONTH',LOAD_DT)) AND (FI_OUTST_AGE<=90 OR FI_OUTST_AGE IS NULL)
)
,InitialBase AS(
SELECT DISTINCT DATE(DATE_TRUNC('MONTH',d.LOAD_DT)) AS Month,DATE(d.LOAD_DT) AS Load_dt,d.act_acct_cd,d.fi_outst_age AS DueDays
,CASE WHEN ACT_BLNG_CYCL IN('A','B','C') THEN 14 ELSE 27 END AS FirstOverdueDay
FROM "lla_cco_int_stg"."cwp_fix_union_dna" d INNER JOIN NoOverdueFirstDay i 
 ON d.ACT_ACCT_CD=i.ACT_ACCT_CD AND DATE(DATE_TRUNC('MONTH',d.LOAD_DT))=i.Month
WHERE act_cust_typ_nm = 'Residencial' AND techflag IN('FTTH','HFC')
)
,UsersInOverdue AS(
SELECT DISTINCT Month,load_dt as OverdueDate,act_acct_cd,Duedays
FROM InitialBase
WHERE DueDays=FirstOverdueDay
)
,UsersSoftDx AS(
SELECT DISTINCT u.Month, OverdueDate,u.Duedays,u.act_acct_cd,i.Month AS SoftDXMonth, i.load_dt AS SoftDxDay,i.DueDays
FROM UsersInOverdue u INNER JOIN InitialBase i ON u.act_acct_cd=i.act_acct_cd
WHERE OverdueDate<i.load_dt AND DATE_DIFF('DAY',OverdueDate,load_dt)<=32 AND i.DueDays=46
)
,UsersHardDx AS(
SELECT DISTINCT u.Month,u.OverdueDate,u.SoftDxDay,u.act_acct_cd,i.Month AS HardDxMonth,i.load_dt AS HardDxDay
FROM UsersSoftDx u INNER JOIN InitialBase i ON u.act_acct_cd=i.act_acct_cd
WHERE SoftDxDay<i.load_dt AND DATE_DIFF('DAY',SoftDxDay,load_dt)<=44 AND i.DueDays=90
)
,Funnel AS(
SELECT DISTINCT i.Month AS InitialMonth,i.act_acct_cd AS InitialAccount,a.act_acct_cd AS OverdueAccount,b.act_acct_cd AS SoftDxAccount,c.act_acct_cd AS HardDxAccount
FROM InitialBase i
 LEFT JOIN UsersInOverdue a ON i.act_acct_cd=a.act_acct_cd AND i.Month=a.Month
 LEFT JOIN UsersSoftDx b ON i.act_acct_cd=b.act_acct_cd AND i.Month=b.Month
 LEFT JOIN UsersHardDx c ON i.act_acct_cd=c.act_acct_cd AND i.Month=c.Month
)
SELECT DISTINCT InitialMonth, COUNT(DISTINCT InitialAccount) AS InitialAccounts,COUNT(DISTINCT OverdueAccount) AS OverdueAccounts,COUNT(DISTINCT SoftDxAccount) AS SoftDx, COUNT(DISTINCT HardDxAccount) AS HardDx
FROM Funnel
GROUP BY 1 ORDER BY 1
