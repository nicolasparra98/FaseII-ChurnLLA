SELECT
/*CASE WHEN f.act_acct_cd IS NOT NULL AND l.act_acct_cd IS NOT NULL THEN "ActiveAllMonth"
     WHEN f.act_acct_cd IS NOT NULL AND l.act_acct_cd IS NULL THEN "Churner"
     WHEN f.act_acct_cd IS NULL AND l.act_acct_cd IS NOT NULL THEN "GrossAdd"
     ELSE "Null" END AS Status*/
CASE WHEN f.act_acct_cd IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM
,CASE WHEN l.act_acct_cd IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM
FROM FirstDayRGU f FULL JOIN LastDayRGU l ON f.act_acct_cd=l.act_acct_cd
