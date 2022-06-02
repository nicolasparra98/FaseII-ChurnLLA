
   -----I1----
WITH check_tech as (
Select cast(concat(substr(cast(dt as varchar),1,4),substr(cast(dt as varchar),6,2),substr(cast(dt as varchar),9,2),act_acct_cd) as varchar) as Key_dna , act_acct_cd,

        Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
        When pd_bb_accs_media = 'HFC' Then '2. HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as Technology
    
FROM "db-analytics-dev"."dna_fixed_cwp"
), 
max_record as (
Select  
MAX(cast(concat(substr(cast(dt as varchar),1,4),substr(cast(dt as varchar),6,2),substr(cast(dt as varchar),9,2),act_acct_cd) as varchar)) as Key_max
from "db-analytics-dev"."dna_fixed_cwp"
WHERE act_acct_typ_grp IS NOT NULL
GROUP BY act_acct_cd
),
Join_DNA as (
SELECT a.*, b.*
FROM max_record AS a
LEFT JOIN check_tech AS b
    ON a.Key_max = b.Key_dna
),
clean_interaction_time as (
select *
FROM "db-stage-prod"."interactions_cwp"
    WHERE cast(INTERACTION_START_TIME as varchar) != ' '
    AND(INTERACTION_START_TIME IS NOT NULL)
    AND INTERACTION_ID NOT LIKE '%-%'
),
interactions as (
    select *,
    CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE) AS INTERACTION_DATE, DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) AS month
    FROM clean_interaction_time
),
Tickets_per_month as (
SELECT
ACCOUNT_ID, INTERACTION_DATE
FROM INTERACTIONS
WHERE INTERACTION_PURPOSE_DESCRIP = 'TICKET' 
GROUP BY ACCOUNT_ID, INTERACTION_DATE
),
Join_Filter_tech as (
Select
a.*,
b.*
FROM Tickets_per_month AS a
LEFT JOIN Join_DNA AS b
    ON a.ACCOUNT_ID = b.act_acct_cd
)
SELECT DATE_TRUNC('month',INTERACTION_DATE), COUNT (ACCOUNT_ID)
--*
FROM Join_Filter_tech
WHERE Technology IS NOT Null
GROUP BY DATE_TRUNC('month',INTERACTION_DATE)
order by DATE_TRUNC('month',INTERACTION_DATE)
/*

--Select dt
--FROM "db-analytics-dev"."dna_fixed_cwp"
--limit 10

--SELECT COUNT(DISTINCT(act_acct_cd))
--FROM "db-analytics-dev"."dna_fixed_cwp"
--WHERE act_acct_cd IN (SELECT ACCOUNT_ID FROM "db-stage-prod"."interactions_cwp")

-----------------I2----------------------

Columan NODE --¿?
/*

WITH Nodes_SIR AS (
SELECT node,act_acct_cd,
cantidad_tech_claims_calls__ultimo_mes,cantidad_tickets__ultimo_mes,
--CASE WHEN cantidad_tech_claims_calls__ultimo_mes > 0 THEN 1 ELSE 0 END AS flag
CASE WHEN cantidad_tickets__ultimo_mes > 0 THEN 1 ELSE 0 END AS flag
FROM "db-stage-prod"."scores_001_cwp"
WHERE CAST(dt as dATE) = DATE('2022-03-06')
AND node IS NOT NULL
),
flag_pannel as (
SELECT NODE, 
COUNT (act_acct_cd) TOTAL_CUSTOMERS,
sum(CASE WHEN flag = 1 then 1 else 0 end )as With_ticket,
(CAST(sum(CASE WHEN flag = 1 then 1 else 0 end) as double)/CAST(COUNT (act_acct_cd) as double)) as RATE
FROM Nodes_SIR
GROUP BY node
)

SELECT  
--*
COUNT (node)
FROM flag_pannel
WHERE rate >= 0.06

*/
