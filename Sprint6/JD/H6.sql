------H6------

WITH TEch_filter as (
Select *,
     Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
        When pd_bb_accs_media = 'HFC' Then '2. HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as Technology
FROM "db-analytics-dev"."dna_fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
AND act_acct_typ_grp ='MAS MOVIL'

)
,DNA as (
Select act_acct_cd
from TEch_filter
WHERE Technology  IS NOT NULL
GROUP BY act_acct_cd

)
,clean_interaction_time as (
select *
FROM "db-stage-prod"."interactions_cwp"
    WHERE cast(INTERACTION_START_TIME as varchar) != ' '
    AND(INTERACTION_START_TIME IS NOT NULL)
    AND INTERACTION_ID NOT LIKE '%-%'
)
,interactions_ticket as (
SELECT ACCOUNT_ID, DATE_TRUNC ('Month',cast(substr(cast(interaction_start_time as varchar),1,10) as date)) AS INTERACTION_MONTH,other_interaction_info8
FROM clean_interaction_time
WHERE interaction_purpose_descrip = 'TRUCKROLL'
AND interaction_status ='CLOSED'
)
,Join_interaction as (
SELECT a.*, b.*
FROM interactions_ticket AS a
LEFT JOIN DNA AS b
ON a.ACCOUNT_ID = b.act_acct_cd
)
Select 
INTERACTION_MONTH,other_interaction_info8, COUNT (*) AS COUNTING
from Join_interaction
WHERE
INTERACTION_MONTH = DATE ('2022-02-01') AND
act_acct_cd IS NOT NULL
GROUP BY INTERACTION_MONTH, other_interaction_info8
order by 1,3 desc

------¿Qué cuentas agrupo?---------

--LIMIT 10

/*
--------------------Cliente reagenda cita (57)--------------------------------
Select distinct(other_interaction_info8), count (account_id)
FROM "db-stage-prod"."interactions_cwp"
group by 1
order by 2 desc
Limit 100
------------------------Cita Incumplida(92)----------------------------------
Select distinct(other_interaction_info5), count (account_id)
FROM "db-stage-prod"."interactions_cwp"
group by 1
order by 2 desc
Limit 100
------------------------------Cita Incumplida(34)-----------------------------
Select distinct(other_interaction_info6), count (account_id)
FROM "db-stage-prod"."interactions_cwp"
group by 1
order by 2 desc
Limit 100
------------------------------------------------------------------------------

Select *
FROM "db-stage-prod"."interactions_cwp"
Limit 100
*/
