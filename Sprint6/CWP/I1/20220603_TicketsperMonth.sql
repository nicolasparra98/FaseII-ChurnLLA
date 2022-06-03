-----I1----
WITH 
check_tech as (
Select distinct date_trunc('month',date(load_dt)) as month,date(load_dt) as load_dt, act_acct_cd
        ,Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
        When pd_bb_accs_media = 'HFC' Then '2. HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as Technology
FROM "db-analytics-dev"."dna_fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
)
,clean_interaction_time as (
select distinct *
FROM "db-stage-prod"."interactions_cwp"
    WHERE cast(INTERACTION_START_TIME as varchar) != ' '
    AND(INTERACTION_START_TIME IS NOT NULL)
    AND INTERACTION_ID NOT LIKE '%-%'
)
,interactions as (
select distinct *,
    CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE) AS INTERACTION_DATE, DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) AS month
    FROM clean_interaction_time
)
,Tickets_per_month as (
select distinct month,interaction_id
FROM interactions
WHERE INTERACTION_PURPOSE_DESCRIP = 'TICKET' 
GROUP BY month,interaction_id
)

select distinct month,count(distinct interaction_id) AS Tickets
from Tickets_per_month
group by 1
order by 1
