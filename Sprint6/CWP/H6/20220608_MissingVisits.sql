------H6------

WITH 
Tech_filter as (
Select distinct date_trunc('month',date(load_dt)) as month,date(load_dt) as load_dt, act_acct_cd
     ,Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
        When pd_bb_accs_media = 'HFC' Then '2. HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as Technology
FROM "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial'

)
,clean_interaction_time as (
select *
FROM "db-stage-prod"."interactions_cwp"
    WHERE cast(INTERACTION_START_TIME as varchar) != ' '
    AND(INTERACTION_START_TIME IS NOT NULL)
    AND INTERACTION_ID NOT LIKE '%-%'
)
,interactions_truckroll as (
SELECT ACCOUNT_ID, DATE_TRUNC ('Month',cast(substr(cast(interaction_start_time as varchar),1,10) as date)) AS Month,other_interaction_info8
FROM clean_interaction_time
WHERE interaction_purpose_descrip = 'TRUCKROLL'
AND interaction_status ='CLOSED'
AND other_interaction_info8 IN('Cliente reagenda cita','Cliente ausente','Cliente no deja entrar')
)
,Join_DNA AS(
select distinct i.*--,technology
from interactions_truckroll i inner join tech_filter t on i.account_id=t.act_acct_cd and i.month=t.month
)
--/*
Select 
Month,COUNT (distinct account_id) AS COUNTING
from Join_DNA
GROUP BY Month
order by 1 
