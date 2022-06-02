-----H4-----
WITH 
Tech_filter as (
Select distinct date_trunc('month',date(load_dt)) as month,date(load_dt) as load_dt, act_acct_cd
,Case when pd_bb_accs_media = 'FTTH' Then '1. FTTH'
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
select distinct *
FROM "db-stage-prod"."interactions_cwp"
    WHERE (CAST(INTERACTION_START_TIME AS VARCHAR) != ' ')
    AND(INTERACTION_START_TIME IS NOT NULL)
    AND INTERACTION_ID NOT LIKE '%-%'
)
,interactions_ticket as (
select distinct account_id, cast(substr(cast(interaction_start_time as varchar),1,10) as date) as interaction_start_time, cast(substr(cast(interaction_end_time as varchar),1,10) as date) as interaction_end_time, DATE_DIFF('day',cast(substr(cast(interaction_start_time as varchar),1,10) as date), cast(substr(cast(interaction_end_time as varchar),1,10) as date)) AS Duration, DATE_TRUNC ('Month',cast(substr(cast(interaction_start_time as varchar),1,10) as date)) AS Month
FROM clean_interaction_time
WHERE interaction_purpose_descrip = 'TICKET' AND interaction_status ='CLOSED'
)
,Join_DNA AS(
select distinct i.*,technology
from interactions_ticket i inner join tech_filter t on i.account_id=t.act_acct_cd and i.month=t.month
)
select distinct month,technology,count(account_id) as Records
from Join_DNA
where duration>=4
group by 1,2
order by 1,2

