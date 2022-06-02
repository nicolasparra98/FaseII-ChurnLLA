-----H4-----
WITH TEch_filter as (
Select *,

Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
        When pd_bb_accs_media = 'HFC' Then '2. HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as Technology

/*CASE WHEN (PD_BB_ACCS_MEDIA='FTTH' OR PD_TV_ACCS_MEDIA ='FTTH' OR PD_VO_ACCS_MEDIA='FTTH') THEN 'FTTH'
      WHEN (PD_BB_ACCS_MEDIA='HFC' OR PD_TV_ACCS_MEDIA ='HFC' OR PD_VO_ACCS_MEDIA='HFC') THEN 'HFC'
      WHEN (PD_BB_ACCS_MEDIA='VDSL' OR PD_TV_ACCS_MEDIA ='VDSL' OR PD_VO_ACCS_MEDIA='VDSL' OR 
            PD_BB_ACCS_MEDIA='COPPER' OR PD_TV_ACCS_MEDIA ='COPPER' OR PD_VO_ACCS_MEDIA='COPPER') THEN 'COPPER'*/

/*Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'*/
   -- ELSE null end as Technology
FROM "db-analytics-dev"."dna_fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
AND act_acct_typ_grp ='MAS MOVIL'
),
DNA as (
Select act_acct_cd,
Technology
from TEch_filter
WHERE Technology  IS NOT NULL
GROUP BY act_acct_cd ,
Technology

),

clean_interaction_time as (
select *
FROM "db-stage-prod"."interactions_cwp"
    WHERE (CAST(INTERACTION_START_TIME AS VARCHAR) != ' ')
    AND (CAST(INTERACTION_START_TIME AS VARCHAR) != '1858-11-01')
    --HAY 2 CUENTAS QUE INICIAN EN ESE AÑO
    AND(INTERACTION_START_TIME IS NOT NULL)
    AND INTERACTION_ID NOT LIKE '%-%'
    
),

interactions_ticket as (
--SELECT ACCOUNT_ID, interaction_start_time, interaction_end_time, DATE_DIFF('day',cast(substr(cast(interaction_start_time as varchar),1,10) as date), cast(substr(cast(interaction_end_time as varchar),1,10) as date)) AS Duration, DATE_TRUNC ('Month',cast(substr(cast(interaction_start_time as varchar),1,10) as date)) AS INTERACTION_MONTH

SELECT ACCOUNT_ID, interaction_start_time, interaction_end_time, 
DATE_DIFF('day',cast(substr(cast(interaction_start_time as varchar),1,10) as date), cast(substr(cast(interaction_end_time as varchar),1,10) as date)) AS Duration, 
DATE_TRUNC ('Month',cast(substr(cast(interaction_start_time as varchar),1,10) as date)) AS INTERACTION_MONTH

FROM clean_interaction_time
WHERE interaction_purpose_descrip = 'TICKET'
--AND interaction_status ='CLOSED'

),

Join_interaction as (
SELECT a.*, b.*
FROM interactions_ticket AS a
LEFT JOIN DNA AS b
ON a.ACCOUNT_ID = b.act_acct_cd
)
Select
DISTINCT INTERACTION_MONTH,
COUNT (ACCOUNT_ID),
Technology
from Join_interaction
WHERE 
--INTERACTION_MONTH = DATE ('1858-11-01')
--AND 
duration >= 4 AND 
act_acct_cd IS NOT NULL
GROUP BY INTERACTION_MONTH,
Technology
ORDER BY INTERACTION_MONTH ASC
--LIMIT 10
