WITH MAXDATEMONTH AS(
SELECT DISTINCT DATE_TRUNC('Month', date(dt)) as Month, date(first_value(dt) over (partition by DATE_TRUNC('Month', date(dt)) order by dt desc )) as MaxDate
FROM "lla_cco_int_ext_prod"."cwc_networkdata_ods" 
)
, FTTH_BASE AS(

SELECT Month, MaxDate, n.*
FROM "lla_cco_int_ext_prod"."cwc_networkdata_ods" n inner join MAXDATEMONTH m on date(n.dt) = m.MaxDate and date_trunc('Month' , date(n.dt)) = m.Month
where homes_passed <> 'nan' and homes_passed <> '2019/02/28'
)

,FMC_Table AS
( SELECT * FROM  "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where Month = date(dt)
)
,Coordenates as(
select *
from "lla_cco_int_san"."cwc_ext_area_coord"
)
,ftth_adj as(
select f.*,c.region as region_c,area_code,longitude,latitude
from ftth_base f left join Coordenates c on f.AREA=c.area_code and f.region=c.region
)
,USEFULFIELDS_FTTH AS( 
 SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month,
    dt,act_acct_cd, nr_short_node, nr_long_node, nr_bb_mac, nr_cable, nr_fdh, nr_fdp, nr_minibox, nr_odfx, nr_ont,
    CASE WHEN length(cast(act_acct_cd as varchar))=8 then 'HFC' 
            WHEN NR_FDP<>'' and NR_FDP<>' ' and NR_FDP is not null THEN 'FTTH' 
            WHEN pd_vo_tech='FIBER' THEN 'FTTH' 
            WHEN (pd_bb_prod_nm like '%GPON%'  OR pd_bb_prod_nm like '%FTT%') and 
            (pd_bb_prod_nm not like '%ADSL%' and pd_bb_prod_nm not like '%VDSL%') THEN 'FTTH' 
            ELSE 'COPPER' END AS Technology_type
  FROM "db-analytics-prod"."tbl_fixed_cwc" 
  WHERE org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)

,dna_nodes as (
select distinct Month,act_acct_cd, nr_short_node, nr_long_node, nr_bb_mac,nr_cable, nr_fdh, nr_fdp, nr_minibox, nr_odfx, nr_ont
from usefulfields_ftth
--where technology_type = 'FTTH'
),

fmc_nodes_join as (
select f.*,
nr_short_node,nr_long_node,nr_bb_mac,nr_cable,nr_fdh,nr_fdp,nr_minibox,nr_odfx,nr_ont 
from fmc_table f left join dna_nodes n  on final_account = act_acct_cd and f.month = n.month
)

, final_join as (
select f.month, region,area
, dp_unique_key
, longitude,latitude,sum(cast(homes_passed as double)) as homespassed, count(distinct final_account) as activeusers, round(count(distinct final_account)/sum(cast(homes_passed as double)),2) as penetration
from fmc_nodes_join n left join FTTH_adj f on dp_unique_key = nr_short_node and
n.month = f.month
group by 1,2,3,4,5,6
)

SELECT distinct * --month,dp_unique_key,count(*) as cont
--region,area,longitude,latitude,sum(penetration),sum(activeusers)
from final_join
where month is not null and dp_unique_key is not null --and month=date('2022-05-01')
--and penetration>100
--group by 1,2,3,4 order by 5 desc,6 desc
