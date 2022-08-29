with FMC_Table AS
( SELECT * FROM  "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where Month = date(dt)
  and concat(coalesce(b_final_tech_flag,''),coalesce(e_final_tech_flag,'')) LIKE '%FTTH%'
)
,USEFULFIELDS_FTTH AS( 
 select *
 from(SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month,date(dt) as dt,act_acct_cd, nr_short_node, nr_long_node, nr_bb_mac, nr_cable, nr_fdh, nr_fdp, nr_minibox, nr_odfx, nr_ont,act_rgn_cd,act_area_cd,act_prvnc_cd
     ,lpad(nr_short_node,3,'0') as area_adj
     ,CASE WHEN length(cast(act_acct_cd as varchar))=8 then 'HFC' 
            WHEN NR_FDP<>'' and NR_FDP<>' ' and NR_FDP is not null THEN 'FTTH' 
            WHEN pd_vo_tech='FIBER' THEN 'FTTH'
            WHEN pd_bb_tech='FIBER' THEN 'FTTH'
            ELSE 'COPPER' END AS Tech
 --,case when nr_short_node is not null then lpad(nr_short_node,3,'0') else lpad(nr_long_node,3,'0') end as area_adj
 ,first_value(date(dt)) over(partition by act_acct_cd order by date(dt)) as start_date,pd_vo_tech,pd_bb_tech
  FROM "db-analytics-prod"."tbl_fixed_cwc" 
  WHERE org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') --and nr_short_node is not null --and date(dt)=date('2022-07-31')
  )
  where tech='FTTH' --and area_adj='BOS' 
  --and month=date('2022-07-01')
  --and Act_acct_Cd in('301204080000','298381420000','316376320000')
--order by 3,2
)
/*
select distinct month,dt,fixed_account,NR_FDP,pd_vo_tech,pd_bb_prod_nm,b_final_tech_flag,e_final_tech_flag,area,area_adj,nr_short_node,start_date as First_DNA_Record
from(select distinct f.*,act_area_cd as area,nr_short_node, nr_long_node,area_adj,start_date,NR_FDP,pd_vo_tech,pd_bb_prod_nm
from FMC_Table f left join USEFULFIELDS_FTTH u on f.fixed_account=u.act_acct_cd and f.month=u.month)
where month=date('2022-07-01') 
--and area_adj is null --in('ALB','CDS','JRV','LLF') --and date(dt)=date('2022-07-31')
and fixed_account in('319032820000','321478260000','314487080000')
--and concat(coalesce(b_final_tech_flag,''),coalesce(e_final_tech_flag,'')) LIKE '%FTTH%'
*/
,Najeeb_file as(
SELECT distinct --site,exch,
"area code" as area_code
--,"unique key" as unique_key,parish
,sum(serviceable_hhp) as home_passed
--,working
FROM "lla_cco_int_san"."cwc_ext_ftth_add"
group by 1 order by 1
)
,fmc_nodes_join as (
select f.*,
nr_short_node,nr_long_node,nr_bb_mac,nr_cable,nr_fdh,nr_fdp,nr_minibox,nr_odfx,nr_ont,act_rgn_cd,act_area_cd,area_adj
from fmc_table f left join USEFULFIELDS_FTTH n  on fixed_account = act_acct_cd and f.month = n.month
)


,MAXDATEMONTH AS(
SELECT DISTINCT DATE_TRUNC('Month', date(dt)) as Month, date(first_value(dt) over (partition by DATE_TRUNC('Month', date(dt)) order by dt desc )) as MaxDate
FROM "lla_cco_int_ext_prod"."cwc_networkdata_ods" where dp_category = 'GPON'
)
, FTTH_BASE AS(
SELECT m.Month, MaxDate, n.region,n.area,n.dp_unique_key,n.plant_status,sum(cast(n.homes_passed as double)) as home_passed
FROM "lla_cco_int_ext_prod"."cwc_networkdata_ods" n inner join MAXDATEMONTH m on date(n.dt) = m.MaxDate and date_trunc('Month' , date(n.dt)) = m.Month
where homes_passed <> 'nan' and homes_passed <> '2019/02/28' AND dp_category = 'GPON'
and month=date('2022-07-01')
group by 1,2,3,4,5,6
)
--select distinct * from ftth_base
--where area='DPX' and month=date('2022-07-01') and dp_unique_key='DPXFDP1006'

,ftth_approach1_prev as (
select f.month, region,area,dp_unique_key
--,sum(cast(homes_passed as double)) as homespassed
,home_passed as homespassed
,count(distinct fixed_account) as activeusers--, round(count(distinct fixed_account)/sum(cast(homes_passed as double)),2) as penetration
from fmc_nodes_join n inner join FTTH_base f on dp_unique_key = nr_short_node and n.month = f.month
--where dp_unique_key is not null and f.month=date('2022-07-01')
group by 1,2,3,4,5 order by 1,2,3,4,5
)
--select * from ftth_approach1 where area='ACX'
,ftth_approach1 as (
select month, region,area
--,sum(cast(homes_passed as double)) as homespassed
,sum(homespassed) as homespassed
,sum(activeusers) as activeusers --, round(count(distinct fixed_account)/sum(cast(homes_passed as double)),2) as penetration
from ftth_approach1_prev
--where dp_unique_key is not null and f.month=date('2022-07-01')
group by 1,2,3 order by 1,2,3
)
,areas_null as(
select distinct f.month,area_adj,area,homespassed as homespassed_a1,area_code,home_passed as homespassed_a2
,fixed_account
--, count(distinct fixed_account) as users
from fmc_nodes_join f left join Najeeb_file n on f.area_adj=n.area_code
left join ftth_approach1 a on --dp_unique_key = nr_short_node and 
a.month = f.month and f.area_adj=area
where f.month=date('2022-07-01') --and area_adj='ABR'
--and fixed_account in('319032820000','321478260000','314487080000')
--and fixed_account in('201026660000','261307650000','312163100000')
--and fixed_account in('969091220000','293245790000','306290950000','281003100000','314715170000','294028540000')
and area_adj is null
--group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
)
 select *
 from(SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month,date(dt) as dt,act_acct_cd, nr_short_node, nr_long_node, nr_bb_mac, nr_cable, nr_fdh, nr_fdp, nr_minibox, nr_odfx, nr_ont,act_rgn_cd,act_area_cd,act_prvnc_cd
     ,lpad(nr_short_node,3,'0') as area_adj
     ,CASE WHEN length(cast(act_acct_cd as varchar))=8 then 'HFC' 
            WHEN NR_FDP<>'' and NR_FDP<>' ' and NR_FDP is not null THEN 'FTTH' 
            WHEN pd_vo_tech='FIBER' THEN 'FTTH'
            WHEN pd_bb_tech='FIBER' THEN 'FTTH'
            ELSE 'COPPER' END AS Tech
 --,case when nr_short_node is not null then lpad(nr_short_node,3,'0') else lpad(nr_long_node,3,'0') end as area_adj
 ,first_value(date(dt)) over(partition by act_acct_cd order by date(dt)) as start_date,pd_vo_tech,pd_bb_tech
  FROM "db-analytics-prod"."tbl_fixed_cwc" 
  WHERE org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') --and nr_short_node is not null --and date(dt)=date('2022-07-31')
  )
  where tech='FTTH' --and area_adj='BOS' 
  and month=date('2022-07-01')
  and Act_acct_Cd in(select fixed_account from areas_null)
  --'301204080000','298381420000','316376320000'
--)
order by 3,2
