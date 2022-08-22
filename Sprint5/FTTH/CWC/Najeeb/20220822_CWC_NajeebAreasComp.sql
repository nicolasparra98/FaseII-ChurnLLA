with FMC_Table AS
( SELECT * FROM  "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where Month = date(dt)
)
,USEFULFIELDS_FTTH AS( 
 SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month,dt,act_acct_cd, nr_short_node, nr_long_node, nr_bb_mac, nr_cable, nr_fdh, nr_fdp, nr_minibox, nr_odfx, nr_ont,act_rgn_cd,act_area_cd,act_prvnc_cd
  FROM "db-analytics-prod"."tbl_fixed_cwc" 
  WHERE org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
,Najeeb_file as(
SELECT distinct site,exch,"area code" as area_code,"unique key" as unique_key,parish,"serviceable hhp" as home_passed,working
FROM "lla_cco_int_san"."cwc_ext_ftth_add"
)
,fmc_nodes_join as (
select f.*,
nr_short_node,nr_long_node,nr_bb_mac,nr_cable,nr_fdh,nr_fdp,nr_minibox,nr_odfx,nr_ont,act_rgn_cd,act_area_cd
from fmc_table f left join USEFULFIELDS_FTTH n  on final_account = act_acct_cd and f.month = n.month
)
select distinct act_area_cd,area_code,count(distinct fixed_account)
from fmc_nodes_join f left join Najeeb_file n on f.act_area_cd=n.area_code
group by 1,2
order by 1,2
