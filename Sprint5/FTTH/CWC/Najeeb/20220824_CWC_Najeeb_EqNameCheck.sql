with FMC_Table AS
( SELECT * FROM  "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where Month = date(dt)
  and concat(coalesce(b_final_tech_flag,''),coalesce(e_final_tech_flag,'')) LIKE '%FTTH%'
)
,USEFULFIELDS_FTTH AS( 
select *
 from(SELECT DATE_TRUNC('MONTH',DATE(dt)) as month_dna,date(dt) as dt,act_acct_cd, trim(nr_tel_center) as nr_tel_center,nr_short_node, nr_long_node, nr_bb_mac, nr_cable, nr_fdh, nr_fdp, nr_minibox, nr_odfx, nr_ont,act_rgn_cd,act_area_cd,act_prvnc_cd
     ,lpad(nr_short_node,3,'0') as area_adj
 --,first_value(date(dt)) over(partition by act_acct_cd order by date(dt)) as start_date
 --,pd_vo_tech,pd_bb_tech
      ,CASE WHEN length(cast(act_acct_cd as varchar))=8 then 'HFC' 
            WHEN NR_FDP<>'' and NR_FDP<>' ' and NR_FDP is not null THEN 'FTTH' 
            WHEN pd_vo_tech='FIBER' THEN 'FTTH'
            WHEN pd_bb_tech='FIBER' THEN 'FTTH'
            ELSE 'COPPER' END AS Tech
  FROM "db-analytics-prod"."tbl_fixed_cwc" 
  WHERE org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') --and nr_short_node is not null --
  and date_trunc('month',date(dt))=date('2022-07-01'))
where tech='FTTH' --and area_adj='ACX'
)
,fmc_nodes_join as (
select *
from fmc_table f left join USEFULFIELDS_FTTH n  on fixed_account = act_acct_cd and f.month = n.month_dna
where month=date('2022-07-01')
)
,Najeeb_file as(
SELECT distinct
"area code" as area_code
,trim("Equipment Name") as eq_name
--,"unique key" as unique_key,parish
,sum(serviceable_hhp) as home_passed
--,working
FROM "lla_cco_int_san"."cwc_ext_ftth_add"
group by 1,2 order by 1,2
)
select distinct month,area_adj,nr_tel_center,area_code,eq_name,home_passed,count(distinct act_acct_cd)
from fmc_nodes_join u left join Najeeb_file n on u.nr_tel_center=n.eq_name and area_adj=area_code
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
