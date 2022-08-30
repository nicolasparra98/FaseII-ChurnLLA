with FMC_Table AS
( SELECT * FROM  "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where Month = date(dt)
  and concat(coalesce(b_final_tech_flag,''),coalesce(e_final_tech_flag,'')) LIKE '%FTTH%'
  and month>=date('2022-01-01')
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
)
where tech='FTTH' and month_dna>date('2022-01-01')
)
,fmc_nodes_join as (
select *
from fmc_table f left join USEFULFIELDS_FTTH n  on fixed_account = act_acct_cd and f.month = n.month_dna
)
,Najeeb_file as(
SELECT distinct
date(date_parse(cast(dt as varchar),'%Y%m%d')) as Month
,"area code" as area_code
,trim("Equipment Name") as eq_name
--,"unique key" as unique_key,parish
,sum("serviceable_hhp") as home_passed
--,working
FROM "lla_cco_int_san"."cwc_ext_ftth_final"
--"lla_cco_int_san"."cwc_ext_ftth_add"
group by 1,2,3 order by 1,2,3
)
select distinct n.month,area_code,home_passed,count(distinct act_acct_cd) as users,round(cast(count(distinct act_acct_cd) as double)/cast(home_passed as double),2) as P
from fmc_nodes_join u right join Najeeb_file n on --u.nr_tel_center=n.eq_name and 
area_adj=area_code and u.month=n.month
group by 1,2,3--,4
order by 1,2,3--,4
