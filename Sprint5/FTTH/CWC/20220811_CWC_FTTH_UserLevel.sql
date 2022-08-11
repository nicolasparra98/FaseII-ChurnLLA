with
fmc_table as(
SELECT * FROM  "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where Month = date(dt)
)
,MAXDATEMONTH AS(
SELECT DISTINCT DATE_TRUNC('Month', date(dt)) as Month, date(first_value(dt) over (partition by DATE_TRUNC('Month', date(dt)) order by dt desc )) as MaxDate
FROM "lla_cco_int_ext_prod"."cwc_networkdata_ods" 
)
, FTTH_BASE AS(
SELECT Month, MaxDate, n.*
FROM "lla_cco_int_ext_prod"."cwc_networkdata_ods" n inner join MAXDATEMONTH m on date(n.dt) = m.MaxDate and date_trunc('Month' , date(n.dt)) = m.Month
where homes_passed <> 'nan' and homes_passed <> '2019/02/28'
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
)
,ftth_final_join as (
select f.month, region,area,dp_unique_key,act_acct_cd
from dna_nodes n inner join FTTH_base f on dp_unique_key = nr_short_node and n.month = f.month
)
,ftth_flags as(
select f.*,region,area,dp_unique_key as fdh
,case when act_acct_cd is not null then 'FTTH User' 
      when act_acct_cd is null and (bb_rgu_eom is not null or bb_rgu_bom is not null) then 'Other BB User'
      when act_acct_cd is null and bb_rgu_eom is null and bb_rgu_bom is null then 'Other User'
else null end as FTTH_User
,case when act_acct_cd is not null and concat(coalesce(b_final_tech_flag,''),coalesce(e_final_tech_flag,'')) NOT LIKE '%FTTH%' then 1 else 0 end as Atypical_FTTH
from fmc_table f left join ftth_final_join j on f.fixed_account=j.act_acct_cd and f.month=j.month
)
select distinct month,region,area,fixed_account,fdh,final_bom_activeflag,final_eom_activeflag,activebom,activeeom,mainmovement,spinmovement,fixedchurntypeflag,finalchurnflag,churntypefinalflag,b_finaltenuresegment,e_finaltenuresegment,b_fmctype,e_fmctype,b_fmc_segment,e_fmc_segment,b_totalmrc,e_totalmrc,partial_total_churnflag,waterfall_flag,b_final_tech_flag,e_final_tech_flag,downsell_split,downspin_split,Atypical_FTTH
from ftth_flags
where fdh is not null
