with
-------------------------------------Previoulsy Calculated KPIs-------------------------------------------------
FMC_Table AS ( 
SELECT *,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit
,case when fixedmainmovement='4.New Customer' then fixedaccount else null end as Gross_Adds
,case when fixedaccount is not null then fixedaccount else null end as Active_Base
,case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech
from( select *,concat(coalesce(b_final_techflag,''),coalesce(e_final_techflag,'')) as tech_concat
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where month=date(dt))
)
,Sprint3_KPIs as (
select distinct month,case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech,sum(activebase) as activebase,sum(unique_sales) as unique_sales,sum(unique_softdx) as unique_softdx,sum(unique_neverpaid) as unique_neverpaid,sum(unique_longinstall) as unique_longinstall,sum(unique_earlyinteraction) as unique_earlyinteraction,sum(unique_earlyticket) as unique_earlyticket,sum(unique_billclaim) as unique_billclaim,sum(unique_mrcchange) as unique_mrcchange,sum(unique_mountingbill) as unique_mountingbill,sum(noplan) as noplan
from( select *,concat(coalesce(b_final_techflag,''),coalesce(e_final_techflag,'')) as tech_concat
from "lla_cco_int_ana_prod"."cwp_operational_drivers_prod" where month=date(dt)) group by 1,2
)
,S3_CX_KPIs as(
select distinct month,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit
,sum(activebase) as activebase,sum(unique_mountingbill) as mountingbills,sum(unique_mrcchange) as  mrc_change,sum(noplan) as noplan_customers,sum(unique_sales) as unique_sales,sum(unique_softdx) as unique_softdx,sum(unique_longinstall) as unique_longinstall,sum(unique_earlyticket) as unique_earlyticket,sum(unique_earlyinteraction) as unique_earlyinteraction
,round(sum(cast(unique_mrcchange as double))/sum(cast(noplan as double)),4) as Customers_w_MRC_Changes
,round(sum(cast(unique_mountingbill as double))/sum(cast(activebase as double)),4) as Customers_w_Mounting_Bills
,round(sum(cast(unique_softdx as double))/sum(cast(unique_sales as double)),4) as New_Sales_to_Soft_Dx
,round(sum(cast(unique_longinstall as double))/sum(cast(unique_sales as double)),4) as breech_cases_install
,round(sum(cast(unique_earlyticket as double))/sum(cast(unique_sales as double)),4) as Early_Tech_Tix
, round(sum(cast(unique_earlyinteraction as double))/sum(cast(unique_sales as double)),4) as New_Customer_Callers
from Sprint3_KPIs where tech is not null group by 1 order by 1
)
,S3_CX_KPIs_Network as(
select distinct month,Tech,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit
,activebase,unique_mountingbill as mountingbills,unique_mrcchange as  mrc_change,noplan as noplan_customers,unique_sales,unique_softdx,unique_longinstall,unique_earlyticket,unique_earlyinteraction
,round(cast(unique_mrcchange as double)/cast(noplan as double),4) as Customers_w_MRC_Changes
,round(cast(unique_mountingbill as double)/cast(activebase as double),4) as Customers_w_Mounting_Bills
,round(cast(unique_softdx as double)/cast(unique_sales as double),4) as New_Sales_to_Soft_Dx
,round(cast(unique_longinstall as double)/cast(unique_sales as double),4) as breech_cases_install
,round(cast(unique_earlyticket as double)/cast(unique_sales as double),4) as Early_Tech_Tix
, round(cast(unique_earlyinteraction as double)/cast(unique_sales as double),4) as New_Customer_Callers
from Sprint3_KPIs where tech is not null order by 1
)
,Sprint5_KPIs as(
select distinct Month,case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech,case when InteractionsTier in('2','>3') then sum(usersinteractions) end as RepeatedCallers,sum(fixed_accounts) fixed_accounts,sum(outlierrepairs) as outlier_repairs,sum(numbertickets) as numbertickets
from (select *,concat(coalesce(b_final_techflag,''),coalesce(e_final_techflag,'')) as tech_concat
from "lla_cco_int_stg"."cwp_operationaldrivers2_temp") group by 1,2,interactionstier order by 1
)
,S5_CX_KPIs as(
select distinct month,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit
,sum(fixed_accounts) as fixed_acc,sum(repeatedcallers) as repeatcall,sum(outlier_repairs) as outlier_rep,sum(numbertickets) as tickets
,round(cast(sum(repeatedcallers) as double)/cast(sum(fixed_accounts) as double),4) as Repeat_Callers
,round(cast(sum(outlier_repairs) as double)/cast(sum(fixed_accounts) as double),4) as Breech_Cases_Repair
,round(cast(sum(numbertickets) as double)/cast(sum(fixed_accounts) as double),4)*100 as Tech_Tix_per_100_Acct
from Sprint5_KPIs where tech is not null group by 1,2,3,4,5,6 order by 1,2,3,4,5,6
)
,S5_CX_KPIs_Network as(
select distinct month,Tech,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit
,sum(fixed_accounts) as fixed_acc,sum(repeatedcallers) as repeatcall,sum(outlier_repairs) as outlier_rep,sum(numbertickets) as tickets
,round(cast(sum(repeatedcallers) as double)/cast(sum(fixed_accounts) as double),4) as Repeat_Callers
,round(cast(sum(outlier_repairs) as double)/cast(sum(fixed_accounts) as double),4) as Breech_Cases_Repair
,round(cast(sum(numbertickets) as double)/cast(sum(fixed_accounts) as double),4)*100 as Tech_Tix_per_100_Acct
from Sprint5_KPIs where tech is not null group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6,7
)
,home_integrity_node_base as (
select month,mac_address, max(replace(mac_address,':','')) as MAC_JOIN,max(first_node_name) as first_node_name, min(first_fecha_carga) as first_fecha_carga
from (select dATE_TRUNC('month',date(fecha_carga)) as month,mac_address, first_value(node_name) over(partition by mac_address,dATE_TRUNC('month',date(fecha_carga)) order by fecha_carga) as first_node_name,first_value(fecha_carga) over(partition by mac_address,dATE_TRUNC('month',date(fecha_carga)) order by fecha_carga) as first_fecha_carga
      from "db-stage-dev"."home_integrity_history" where node_name is not null) group by month,mac_address
)
,map_mac_account as (
select month_map_mac,act_acct_cd, max(first_nr_bb_mac) as nr_bb_mac, max(first_fi_outst_age) as first_fi_outst_age
    from ( select date_trunc('month',date(dt)) as month_map_mac,act_acct_cd, first_value(nr_bb_mac) over(partition by act_acct_cd,dATE_TRUNC('month',date(dt)) order by dt) as first_nr_bb_mac,first_value(fi_outst_age) over(partition by act_acct_cd,dATE_TRUNC('month',date(dt)) order by dt) as first_fi_outst_age
from "db-analytics-prod"."fixed_cwp" where date(dt) =date_trunc('month',date(dt)) and nr_bb_mac is not null )
where (cast(first_fi_outst_age as int) < (90) or first_fi_outst_age is null) group by 1,act_acct_cd
)
,join_account_id as (
select a.*,b.* from map_mac_account a
left join home_integrity_node_base  b on b.MAC_JOIN = a.nr_bb_mac and a.month_map_mac=b.month where  b.MAC_JOIN is not null 
)
,interactions_panel as (
select DATE_TRUNC('month',date(INTERACTION_START_TIME)) as inter_month,ACCOUNT_ID as interactions_account_id, count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'CLAIM' then date(INTERACTION_START_TIME) end) as num_total_claims,count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) as num_tech_tickets,count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TRUCKROLL' then date(INTERACTION_START_TIME) end) as num_tech_truckrolls,case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'CLAIM' then date(INTERACTION_START_TIME) end) > 0 then 1 else 0 end as claims_flag,case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) > 0 then 1 else 0 end as tickets_flag,case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TRUCKROLL' then date(INTERACTION_START_TIME) end)  > 0 then 1 else 0 end as truckroll_flag,array_agg(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) as list_dates_tickets,array_agg(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then interaction_id end) as list_interaction_id_tickets
    from "db-stage-prod"."interactions_cwp" where ACCOUNT_ID in (select act_acct_cd from join_account_id) and interaction_id is not null and INTERACTION_ID NOT LIKE '%-%' group by 1,ACCOUNT_ID
)
,join_interactions as (
select a.*,b.* from join_account_id a 
left join interactions_panel b on a.act_acct_cd = b.interactions_account_id and a.month=b.inter_month
)
,group_node as (
select month,month_map_mac,hfc_node,accounts_with_tickets,total_accounts,accounts_with_tickets*100/total_accounts as percentage_accounts_with_tickets
from (select month,month_map_mac,first_node_name as hfc_node, cast(sum(tickets_flag) as double) as accounts_with_tickets,cast(count(distinct act_acct_cd) as double) as total_accounts from join_interactions group by 1,2,first_node_name)
)
,nodes_by_severity as (
select month,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,month_map_mac,total_nodes,nodes_higher_than_6_perc,nodes_higher_than_6_perc*100/total_nodes as kpi_percentage
from( select month,month_map_mac,cast(count(distinct hfc_node) as double) as total_nodes,cast(count(distinct case when percentage_accounts_with_tickets > 6 then hfc_node end) as double) as nodes_higher_than_6_perc
    from group_node group by month,2)
)
------------------------------------New KPIs--------------------------------------------------------------
,payments as(
select distinct month,opco,market,marketsize,product,biz_unit,'digital_shift' as facet,'pay' as journey_waypoint,'Digital_Payments' as kpi_name,round(cast(sum(digital) as double)/cast(sum (pymt_cnt) as double),2) as kpi_meas,sum(digital) as kpi_num,sum(pymt_cnt) as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network
from( select date_trunc('month',date(dt)) as month,'CWP' as opco,'Panama' as market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,count(distinct(payment_doc_id)) as pymt_cnt
,case when digital_nondigital = 'Digital' then count(distinct(payment_doc_id)) end as digital
FROM "db-stage-prod"."payments_cwp" where account_type = 'B2C' group by 1,2,3,4,5,6,digital_nondigital)
group by 1,2,3,4,5,6,7,8,9
)
,service_delivery as(
select distinct Month,network,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,sum(Installations) as Install,round(sum(Inst_MTTI)/sum(Installations),2) as MTTI,sum(Repairs) as Repairs,round(sum(Rep_MTTR)/sum(Repairs),2) as MTTR,round(sum(scr),2) as Repairs_1k_rgu,round((sum(FTR_Install_M)/sum(Installations))/100,4) as FTR_Install,round((sum(FTR_Repair_M)/sum(Repairs))/100,4) as FTR_Repair
from( select distinct date_trunc('month',date(date_parse(cast(end_week_adj as varchar),'%Y%m%d'))) as Month,Network,date(date_parse(cast(end_week_adj as varchar),'%Y%m%d')) as End_Week,sum(Total_Subscribers) as Total_Users,sum(Assisted_Installations) as Installations,sum(mtti) as MTTI,sum(Assisted_Installations)*sum(mtti) as Inst_MTTI,sum(truck_rolls) as Repairs,sum(mttr) as MTTR,sum(truck_rolls)*sum(mttr) as Rep_MTTR,sum(scr) as SCR,(100-sum(i_elf_28days)) as FTR_Install,(100-sum(r_elf_28days)) as FTR_Repair,(100-sum(i_elf_28days))*sum(Assisted_Installations) as FTR_Install_M,(100-sum(r_elf_28days))*sum(truck_rolls) as FTR_Repair_M
from "lla_cco_int_san"."cwp_ext_servicedelivery_result" where market='Panama' --and network='OVERALL'
group by 1,2,3 order by 1,2,3) group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6,7)
,nps_kpis as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,Network from "lla_cco_int_san"."cwp_ext_nps_kpis" where opco='CWP')
,wanda_kpis as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,opco,market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,network
from "lla_cco_int_san"."cwp_ext_nps_wanda"  where opco='CWP')
,digital_sales as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,opco,market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,kpi_sla,network
from "lla_cco_int_san"."cwp_ext_digitalsales" where opco='CWP')
---------------------------------All Flags KPIs------------------------------------------------------------
-------Prev Calculated
,GrossAdds_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,count(distinct Gross_Adds) as kpi_meas,0 as kpi_num,0 as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table where tech is not null Group by 1,2,3,4,5,6,7,8,9 )
,GrossAdds_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,count(distinct Gross_Adds) as kpi_meas,0 as kpi_num,0 as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from fmc_table where tech is not null Group by 1,2,3,4,5,6,7,8,9,14 )
,ActiveBase_Flag1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table where tech is not null Group by 1,2,3,4,5,6,7,8,9 )
,ActiveBase_Flag2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network  from fmc_table where tech is not null Group by 1,2,3,4,5,6,7,8,9 )
,ActiveBase_Network1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from fmc_table where tech is not null Group by 1,2,3,4,5,6,7,8,9,14)
,ActiveBase_Network2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network  from fmc_table where tech is not null Group by 1,2,3,4,5,6,7,8,9,14)
,TechTickets_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,round(Tech_Tix_per_100_Acct,3) as kpi_meas,tickets as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network  from S5_CX_KPIs)
,TechTickets_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,round(Tech_Tix_per_100_Acct,3) as kpi_meas,tickets as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network  from S5_CX_KPIs_Network)
,MRCChanges_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,round(Customers_w_MRC_Changes,3) as kpi_meas,mrc_change as kpi_num,noplan_customers as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs )
,MRCChanges_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,round(Customers_w_MRC_Changes,3) as kpi_meas,mrc_change as kpi_num,noplan_customers as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network )
,SalesSoftDx_Flag as(
select distinct date_add('month',2,month) as month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,round(New_Sales_to_Soft_Dx,3) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den, 'M-2' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs )
,SalesSoftDx_Network as(
select distinct date_add('month',2,month) as month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,round(New_Sales_to_Soft_Dx,3) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den, 'M-2' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
,EarlyIssues_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Customer_Callers_2+calls_21Days' as kpi_name,round(New_Customer_Callers,3) as kpi_meas,unique_earlyinteraction as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs )
,EarlyIssues_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Customer_Callers_2+calls_21Days' as kpi_name,round(New_Customer_Callers,3) as kpi_meas,unique_earlyinteraction as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
,LongInstall_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,round(breech_cases_install,3) as kpi_meas,unique_longinstall as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs )
,LongInstall_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,round(breech_cases_install,3) as kpi_meas,unique_longinstall as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
,EarlyTickets_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,round(early_tech_tix,3) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs)
,EarlyTickets_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,round(early_tech_tix,3) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
,RepeatedCall_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,round(Repeat_Callers,3) as kpi_meas,repeatcall as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S5_CX_KPIs )
,RepeatedCall_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,round(Repeat_Callers,3) as kpi_meas,repeatcall as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S5_CX_KPIs_Network)
,OutlierRepair_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'Breech_Cases_Repair_4+Days' as kpi_name,round(Breech_Cases_Repair,3) as kpi_meas,outlier_rep as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S5_CX_KPIs )
,OutlierRepair_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'Breech_Cases_Repair_4+Days' as kpi_name,round(Breech_Cases_Repair,3) as kpi_meas,outlier_rep as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S5_CX_KPIs_Network)
,MountingBill_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,round(Customers_w_Mounting_Bills,3) as kpi_meas,mountingbills as kpi_num,activebase as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs)
,MountingBill_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,round(Customers_w_Mounting_Bills,3) as kpi_meas,mountingbills as kpi_num,activebase as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
--Service Delivery KPIs
,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'FTR_Installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_displa,Network from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_RGU' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
---NotCalculated kpis
--BUY
,ecommerce as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'e-Commerce' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network,kpi_sla from digital_sales)
,tBuy as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,round(kpi_meas,2) as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tBuy')
,mttb as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,Buyingcalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calls/GA' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
--GET
,tinstall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,round(kpi_meas,2) as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tInstall')
,selfinstalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'Self_Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,installscalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
--PAY
,MTTBTR as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,tpay as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tPay' as kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,	null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tpay')
,ftr_billing as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'FTR_Billing' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,billbill as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
--Support-call
,helpcare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_Care')
--support-Tech
,helprepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tHelp_Repair' as kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_repair')
--use
,highrisk as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'High_Tech_Call_Nodes_+6%Monthly' as kpi_name, round(kpi_percentage/100,4) as kpi_meas, nodes_higher_than_6_perc as kpi_num,total_nodes as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from nodes_by_severity)
,pnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='pNPS')
,rnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas, null as kpi_num,null as kpi_den,Kpi_delay_display,Network from nps_kpis where kpi_name='rNPS')
--Wanda Dashboard
,cccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Care')
,cctech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Tech')
,chatbot as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Care')
,carecall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Care_Calls_Intensity')
,techcall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Tech_Calls_Intensity')
,chahtbottech as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Tech')
,frccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='FRC_Care')
---------------------------------Join Flags-----------------------------------------------------------------
,Join_DNA_KPIs as(
select distinct *
from( select * from GrossAdds_Flag union all select * from ActiveBase_Flag1 union all select * from ActiveBase_Flag2)
)
,Join_Sprints_KPIs as(
select distinct *
from( select * from join_dna_kpis union all select * from TechTickets_Flag union all select * from MRCChanges_Flag union all select * from SalesSoftDx_Flag union all select * from EarlyIssues_Flag union all select * from LongInstall_Flag union all select * from EarlyTickets_Flag union all select * from RepeatedCall_Flag union all select * from OutlierRepair_Flag union all select * from MountingBill_Flag)
)
,Join_New_KPIs as(
select distinct *
from( select * from join_sprints_kpis union all select * from payments))
,final_full_kpis as (select * from Join_New_KPIs union all select * from mttb union all select * from Buyingcalls union all select * from tbuy union all select * from MTTI union all select * from tinstall union all select * from ftr_installs union all select * from installs union all select * from selfinstalls union all select * from installscalls union all select * from MTTBTR union all select * from tpay union all select * from ftr_billing union all select * from helpcare union all select * from frccare 
 union all select * from mttr  union all select * from helprepair union all select * from ftrrepair union all select * from repairs1k union all select * from highrisk union all select * from justrepairs union all select * from pnps union all select * from rnps)
--Join Wanda Dashboard
,Join_Wanda as(
select distinct * from(select * from final_full_kpis union all select * from billbill union all select * from cccare union all select * from cctech union all select * from chatbot union all select * from carecall union all select * from techcall union all select * from chahtbottech)
)
,Join_Technology as(
select *,null as kpi_sla from(select * from Join_Wanda union all select * from GrossAdds_Network union all select * from ActiveBase_Network1 union all select * from ActiveBase_Network2 union all select * from TechTickets_Network union all select * from MRCChanges_Network union all select * from SalesSoftDx_Network union all select * from EarlyIssues_Network union all select * from LongInstall_Network union all select * from EarlyTickets_Network union all select * from RepeatedCall_Network union all select * from OutlierRepair_Network union all select * from MountingBill_Network)
union all select * from ecommerce 
)
,Gross_adds_disc as(
select distinct month,network,'Gross_Adds' as kpi_name,kpi_meas as div from join_technology where facet='contact_drivers' and kpi_name='Active_Base')
,disclaimer_fields as(
select *,concat(cast(round(kpi_disclaimer_meas*100,2) as varchar),'% of base') as kpi_disclaimer_display
from(select j.*,case when j.kpi_name='Gross_Adds' then round(j.kpi_meas/g.div,4) else null end as kpi_disclaimer_meas
from join_technology j left join Gross_adds_disc g on j.month=g.month and j.network=g.network and j.kpi_name=g.kpi_name)
)

select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den, KPI_Sla, kpi_delay_display,kpi_disclaimer_display,kpi_disclaimer_meas,Network,year(Month) as ref_year,month(month) as ref_mo,null as kpi_sla_below_threshold,null as kpi_sla_middling_threshold,null as kpi_sla_above_threshold,null as kpi_sla_far_below_threshold,null as kpi_sla_far_above_threshold
from disclaimer_fields
--where --month=date('2022-05-01')
--facet='high_risk' and network='OVERALL'
order by 1,kpi_name
