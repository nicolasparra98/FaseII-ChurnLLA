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
,round(cast(sum(numbertickets) as double)/cast(sum(fixed_accounts) as double),4) as Tech_Tix_per_100_Acct
from Sprint5_KPIs where tech is not null group by 1,2,3,4,5,6 order by 1,2,3,4,5,6
)
,S5_CX_KPIs_Network as(
select distinct month,Tech,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit
,sum(fixed_accounts) as fixed_acc,sum(repeatedcallers) as repeatcall,sum(outlier_repairs) as outlier_rep,sum(numbertickets) as tickets
,round(cast(sum(repeatedcallers) as double)/cast(sum(fixed_accounts) as double),4) as Repeat_Callers
,round(cast(sum(outlier_repairs) as double)/cast(sum(fixed_accounts) as double),4) as Breech_Cases_Repair
,round(cast(sum(numbertickets) as double)/cast(sum(fixed_accounts) as double),4) as Tech_Tix_per_100_Acct
from Sprint5_KPIs where tech is not null group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6,7
)
------------------------------------New KPIs--------------------------------------------------------------
,payments as(
select distinct month,opco,market,marketsize,product,biz_unit,'digital_shift' as facet,'pay' as journey_waypoint,'%digital_payments' as kpi_name,round(cast(sum(digital) as double)/cast(sum (pymt_cnt) as double),2) as kpi_meas,sum(digital) as kpi_num,sum(pymt_cnt) as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network
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
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,round(Tech_Tix_per_100_Acct,2) as kpi_meas,tickets as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network  from S5_CX_KPIs)
,TechTickets_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,round(Tech_Tix_per_100_Acct,2) as kpi_meas,tickets as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network  from S5_CX_KPIs_Network)
,MRCChanges_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'%Customers_w_MRC_Changes_5%+_excl_plan' as kpi_name,round(Customers_w_MRC_Changes,2) as kpi_meas,mrc_change as kpi_num,noplan_customers as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs )
,MRCChanges_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'%Customers_w_MRC_Changes_5%+_excl_plan' as kpi_name,round(Customers_w_MRC_Changes,2) as kpi_meas,mrc_change as kpi_num,noplan_customers as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network )
,SalesSoftDx_Flag as(
select distinct date_add('month',2,month) as month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Sales_to_Soft_Dx' as kpi_name,round(New_Sales_to_Soft_Dx,2) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den, 'M-2' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs )
,SalesSoftDx_Network as(
select distinct date_add('month',2,month) as month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Sales_to_Soft_Dx' as kpi_name,round(New_Sales_to_Soft_Dx,2) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den, 'M-2' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
,EarlyIssues_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Customer_Callers_2+calls_21days' as kpi_name,round(New_Customer_Callers,2) as kpi_meas,unique_earlyinteraction as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs )
,EarlyIssues_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Customer_Callers_2+calls_21days' as kpi_name,round(New_Customer_Callers,2) as kpi_meas,unique_earlyinteraction as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
,LongInstall_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'%breech_cases_install_6+days' as kpi_name,round(breech_cases_install,2) as kpi_meas,unique_longinstall as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs )
,LongInstall_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'%breech_cases_install_6+days' as kpi_name,round(breech_cases_install,2) as kpi_meas,unique_longinstall as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
,EarlyTickets_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'%Early_Tech_Tix_-7weeks' as kpi_name,round(early_tech_tix,2) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs)
,EarlyTickets_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'%Early_Tech_Tix_-7weeks' as kpi_name,round(early_tech_tix,2) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
,RepeatedCall_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'%Repeat_Callers_2+calls' as kpi_name,round(Repeat_Callers,2) as kpi_meas,repeatcall as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S5_CX_KPIs )
,RepeatedCall_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'%Repeat_Callers_2+calls' as kpi_name,round(Repeat_Callers,2) as kpi_meas,repeatcall as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S5_CX_KPIs_Network)
,OutlierRepair_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'%breech_bases_repair_4+_days' as kpi_name,round(Breech_Cases_Repair,2) as kpi_meas,outlier_rep as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S5_CX_KPIs )
,OutlierRepair_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'%breech_bases_repair_4+_days' as kpi_name,round(Breech_Cases_Repair,2) as kpi_meas,outlier_rep as kpi_num,fixed_acc as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S5_CX_KPIs_Network)
,MountingBill_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'%Customers_w_Mounting_Bills' as kpi_name,round(Customers_w_Mounting_Bills,2) as kpi_meas,mountingbills as kpi_num,activebase as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs)
,MountingBill_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'%Customers_w_Mounting_Bills' as kpi_name,round(Customers_w_Mounting_Bills,2) as kpi_meas,mountingbills as kpi_num,activebase as kpi_den, 'M-0' as Kpi_delay_display,Tech as Network from S3_CX_KPIs_Network)
--Service Delivery KPIs
,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'%FTR_installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_displa,Network from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'%FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_rgu' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
---NotCalculated kpis
--BUY
,ecommerce as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'%eCommerce' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,tBuy as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,round(kpi_meas,2) as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tBuy')
,mttb as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,Buyingcalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calss/GA' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
--GET
,tinstall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,round(kpi_meas,2) as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tInstall')
,selfinstalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'%self_installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,installscalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
--PAY
,MTTBTR as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,tpay as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tPay' as kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,	null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tpay')
,ftr_billing as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'%FTR_Billing' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
--Support-call
,helpcare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_Care')
,frccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'%FRC_Care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
--support-Tech
,helprepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tHelp_Repair' as kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_repair')
--use
,highrisk as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'%_High_Tech_Call_Nodes_+6%monthly' as kpi_name, null as kpi_meas, null as kpi_num,null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,pnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='pNPS')
--Wanda Dashboard
,billbill as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,cccare as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,'%CC_SL_Care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,cctech as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,'%CC_SL_Tech' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,chatbot as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,'%Chatbot_containment_care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,carecall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,'care_calls_per_1k_rgu' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,techcall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,'tech_calls_per_1k_rgu' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,chahtbottech as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,'%Chatbot_containment_Tech' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from fmc_table)
,rnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas, null as kpi_num,null as kpi_den,Kpi_delay_display,Network from nps_kpis where kpi_name='rNPS')
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
from( select * from join_sprints_kpis union all select * from payments)
)
,final_full_kpis as (select * from Join_New_KPIs union all select * from mttb union all select * from Buyingcalls union all select * from tbuy union all select * from ecommerce 
union all select * from MTTI union all select * from tinstall union all select * from ftr_installs union all select * from installs union all select * from selfinstalls union all select * from installscalls union all select * from MTTBTR union all select * from tpay union all select * from ftr_billing union all select * from helpcare union all select * from frccare 
 union all select * from mttr  union all select * from helprepair union all select * from ftrrepair union all select * from repairs1k union all select * from highrisk union all select * from justrepairs union all select * from pnps union all select * from rnps)
--Join Wanda Dashboard
,Join_Wanda as(
select distinct * from(select * from final_full_kpis union all select * from billbill union all select * from cccare union all select * from cctech union all select * from chatbot union all select * from carecall union all select * from techcall union all select * from chahtbottech)
)
,Join_Technology as(
select distinct * from(select * from Join_Wanda union all select * from GrossAdds_Network union all select * from ActiveBase_Network1 union all select * from ActiveBase_Network2 union all select * from TechTickets_Network union all select * from MRCChanges_Network union all select * from SalesSoftDx_Network union all select * from EarlyIssues_Network union all select * from LongInstall_Network union all select * from EarlyTickets_Network union all select * from RepeatedCall_Network union all select * from OutlierRepair_Network union all select * from MountingBill_Network)
)
,Gross_adds_disc as(
select distinct month,network,'Gross_Adds' as kpi_name,kpi_meas as div from join_technology where facet='contact_drivers' and kpi_name='Active_Base')
,disclaimer_fields as(
select *,concat(cast(round(kpi_disclaimer_meas*100,2) as varchar),'% of base') as kpi_disclaimer_display
from(select j.*,case when j.kpi_name='Gross_Adds' then round(j.kpi_meas/g.div,4) else null end as kpi_disclaimer_meas
from join_technology j left join Gross_adds_disc g on j.month=g.month and j.network=g.network and j.kpi_name=g.kpi_name)
)

select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,null as KPI_Sla, kpi_delay_display,kpi_disclaimer_display,kpi_disclaimer_meas,Network,year(Month) as ref_year,month(month) as ref_mo,null as kpi_sla_below_threshold,null as kpi_sla_middling_threshold,null as kpi_sla_above_threshold,null as kpi_sla_far_below_threshold,null as kpi_sla_far_above_threshold
from disclaimer_fields
--where month=date('2022-05-01')
order by kpi_name
