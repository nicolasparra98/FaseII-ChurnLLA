with -------------------------------------Previoulsy Calculated KPIs-------------------------------------------------
FMC_Table AS (
SELECT *,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla,null as Kpi_delay_display,case when waterfall_flag in ('Gross Ads') then fixed_account else null end as Gross_Adds,case when fixed_account is not null then fixed_account else null end as Active_Base
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
where month = date(dt)
)
,Sprint3_KPIsM as (
select distinct activebase_month as month,sum(activebase) as activebase,
		--Las que uso para kpis to june
sum(soft_dx) as soft_dx,sum(unique_neverpaid) as unique_neverpaid,sum(unique_longinstall) as unique_longinstall,sum(unique_earlytickets) as unique_earlyticket,sum(Unique_NoPlanChanges) as noplan,sum(unique_mountingills) as unique_mountingbill,
		--Las otras homologadas
sum(unique_sales) as unique_sales,sum(unique_softdx) as unique_softdx,
		--sum(unique_earlyinteraction) as unique_earlyinteraction, ticket es otro
		--sum(unique_billclaim) as unique_billclaim, nada de billclaim
sum(unique_mrcincrease) as unique_mrcchange
from "lla_cco_int_ana_prod"."cwc_operational_drivers_prod"
where activebase_month = date(dt) group by 1
) --select unique_mountingbill, activebase from Sprint3_KPIs
,Sprint3_KPIsS as (
select distinct sales_month as month,sum(unique_longinstall) as unique_longinstall,sum(unique_sales) as unique_sales
from "lla_cco_int_ana_prod"."cwc_operational_drivers_prod"
where activebase_month = date(dt) group by 1
),Sprint3_KPIsI as (
select distinct install_month as month,
		--Las que uso para kpis to june
sum(soft_dx) as soft_dx,sum(unique_neverpaid) as unique_neverpaid,sum(unique_earlytickets) as unique_earlyticket,sum(unique_mountingills) as unique_mountingbill,
		--Las otras homologadas
sum(unique_sales) as unique_sales,sum(unique_softdx) as unique_softdx
from "lla_cco_int_ana_prod"."cwc_operational_drivers_prod"
where activebase_month = date(dt) group by 1
)
,S3_CX_KPIsM as(
select distinct month,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,unique_mrcchange,noplan,unique_mountingbill,activebase,round(cast(unique_mrcchange as double) / cast(noplan as double),4) as Customers_w_MRC_Changes,round(cast(unique_mountingbill as double) / cast(activebase as double),4) as Customers_w_Mounting_Bills --,
		--round(cast(unique_earlyinteraction as double) / cast(unique_sales as double),4) as New_Customer_Callers
from Sprint3_KPIsM order by 1
)
,S3_CX_KPIsS as(
select distinct month,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,unique_longinstall,unique_sales,round(cast(unique_longinstall as double) / cast(unique_sales as double),4) as breech_cases_install
from Sprint3_KPIsS
order by 1
)
,S3_CX_KPIsI as(
select distinct month,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,unique_softdx,unique_earlyticket,unique_sales,round(cast(unique_softdx as double) / cast(unique_sales as double),4) as New_Sales_to_Soft_Dx,round(cast(unique_earlyticket as double) / cast(unique_sales as double),4) as Early_Tech_Tix 
from Sprint3_KPIsI order by 1
) 
,Sprint5_KPIs as(
select distinct Month,sum(over1_ticket) as RepeatedCallers,sum(activebase) fixed_accounts,sum(outlier_repairs) as outlier_repairs,sum(totaltickets) as numbertickets
from "lla_cco_int_ana_dev"."cwc_operational_drivers_5_dev" 
group by 1 order by 1
)
,S5_CX_KPIs as(
select distinct month,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,repeatedcallers,fixed_accounts,outlier_repairs,numbertickets,round(cast(sum(repeatedcallers) as double) / cast(sum(fixed_accounts) as double),4) as Repeat_Callers,round(cast(sum(outlier_repairs) as double) / cast(sum(fixed_accounts) as double),4) as Breech_Cases_Repair,round(cast(sum(numbertickets) as double) / cast(sum(fixed_accounts) as double),4) as Tech_Tix_per_100_Acct
	from Sprint5_KPIs
	group by 1,2,3,4,5,6,7,8,9,10,11,12
	order by 1,2,3,4,5,6
) -----------------New KPIs
,payments as(
select distinct month,opco,market,marketsize,product,biz_unit,'pay' as journey_waypoint,'digital_shift' as facet,'%digital_payments' as kpi_name,null as KPI_Sla, 'M-0' as Kpi_delay_display,round(cast(count(distinct digital) as double) / cast(count(distinct pymt_cnt) as double) ,2) as kpi_meas,count( distinct digital) as kpi_num,count(distinct pymt_cnt) as kpi_den
from(select date_trunc('month', date(dt)) as month,'CWC' as opco,'Jamaica' as market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,payment_doc_id as pymt_cnt,case when digital_nondigital = 'Digital' then payment_doc_id end as digital
FROM "db-stage-prod"."payments_cwc"
where account_type = 'B2C' and country_name = 'Jamaica'
group by 1,2,3,4,5,6,7,digital_nondigital,payment_doc_id)
group by 1,2,3,4,5,6,7,8,9,10,11
) ---------------------------------All Flags KPIs------------------------------------------------------------
-------Prev Calculated
,GrossAdds_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,0 as kpi_num,0 as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,count(distinct Gross_Adds) as kpi_meas from fmc_table
Group by 1,2,3,4,5,6,7,8,9,10
)
,ActiveBase_Flag1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,0 as kpi_num,null as KPI_Sla, 'M-0' as Kpi_delay_display,0 as kpi_den, count(distinct Active_Base) as kpi_meas from fmc_table
Group by 1,2,3,4,5,6,7,8,9,10,11
)
,ActiveBase_Flag2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-0' as Kpi_delay_display,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den from fmc_table
Group by 1,2,3,4,5,6,7,8,9,10,11
)
,TechTickets_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,round(Tech_Tix_per_100_Acct , 4) as kpi_meas,numbertickets as kpi_num,fixed_accounts as kpi_den
from S5_CX_KPIs
)
,MRCChanges_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,'contact_drivers' as facet,'pay' as journey_waypoint,'%Customers_w_MRC_Changes_5%+_excl_plan' as kpi_name,round(Customers_w_MRC_Changes , 4) as kpi_meas,unique_mrcchange as kpi_num,noplan as kpi_den	from S3_CX_KPIsm
)
,SalesSoftDx_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,'high_risk' as facet,'buy' as journey_waypoint,'%New_Sales_to_Soft_Dx' as kpi_name,round(New_Sales_to_Soft_Dx , 4) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den	from S3_CX_KPIsi
)
,LongInstall_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,'high_risk' as facet,'get' as journey_waypoint,'%breech_cases_install_6+days' as kpi_name,round(breech_cases_install , 4) as kpi_meas,unique_longinstall as kpi_num,unique_sales as kpi_den	from S3_CX_KPIss
)
,EarlyTickets_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,'high_risk' as facet,'get' as journey_waypoint,'%Early_Tech_Tix_-7weeks' as kpi_name,round(early_tech_tix , 4) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den from S3_CX_KPIsi
)
,RepeatedCall_Flag as (
select distinct month,Opco,Market,MarketSize,Product,null as KPI_Sla, null as Kpi_delay_display,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'%Repeat_Callers_2+calls' as kpi_name,round(Repeat_Callers , 4) as kpi_meas,repeatedcallers as kpi_num,fixed_accounts as kpi_den from S5_CX_KPIs
)
,OutlierRepair_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,'high_risk' as facet,'support-tech' as journey_waypoint,'%breech_bases_repair_4+_days' as kpi_name,round(Breech_Cases_Repair , 4) as kpi_meas,outlier_repairs as kpi_num,fixed_accounts as kpi_den from S5_CX_KPIs
)
,MountingBill_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,'high_risk' as facet,'pay' as journey_waypoint,'%Customers_w_Mounting_Bills' as kpi_name,round(Customers_w_Mounting_Bills , 4) as kpi_meas,unique_mountingbill as kpi_num,activebase as kpi_den from S3_CX_KPIsm
) ---------------------------------Join Flags-----------------------------------------------------------------
,Join_DNA_KPIs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den ,KPI_Sla,Kpi_delay_display
from(select month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display from GrossAdds_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display from ActiveBase_Flag1 union all	select month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display from ActiveBase_Flag2)
)
,Join_Sprints_KPIs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display
from(select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display from join_dna_kpis union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, Kpi_delay_display from TechTickets_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, Kpi_delay_display from MRCChanges_Flag	union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display from SalesSoftDx_Flag	union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display from LongInstall_Flag	union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display from EarlyTickets_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display from RepeatedCall_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, Kpi_delay_display from OutlierRepair_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display from MountingBill_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, 'M-0' as Kpi_delay_display from payments)
)
,Same_month as (
select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, 'M-0' as Kpi_delay_display from Join_Sprints_KPIs
where kpi_name not in ('%New_Sales_to_Soft_Dx','%Early_Tech_Tix_-7weeks','%breech_cases_install_6+days')
)
,Previous_month as (
select date_add('month', 1, month) as month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,Kpi_den,KPI_Sla, 'M-1' as Kpi_delay_display
from Join_Sprints_KPIs
where kpi_name in ('%Early_Tech_Tix_-7weeks','%breech_cases_install_6+days')
)
,soft as (
select date_add('month', 2, month) as month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, 'M-2' as Kpi_delay_display
from Join_Sprints_KPIs
where kpi_name = '%New_Sales_to_Soft_Dx'
)
,Final_Join_Sprints_KPIs as (
select * from(select * from same_month	union all select *	from previous_month	union all select * from soft)
)
,final_table as( select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,year(Month) as ref_year,month(month) as ref_mo
from Final_Join_Sprints_KPIs
where date_trunc('year', month) = date('2022-01-01') order by month)

---NotCalculated kpis
--BUY
,more2calls as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Customer_Callers_2+calls_21days' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,ecommerce as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'%eCommerce' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,tBuy as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'buy' as journey_waypoint,'tBuy' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,mttb as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,Buyingcalls as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calls/GA' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
--GET
,tinstall as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'get' as journey_waypoint,'tInstall' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,selfinstalls as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'%self_installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,installscalls as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
--PAY
,MTTBTR as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,tpay as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'pay' as journey_waypoint,'tpay' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,ftr_billing as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'%FTR_Billing' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
--Support-call
,helpcare as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'support-call' as journey_waypoint,'tHelp_Care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,frccare as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'%FRC_Care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
--support-Tech
,helprepair as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'support-tech' as journey_waypoint,'tHelp_repair' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
--use
,pnps as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'NPS_Detractorship' as facet,'use' as journey_waypoint,'pNPS' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
--Wanda's Dashboard
,billbill as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,cccare as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,'%CC_SL_Care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,cctech as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,'%CC_SL_Tech' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,chatbot as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,'%Chatbot_containment_care' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,carecall as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,'care_calls_per_1k_rgu' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,techcall as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,'tech_calls_per_1k_rgu' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
,chahtbottech as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,'%Chatbot_containment_Tech' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom from fmc_table)
-------------------------Service Delivery--------------
,service_delivery as(
select distinct Month,'cwc' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,sum(Installations) as Install,round(sum(Inst_MTTI)/sum(Installations),2) as MTTI,sum(Repairs) as Repairs,round(sum(Rep_MTTR)/sum(Repairs),2) as MTTR,round(sum(scr),2) as Repairs_1k_rgu,round((sum(FTR_Install_M)/sum(Installations))/100,4) as FTR_Install,round((sum(FTR_Repair_M)/sum(Repairs))/100,4) as FTR_Repair
from( select distinct date_trunc('month',date(date_parse(cast(end_week_adj as varchar),'%Y%m%d'))) as Month,Network,date(date_parse(cast(end_week_adj as varchar),'%Y%m%d')) as End_Week,sum(Total_Subscribers) as Total_Users,sum(Assisted_Installations) as Installations,sum(mtti) as MTTI,sum(Assisted_Installations)*sum(mtti) as Inst_MTTI,sum(truck_rolls) as Repairs,sum(mttr) as MTTR,sum(truck_rolls)*sum(mttr) as Rep_MTTR,sum(scr) as SCR,(100-sum(i_elf_28days)) as FTR_Install,(100-sum(r_elf_28days)) as FTR_Repair,(100-sum(i_elf_28days))*sum(Assisted_Installations) as FTR_Install_M,(100-sum(r_elf_28days))*sum(truck_rolls) as FTR_Repair_M
from "lla_cco_int_san"."cwp_ext_servicedelivery_result" where market='Jamaica' and network='OVERALL'
group by 1,2,3 order by 1,2,3) group by 1,2,3,4,5,6 order by 1,2,3,4,5,6)
--Service Delivery KPIs
,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom  from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom  from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'%FTR_installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom  from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom  from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom  from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'%FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom  from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_rgu' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,	year(Month) as ref_year, month(month) as ref_mofrom  from service_delivery)
--Nodes
,nodes_initial_table as 
(SELECT date_trunc('Month', date(interaction_start_time)) as Month, interaction_id, account_id_2, concat(account_id_2, cast(date_trunc('Month', date(interaction_start_time))as varchar)) as Account_Month
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account_id_2 from "db-stage-dev"."interactions_cwc"
where lower(org_cntry) like '%jam%') where length (account_id_2) = 8
GROUP BY 1,account_id_2, interaction_id,4
)
,nodes_table as (
select date_trunc('Month', date(dt)) as Month, act_acct_cd, max(NR_LONG_NODE) as NR_LONG_NODE,max(case when account_id_2 is not null then 1 else 0 end) as customer_with_ticket,case when length(act_acct_cd) = 8 Then 'Cerilion' else 'Liberate' END AS CRM
from "db-analytics-prod"."tbl_fixed_cwc" t left join nodes_initial_table i on t.act_acct_cd = i.account_id_2 and date_trunc('Month', date(t.dt)) = i.Month
where org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') and NR_LONG_NODE is not null AND NR_LONG_NODE <>'' AND NR_LONG_NODE <>' ' AND length(act_acct_cd)=8  
GROUP BY 1,act_acct_cd 
)
,grouped_by_node as (
select Month, CRM, count(distinct act_acct_cd) as customers_per_node, sum(customer_with_ticket) as customer_with_ticket, NR_LONG_NODE
from nodes_table
GROUP BY Month, NR_LONG_NODE, CRM
)
,final_nodes as (select Month, count(distinct NR_LONG_NODE) as nodes, sum(case when customer_with_ticket>0.06*customers_per_node then 1 else 0 end) overcharged_nodes
from grouped_by_node
where date_trunc('Year', date(Month)) = date('2022-01-01')
group by 1
order by 1 desc
)
,highrisk as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'%_High_Tech_Call_Nodes_+6%monthly' as kpi_name, round(cast(overcharged_nodes as double)/cast(nodes as double), 2) as kpi_meas, overcharged_nodes as kpi_num,	nodes as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,year(Month) as ref_year, month(month) as ref_mofrom from final_nodes group by 1,2,3,4,5,6,7,8,9,11,12,13,14,15,16
)
,final_full_kpis as (select * from final_table union all select * from more2calls union all select * from mttb union all select * from Buyingcalls union all select * from tbuy union all select * from ecommerce union all select * from MTTI union all select * from tinstall union all select * from ftr_installs union all select * from installs union all select * from selfinstalls union all select * from installscalls union all select * from MTTBTR union all select * from tpay union all select * from ftr_billing union all select * from helpcare union all select * from frccare union all select * from mttr  union all select * from helprepair union all select * from ftrrepair union all select * from repairs1k union all select * from highrisk union all select * from justrepairs union all select * from pnps union all select * from chahtbottech union all select * from techcall union all select * from carecall union all select * from chatbot union all select * from cctech union all select * from cccare union all select * from billbill
 )
select  Month,opco,market,marketsize,product,biz_unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,kpi_Sla,  kpi_delay_display,null as network,ref_year,ref_mo 
from final_full_kpis
