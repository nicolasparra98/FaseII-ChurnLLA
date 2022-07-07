with
-------------------------------------Previoulsy Calculated KPIs-------------------------------------------------
FMC_Table AS ( 
SELECT *,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit
,case when fixedmainmovement='4.New Customer' then fixedaccount else null end as Gross_Adds
,case when fixedaccount is not null then fixedaccount else null end as Active_Base
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where month=date(dt)
)
,Sprint3_KPIs as (
select distinct month,sum(activebase) as activebase,sum(unique_sales) as unique_sales,sum(unique_softdx) as unique_softdx,sum(unique_neverpaid) as unique_neverpaid,sum(unique_longinstall) as unique_longinstall,sum(unique_earlyinteraction) as unique_earlyinteraction,sum(unique_earlyticket) as unique_earlyticket,sum(unique_billclaim) as unique_billclaim,sum(unique_mrcchange) as unique_mrcchange,sum(unique_mountingbill) as unique_mountingbill,sum(noplan) as noplan
from "lla_cco_int_ana_prod"."cwp_operational_drivers_prod" where month=date(dt) group by 1
)
,S3_CX_KPIs as(
select distinct month,round(cast(unique_mountingbill as double)/cast(unique_mrcchange as double),4) as Customers_w_MRC_Changes,round(cast(noplan as double)/cast(activebase as double),4) as Customers_w_Mounting_Bills,round(cast(unique_softdx as double)/cast(unique_sales as double),4) as New_Sales_to_Soft_Dx,round(cast(unique_longinstall as double)/cast(unique_sales as double),4) as breech_cases_install,round(cast(unique_earlyticket as double)/cast(unique_sales as double),4) as Early_Tech_Tix, round(cast(unique_earlyinteraction as double)/cast(unique_sales as double),4) as New_Customer_Callers
from Sprint3_KPIs order by 1
)
,Sprint5_KPIs as(
select distinct Month,case when InteractionsTier in('2','>3') then sum(usersinteractions) end as RepeatedCallers
,sum(fixed_accounts) fixed_accounts,sum(outlierrepairs) as outlier_repairs,sum(numbertickets) as numbertickets
from "lla_cco_int_stg"."cwp_operationaldrivers2_temp" group by 1,interactionstier order by 1
)
,S5_CX_KPIs as(
select distinct month,round(cast(sum(repeatedcallers) as double)/cast(sum(fixed_accounts) as double),4) as Repeat_Callers,round(cast(sum(outlier_repairs) as double)/cast(sum(fixed_accounts) as double),4) as Breech_Cases_Repair,round(cast(sum(numbertickets) as double)/cast(sum(fixed_accounts) as double),4) as Tech_Tix_per_100_Acct
from Sprint5_KPIs group by 1 order by 1
)
---------------------------------All Flags KPIs------------------------------------------------------------
-------Prev Calculated
,GrossAdds_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,count(distinct Gross_Adds) as kpi_meas
from fmc_table Group by 1,2,3,4,5,6,7,8,9
)
,ActiveBase_Flag1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas
from fmc_table Group by 1,2,3,4,5,6,7,8,9
)
,ActiveBase_Flag2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas
from fmc_table Group by 1,2,3,4,5,6,7,8,9
)
---------------------------------Join Flags-----------------------------------------------------------------
,Join_DNA_KPIs as(
select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas
from( select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas from GrossAdds_Flag
      union all
      select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas from ActiveBase_Flag1
      union all
      select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas from ActiveBase_Flag2)
)
select *
from join_dna_kpis
