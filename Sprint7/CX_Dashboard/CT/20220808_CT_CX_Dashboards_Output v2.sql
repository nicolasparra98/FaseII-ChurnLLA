with
cx as(
  select *
  from `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-08-04_Cabletica_Final_Sprint7_Table_CX_DashboardInput_v2`
)
,Gross_adds_disc as(
select distinct month,network,'Gross_Adds' as kpi_name,kpi_meas as div from cx where facet='contact_drivers' and kpi_name='Active_Base')
,disclaimer_fields as(
select *,concat(cast(round(kpi_disclaimer_meas*100,2) as string),'% of base') as kpi_disclaimer_display
from(select j.*,case when j.kpi_name='Gross_Adds' then round(safe_divide(j.kpi_meas,g.div),4) else null end as kpi_disclaimer_meas
from cx j left join Gross_adds_disc g on j.month=g.month and j.network=g.network and j.kpi_name=g.kpi_name)
)

select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,null as KPI_Sla, kpi_delay_display,kpi_disclaimer_display,kpi_disclaimer_meas,Network,extract(year from date(Month)) as ref_year,extract(month from date(month)) as ref_mo,null as kpi_sla_below_threshold,null as kpi_sla_middling_threshold,null as kpi_sla_above_threshold,null as kpi_sla_far_below_threshold,null as kpi_sla_far_above_threshold
from disclaimer_fields
where date(month)>=date('2021-11-01')
order by kpi_name
