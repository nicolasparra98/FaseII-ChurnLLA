WITH FMC_Table AS
( select distinct month, B_Final_TechFlag, B_FMCSegment, B_FMCType,E_Final_TechFlag, E_FMCSegment, E_FMCType,b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure,b_numrgus,e_numrgus,finalchurnflag,fixedchurntype,fixedchurnflag,fixedchurnsubtype,fixedmainmovement,waterfall_flag,finalaccount,fixedaccount,f_activebom,f_activeeom,mobile_activeeom,mobilechurnflag
  FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
  --"lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
  where month=date(dt)
)
,Dx_Attempts AS(
select distinct date_trunc('Month', date(completed_date)) as month,account_id
from "db-analytics-prod"."so_hdr_cwp" 
where order_type = 'DEACTIVATION'
and (cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91')
or cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA')
)
,Retention as(
select distinct case mes
                     when 'Febrero' then date('2022-02-01')
                     when 'Marzo' then date('2022-03-01')
                     when 'Abril' then date('2022-04-01')
                     when 'Mayo' then date('2022-05-01')
                end as Month
,account_number,retenidos,canal,r."razon resumida" as razon_resumida,save_id
,case when canal='RCOE' THEN 'RCOE' else 'Other Channels'
end as Channel
from "lla_cco_int_ext"."cwp_con_ext_reten" r
--where canal='RCOE'
)
,Attempt_Dx_Flag AS(
select distinct f.*
,case when account_id is not null then finalaccount else null end as Dx_Attempt_Prel
from fmc_table f left join dx_attempts d on f.finalaccount=cast(d.account_id as varchar) and f.month=d.month
)
,Retention_Flags AS(
select distinct f.*
,case when r.account_number is not null then finalaccount else null end as RetentionCenter
,case when retenidos=1 and fixedchurntype is null then finalaccount else null end as Retained
,case when fixedchurntype='1. Fixed Voluntary Churner' or r.account_number is not null then finalaccount else null end as Dx_Attempt
,case when fixedchurntype='1. Fixed Voluntary Churner' then 'Voluntary Churners'
      when r.account_number is not null and fixedchurntype<>'1. Fixed Voluntary Churner' then 'Other Churners'
      when retenidos=0 and fixedchurntype is null then 'Not Retained' 
else null end as Dx_Type
,case when (razon_resumida in('Cambio de Tecnologia','Desco Paquete Esencial','No tipificado','Moving') or save_id LIKE '%espera%') and retenidos=0 and fixedchurntype is null then 1 else 0 end as Exclude
,case when r.account_number is not null and retenidos=0 and fixedchurntype is null and fixedmainmovement NOT IN('3.Downsell','6.Null last day') then 1 else 0 end as Exclude_All
,case when fixedmainmovement IN('3.Downsell','6.Null last day') then 1 else 0 end as Partial_Churn
,case when fixedmainmovement IN('3.Downsell','6.Null last day') or fixedchurntype is not null then finalaccount else null end as Total_Dx
,case when lag(fixedmainmovement) over (partition by finalaccount order by f.month desc) IN('3.Downsell','6.Null last day') or lag(fixedchurntype) over (partition by finalaccount order by f.month desc) is not null then 1 else 0 end as Downsell_Churner_NextMonth
,canal,razon_resumida,channel
,lag(fixedmainmovement) over (partition by finalaccount order by f.month desc) as next_mainmo
,lag(fixedchurntype) over (partition by finalaccount order by f.month desc) as next_churn
from Attempt_Dx_Flag f left join retention r on f.finalaccount=cast(r.account_number as varchar) and f.month=r.month
)
,RGUs_flags as(
select distinct *
,case when dx_attempt is not null and fixedmainmovement='3.Downsell' then b_numrgus-e_numrgus
      when dx_attempt is not null and dx_type is not null and (fixedmainmovement='6.Null last day' or fixedchurntype is not null) then b_numrgus
      when retained is not null and fixedchurntype is null then e_numrgus
else null end as churn_ret_rgus
,case when dx_attempt is not null and fixedmainmovement='3.Downsell' then b_numrgus-e_numrgus
      when dx_attempt is not null and dx_type is not null and (fixedmainmovement='6.Null last day' or fixedchurntype is not null)  then b_numrgus
else null end as churned_rgus
,case when retained is not null and fixedchurntype is null then e_numrgus else null end as retained_rgus
,case when dx_type is not null then b_numrgus
      when dx_attempt is not null and fixedmainmovement='3.Downsell' then b_numrgus-e_numrgus
else null end as prueba_dxrgus

from retention_flags
)
select distinct
Month, E_Final_TechFlag,E_FMCSegment,E_FMCType,E_Final_Tenure,B_Final_TechFlag,B_FMCSegment,B_FMCType,B_Final_Tenure,FinalChurnFlag,f_activebom,f_activeeom,FixedChurnType,FixedChurnFlag,fixedchurnsubtype,fixedmainmovement,Waterfall_Flag,Exclude,Exclude_All,Channel,Dx_Type,b_numrgus,count(distinct FinalAccount) as TotalAccounts,count(distinct FixedAccount) as FixedAccounts, count(distinct total_dx) as Dx_Users,count(distinct dx_attempt) as Dx_Attempt,count(distinct RetentionCenter) as RetentionCenter,sum(churned_rgus) as Dx_RGUs
--The following columns are not being used (calculated on powerbi)
,count(distinct Retained) as RetainedUsers,sum(churn_ret_rgus) as Dx_AttemptRGUs,sum(retained_rgus) as RetainedRGUs
--Calculated Fields: Attempted_RGUs,RetCenter_RGUs,Retained_RGUs
from rgus_flags
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
order by 1,2
