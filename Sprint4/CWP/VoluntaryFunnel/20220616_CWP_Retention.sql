with
Retention as(
select distinct case mes
                     when 'Febrero' then date('2022-02-01')
                     when 'Marzo' then date('2022-03-01')
                     when 'Abril' then date('2022-04-01')
                     when 'Mayo' then date('2022-05-01')
                end as Month
,account_number,retenidos
from "lla_cco_int_ext"."cwp_con_ext_reten" 
)
select distinct month,count(distinct account_number)
from retention
where retenidos=1
group by 1
order by 1
