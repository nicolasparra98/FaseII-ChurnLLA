--### Query Nodes with more than 6% users with tickets ### ---

with parameters as (
select
--#####################################################################################
--### Change Date in this line to define period, max overdue and technologies #########
date('2022-06-01') as month_analysis,
90 as max_overdue
--####################################################################################
),

home_integrity_node_base as (
    select mac_address, max(replace(mac_address,':','')) as MAC_JOIN,
            max(first_node_name) as first_node_name,
            min(first_fecha_carga) as first_fecha_carga
    from (
        select mac_address, 
        first_value(node_name) over(partition by mac_address order by fecha_carga) as first_node_name,
        first_value(fecha_carga) over(partition by mac_address order by fecha_carga) as first_fecha_carga
        from "db-stage-dev"."home_integrity_history" where DATE_TRUNC('month',date(fecha_carga)) = (select month_analysis from parameters)
        and node_name is not null
        )
    group by mac_address
),

map_mac_account as (
    select act_acct_cd, max(first_nr_bb_mac) as nr_bb_mac, max(first_fi_outst_age) as first_fi_outst_age
    from 
       ( select act_acct_cd, 
        first_value(nr_bb_mac) over(partition by act_acct_cd order by dt) as first_nr_bb_mac,
        first_value(fi_outst_age) over(partition by act_acct_cd order by dt) as first_fi_outst_age
        from "db-analytics-prod"."fixed_cwp"
        where date(dt) = (select month_analysis from parameters) and nr_bb_mac is not null)
        --where DATE_TRUNC('month',date(dt)) = (select month_analysis from parameters) and nr_bb_mac is not null)
    where (cast(first_fi_outst_age as int) < (select max_overdue from parameters) or first_fi_outst_age is null)
    group by act_acct_cd
   
), 

join_account_id as (
    select a.*,b.*
    from map_mac_account a
    left join home_integrity_node_base  b
    on b.MAC_JOIN = a.nr_bb_mac
    where  b.MAC_JOIN is not null
),

interactions_panel as (
    select ACCOUNT_ID as interactions_account_id, 
    count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'CLAIM' then date(INTERACTION_START_TIME) end) as num_total_claims,
    count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) as num_tech_tickets,
    count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TRUCKROLL' then date(INTERACTION_START_TIME) end) as num_tech_truckrolls,
    case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'CLAIM' then date(INTERACTION_START_TIME) end) > 0 then 1 else 0 end as claims_flag,
    case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) > 0 then 1 else 0 end as tickets_flag,
    case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TRUCKROLL' then date(INTERACTION_START_TIME) end)  > 0 then 1 else 0 end as truckroll_flag,
    array_agg(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) as list_dates_tickets,
    array_agg(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then interaction_id end) as list_interaction_id_tickets
    from "db-stage-prod"."interactions_cwp"
        where DATE_TRUNC('month',date(INTERACTION_START_TIME)) = (select month_analysis from parameters)
        and ACCOUNT_ID in (select act_acct_cd from join_account_id) and interaction_id is not null 
        and INTERACTION_ID NOT LIKE '%-%'
    group by ACCOUNT_ID
),

join_interactions as (
    select a.*,b.*
    from join_account_id a
    left join interactions_panel b
    on a.act_acct_cd = b.interactions_account_id
),

group_node as (
select
hfc_node,
accounts_with_tickets,
total_accounts,
accounts_with_tickets*100/total_accounts as percentage_accounts_with_tickets
from 
    (select first_node_name as hfc_node,
    cast(sum(tickets_flag) as double) as accounts_with_tickets,
    cast(count(distinct act_acct_cd) as double) as total_accounts
    from join_interactions 
    group by first_node_name)
),

nodes_by_severity as (
select total_nodes,
nodes_higher_than_6_perc,
nodes_higher_than_6_perc*100/total_nodes as kpi_percentage
from( select
    cast(count(distinct hfc_node) as double) as total_nodes,
    cast(count(distinct case when percentage_accounts_with_tickets > 6 then hfc_node end) as double) as nodes_higher_than_6_perc
    from group_node
    )
)

select * from  nodes_by_severity --join_interactions
