with dna_fields as (
SELECT distinct date_trunc('month',date(dt)) as month,act_acct_cd
,Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
    ELSE 'COPPER' end as TechFlag
    ,nr_short_node,nr_long_node,nr_terminal,nr_minibox,nr_cable,nr_tel_center,nr_odfx,nr_fdh,nr_fdp,nr_ont,nr_tv_stb_free_qty,nr_tv_stb_qty,nr_max_down,nr_bb_mac,nr_prjct_dt,nr_prjct_typ
FROM "db-analytics-prod"."fixed_cwp" 
where date_trunc('month',date(dt))>=date('2022-01-01') and act_cust_typ_nm = 'Residencial' 
)
,ftth_accounts_month_adj as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month_adj,trim(building_fdh) as trim_fdh,*
FROM "lla_cco_int_san"."cwp_con_ext_ftth_ad"
)
,ftth_project as(
select *, trim(b."nodo/fdh") as nodo,Tipo as Tech,b."home passed" as Home_Passed,b."velocidad máxima (coaxial)" as velocidad
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(provincia), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as provincias
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(DISTRITO), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as distritos
FROM "lla_cco_int_san"."cwp_ext_ftth" b
)
,ftth_fields as(
select distinct date(b.dt) as Month,Provincia,Distrito,acct_no,nodo,minibox,CATEGORIA,NODO_HFC,sum(home_passed) as home_passed
from ftth_project b left join ftth_accounts_month_adj a on b.nodo=a.trim_fdh and date(b.dt)=month_adj
where velocidad=1000
group by 1,2,3,4,5,6,7,8
)

select distinct f.month,f.acct_no,f.nodo as fdh,f.minibox,categoria,nodo_hfc,f.home_passed,TechFlag,nr_short_node,nr_long_node,nr_terminal,nr_minibox,nr_cable,nr_tel_center,nr_odfx,nr_fdh,nr_fdp,nr_ont,nr_tv_stb_free_qty,nr_tv_stb_qty,nr_max_down,nr_bb_mac,nr_prjct_dt,nr_prjct_typ
from dna_fields d inner join ftth_fields f on d.act_acct_cd=cast(f.acct_no as varchar) and d.month=f.month
where d.month=date('2022-05-01') --and minibox IS NOT NULL
