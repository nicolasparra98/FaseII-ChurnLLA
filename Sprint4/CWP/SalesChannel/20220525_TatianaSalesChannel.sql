
WITH FMC_Table AS
( SELECT * FROM
"lla_cco_int_stg"."cwp_sp3_basekpis_dashboardinput_dinamico_rj")

-----New Customers-----
,NEW_CUSTOMERS as (
Select 
act_acct_cd,load_dt, DATE_TRUNC('MONTH',CAST(load_dt AS DATE)) AS month_load,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS month_start,CAST(SUBSTR(pd_mix_cd,1,1) AS INT) AS n_rgu, max(date(act_acct_inst_dt)) as act_acct_inst_dt ,max(date(act_cust_strt_dt)) as act_cust_strt_dt,  DATE_DIFF ('DAY',CAST (max(act_cust_strt_dt) AS DATE),CAST (max(act_acct_inst_dt) AS DATE)) as Installation_lapse, 1 as NEW_CUSTOMER,
pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
,evt_frst_sale_chnl, evt_lst_sale_chnl
from --"db-analytics-prod"."fixed_cwp"
"lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial'
AND DATE_TRUNC('month',CAST(load_dt AS DATE)) = DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))
GROUP BY act_acct_cd, load_dt, DATE_TRUNC('MONTH',CAST(load_dt AS DATE)),CAST(act_cust_strt_dt AS DATE),
CAST(SUBSTR(pd_mix_cd,1,1) AS INT), --max(act_acct_inst_dt) , max(act_cust_strt_dt) , 
--DATE_DIFF ('DAY',CAST (max(act_cust_strt_dt) AS DATE),CAST (max(act_acct_inst_dt) AS DATE)),
1, pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media,evt_frst_sale_chnl, evt_lst_sale_chnl
)
,New_Customers_FLAG as(
SELECT f.*, a.load_dt,a.installation_lapse, a.new_customer,evt_frst_sale_chnl, evt_lst_sale_chnl,a.act_acct_inst_dt,a.act_cust_strt_dt,
CASE when f.FIRST_SALES_CHNL_BOM is not null and f.FIRST_SALES_CHNL_EOM is not null then f.FIRST_SALES_CHNL_EOM
when f.FIRST_SALES_CHNL_BOM is null and f.FIRST_SALES_CHNL_EOM is not null then f.FIRST_SALES_CHNL_EOM
WHEN  f.FIRST_SALES_CHNL_EOM is null and f.FIRST_SALES_CHNL_BOM is not null then f.FIRST_SALES_CHNL_BOM
END as SALES_CHANNEL,
CASE WHEN a.act_acct_cd is not null then 1 else 0 end as monthsale_flag
 FROM FMC_TABLE AS f left join NEW_CUSTOMERS AS a
ON f.finalaccount = a.act_acct_cd and f.month = a.month_load
)
,NewFebCustomers AS(
SELECT DISTINCT month,new_customer,monthsale_flag,finalaccount,act_acct_inst_dt,act_cust_strt_dt
,first_value(evt_frst_sale_chnl) over (partition by finalaccount order by load_dt) as evt_frst_sale_chnl
,first_value(evt_lst_sale_chnl) over (partition by finalaccount order by load_dt desc) as evt_lst_sale_chnl
FROM New_Customers_FLAG
WHERE month=date('2022-02-01') AND new_customer=1 AND monthsale_flag=1
)
,ServiceOrdersFebruary AS(
SELECT DISTINCT date(DATE_TRUNC('MONTH',order_start_date)) AS SO_Month, account_id,
first_value(channel_desc) over (partition by account_id order by order_start_date) as channel_desc
FROM "db-analytics-prod"."so_hdr_cwp"
WHERE ACCOUNT_TYPE='R' AND order_type ='INSTALLATION' AND DATE_TRUNC('MONTH',order_start_date)=date('2022-02-01') 
)
SELECT DISTINCT month,finalaccount,act_cust_strt_dt,evt_frst_sale_chnl,evt_lst_sale_chnl,channel_desc
FROM NewFebCustomers n INNER JOIN ServiceOrdersFebruary s ON n.finalaccount = cast(s.account_id as varchar)
where channel_desc is not null
--GROUP BY 1
ORDER BY finalaccount
