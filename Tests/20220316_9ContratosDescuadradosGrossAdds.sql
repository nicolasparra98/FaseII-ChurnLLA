SELECT org_cntry,cst_cust_cd,cst_cust_name,act_acct_cd
,act_cust_strt_dt,Min(act_cust_strt_dt) AS MinStartDate
,act_acct_inst_dt,load_dt,pd_mix_cd,fi_outst_age
,bundle_code
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.cwc_jam_dna_fullmonth_202202` 
WHERE act_acct_cd=
--50550018
--326053900000
--323053560000
--50548813
--50550551
--321053400000
--50550524
--327053770000
50550590

GROUP BY 1,2,3,4,5,7,8,9,10,11
ORDER BY load_dt
