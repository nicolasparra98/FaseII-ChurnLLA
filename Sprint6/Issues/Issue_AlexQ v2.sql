--/*
with
mrc_amts as(
Select distinct date(dt) as dt,ACT_ACCT_CD,
		--first_value(cast(fi_tot_mrc_amt as double)) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_MRC,
		round(cast(fi_tot_mrc_amt as double),0) as MRC_amt,
		round((cast(FI_VO_MRC_AMT as double) + cast(FI_BB_MRC_AMT as double) + cast(FI_TV_MRC_AMT as double) - cast(FL_TOT_SRV_CHRG_AMT_adj as double)),0) as MRC_proxy,
		--first_value(cast(fi_tot_mrc_amt as double)) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) - (cast(FI_VO_MRC_AMT as double) + cast(FI_BB_MRC_AMT as double) + cast(FI_TV_MRC_AMT as double) - cast(FL_TOT_SRV_CHRG_AMT as double)) as MRC_diff,
		round(cast(FI_VO_MRC_AMT as double),2) as voice_amt,
		round(cast(FI_BB_MRC_AMT as double),2) as bb_amt,
		round(cast(FI_TV_MRC_AMT as double),2) as tv_amt,
		round(cast(FL_TOT_SRV_CHRG_AMT_adj as double),2) as chrg_amt
	from (select distinct dt,act_acct_cd,fi_tot_mrc_amt,FI_VO_MRC_AMT,FI_BB_MRC_AMT,FI_TV_MRC_AMT,case when FL_TOT_SRV_CHRG_AMT='' or FL_TOT_SRV_CHRG_AMT is null then 0 else cast(FL_TOT_SRV_CHRG_AMT as double) end as FL_TOT_SRV_CHRG_AMT_adj
	FROM "db-analytics-prod"."tbl_fixed_cwc" --where FI_TOT_MRC_AMT IS NOT NULL
	where ORG_CNTRY = 'Jamaica' and date_trunc('month',date(dt))=date('2022-02-01')
	--and date(dt)=date('2022-02-28')
	)
)
select count(distinct act_acct_cd)
--dt,act_acct_cd,mrc_amt,mrc_proxy,round(mrc_amt-mrc_proxy,0) as mrc_difff,voice_amt,bb_amt,tv_amt,chrg_amt
--where mrc_proxy is not null
from mrc_amts
where mrc_amt<>mrc_proxy
--where act_acct_cd='870038950000'
--'125344670000'
--order by dt
--*/
/*
select distinct date(dt) as dt,date_trunc('month',date(dt)),act_acct_Cd, FI_TOT_MRC_AMT,FI_VO_MRC_AMT,FI_BB_MRC_AMT,FI_TV_MRC_AMT,FL_TOT_SRV_CHRG_AMT,FL_VO_SRV_CHRG_AMT,FL_BB_SRV_CHRG_AMT,FL_TV_SRV_CHRG_AMT
	FROM "db-analytics-prod"."tbl_fixed_cwc" --where FI_TOT_MRC_AMT IS NOT NULL
	where ORG_CNTRY = 'Jamaica' and date_trunc('month',date(dt))=date('2022-02-01')
	and act_acct_cd='870038950000'
	order by dt
*/
