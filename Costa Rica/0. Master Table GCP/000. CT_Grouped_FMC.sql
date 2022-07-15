with
fmc_table as(
select *
from `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-07-14_Cabletica_FMC_DashboardInput`
)
,repeated_accounts as(
select distinct month as r_month,final_account as r_account,count(*) as repeated
from fmc_table
group by 1,2 order by 1,2
)
,repeated_fixed as(
select distinct month as r_month,fixed_account as r_account,count(*) as rep_fixed
from fmc_table
group by 1,2 order by 1,2
)
,repeated_mobile as(
select distinct month as r_month,mobile_account as r_account,count(*) as rep_mobile
from fmc_table
group by 1,2 order by 1,2
)
,fmc_adj as(
select l.*,repeated,rep_fixed,rep_mobile
from fmc_table l 
 left join repeated_accounts r on l.month=r.r_month and l.final_account=r.r_account
 left join repeated_fixed f on l.month=f.r_month and l.fixed_account=f.r_account
 left join repeated_mobile m on l.month=m.r_month and l.mobile_account=m.r_account
)
,prueba as(
SELECT distinct 
Month,Final_BOM_ActiveFlag,Final_EOM_ActiveFlag,B_FMC_Status,E_FMC_Status,Fixed_Month,ActiveBOM,ActiveEOM,B_Date,B_VO_id,B_VO_nm,B_TV_id,B_TV_nm,B_BB_id,B_BB_nm,B_RGU_VO,B_RGU_TV,B_RGU_BB,B_NumRGUs,B_Overdue,B_Tenure,B_MinInst,B_Bundle_Type,B_BundleName,B_MIX,B_TechAdj,B_FixedTenureSegment,B_MORA,B_ACT_ACCT_SIGN_DT,BB_RGU_BOM,TV_RGU_BOM,VO_RGU_BOM,B_MixCode_Adj,E_Date,E_VO_id,E_VO_nm,E_TV_id,E_TV_nm,E_BB_id,E_BB_nm,E_RGU_VO,E_RGU_TV,E_RGU_BB,E_NumRGUs,E_Overdue,E_Tenure,E_MinInst,E_Bundle_Type,E_BundleName,E_MIX,E_TechAdj,E_FixedTenureSegment,E_MORA
,E_ACT_ACCT_SIGN_DT,BB_RGU_EOM,TV_RGU_EOM,VO_RGU_EOM,E_MixCode_Adj,MainMovement,GainMovement,SpinMovement,FixedChurnTypeFlag,Fixed_PR,Fixed_Rejoiner,RGU_Churn,B_PLAN,E_PLAN,Mobile_Month,Mobile_ActiveBOM,Mobile_ActiveEOM,B_FMCAccount,E_FMCAccount,Mobile_MRC_BOM,Mobile_MRC_EOM,B_Mobile_MaxStart,E_Mobile_MaxStart,Mobile_B_TenureDays,B_MobileTenureSegment,Mobile_E_TenureDays,E_MobileTenureSegment,MobileMovementFlag
,MobileSpinFlag,MobileChurnFlag,MobileChurnType,Mobile_PRMonth,Mobile_RejoinerMonth,B_FMCType,E_FMCType,B_MobileRGUs,E_MobileRGUs,B_FinalTechFlag,E_FinalTechFlag,B_TenureFinalFlag,E_TenureFinalFlag,B_FMC_Segment,FinalChurnFlag,B_TotalRGUs,E_TotalRGUs,Rejoiner_FinalFlag,Partial_Total_ChurnFlag,churntypefinalflag,E_FMC_Segment,Waterfall_Flag,Downsell_Split,Downspin_Split,repeated,rep_fixed,rep_mobile
--Si hay tiempo doble click en diferencia
,count(distinct final_account)/repeated as Final_Account
,count(distinct fixed_account)/rep_fixed as Fixed_Account
,count(distinct mobile_account)/rep_mobile as Mobile_Account
--Arreglar RGUs (que no hayan repetidos)
,sum(cast(round(B_VO_MRC,0) as int)) as B_VO_MRC,sum(cast(round(B_BB_MRC,0) as int)) as B_BB_MRC,sum(cast(round(B_TV_MRC,0) as int)) as B_TV_MRC,sum(cast(round(B_AVG_MRC,0) as int)) as B_AVG_MRC,sum(cast(round(B_BILL_AMT,0) as int)) as B_BILL_AMT,sum(cast(round(E_VO_MRC,0) as int)) as E_VO_MRC,sum(cast(round(E_BB_MRC,0) as int)) as E_BB_MRC,sum(cast(round(E_TV_MRC,0) as int)) as E_TV_MRC,sum(cast(round(E_AVG_MRC,0) as int)) as E_AVG_MRC,sum(cast(round(E_BILL_AMT,0) as int)) as E_BILL_AMT,avg(cast(round(DIF_TOTAL_RGU,0) as int)) as DIF_TOTAL_RGU,avg(cast(round(mobile_mrc_diff,0) as int)) as Mobile_MRC_Diff,sum(FixedCount) as FixedCount,sum(cast(round(TOTAL_B_MRC,0) as int)) as TOTAL_B_MRC,sum(cast(round(TOTAL_E_MRC,0) as int)) as TOTAL_E_MRC,avg(cast(round(MRC_Change,0) as int)) as MRC_Change

from fmc_adj
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108
)
select distinct month,cast(round(sum(final_account),0) as int) as final,cast(round(sum(fixed_account),0) as int) as fixed,cast(round(sum(mobile_account),0) as int) as mobile
from prueba
where Final_BOM_ActiveFlag=1
group by 1--,2
order by 1--,2
