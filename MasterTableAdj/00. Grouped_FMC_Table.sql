
SELECT distinct month as Month,final_bom_activeflag as Final_BOM_ActiveFlag,final_eom_activeflag as Final_EOM_ActiveFlag,fmcflag	 as FmcFlag
,f_activebom as F_ActiveBOM,f_activeeom	as F_ActiveEOM,fix_b_date as Fix_B_Date,count(distinct fixed_b_phone) as Fixed_B_Phone,b_overdue as B_Overdue,fixed_b_maxstart as Fixed_B_MaxStart,b_fixedtenure as B_FixedTenure,cast(round(b_fixed_mrc,0) as int) as B_Fixed_MRC,b_techflag as B_TechFlag,b_numrgus as B_NumRGUs,b_mixname_adj as B_MixName_Adj,b_mixcode_adj as B_MixCode_Adj,b_bbcode as B_bbCode,b_tvcode as B_tvCode,b_vocode as B_voCode,b_hard_fmc_flag as B_Hard_FMC_Flag,fix_e_date as Fix_E_Date
,count( distinct fixed_e_phone) as Fixed_E_Phone,e_overdue as E_Overdue,fixed_e_maxstart as Fixed_E_MaxStart,e_fixedtenure as E_FixedTenure,cast(round(e_fixed_mrc,0) as int)	 as E_Fixed_MRC,e_techflag	 as E_TechFlag,e_numrgus as E_NumRGUs,e_mixname_adj as E_MixName_Adj,e_mixcode_adj as E_MixCode_Adj	,e_bbcode as E_bbCode, e_tvcode as E_tvCode,e_vocode as E_voCode,e_hard_fmc_flag as E_Hard_FMC_Flag,first_sales_chnl_bom,last_sales_chnl_bom,	first_sales_chnl_eom,	last_sales_chnl_eom,fixedmainmovement as FixedMainMovement,fixedspinmovement as FixedSpinMovement,fixedchurnflag as FixedChurnFlag,fixedchurntype as FixedChurnType,fmcflagfix as FMCFlagFix
,count(distinct phonenumber) as PhoneNumber
,mobile_activebom as Mobile_ActiveBOM,mobile_activeeom as Mobile_ActiveEOM,b_date as B_Date
,count(distinct phone_bom) as Phone_BOM,mobile_b_maxstart as Mobile_B_MaxStart,b_mob_acc_name as B_Mob_Acc_Name
,count(distinct b_mobile_id) as B_Mobile_ID,b_mobilergus as B_MobileRGUs,b_mobiletenure as B_MobileTenure,e_date as E_Date,count(distinct phone_eom) as Phone_EOM,mobile_e_maxstart as Mobile_E_MaxStart,e_mob_acc_name as E_Mob_Acc_Name
,count(distinct e_mobile_id) as E_Mobile_ID,e_mobilergus as E_MobileRGUs,e_mobiletenure as E_MobileTenure,mobilemainmovement as MobileMainMovement,mobilespinflag as MobileSpinFlag,FmcFlagMob,drc	as DRC,mobilechurnflag as MobileChurnFlag,mobilechurnertype as MobileChurnerType
,finalchurnflag as FinalChurnFlag,churntypefinalflag as ChurnTypeFinalFlag,b_final_tenure as B_Final_Tenure,e_final_tenure as E_Final_Tenure,b_final_techflag as B_Final_TechFlag,e_final_techflag as E_Final_TechFlag,b_fmctype as B_FMCType,e_fmctype as E_FMCType,b_fmcsegment as B_FMCSegment,e_fmcsegment as E_FMCSegment,rejoinerflag as RejoinerFlag,rejoinerfmcflag as RejoinerFMCFlag,waterfall_flag as Waterfall_Flag,Downsell_Split,Downspin_Split
,fixedmonth
,fixedchurnsubtype,fixed_prmonth,fixed_rejoinermonth,gap,mobile_month,mobile_prmonth,mobile_rejoinermonth,b_totalrgus,e_totalrgus,potentialrejoinerflag,Partial_Total_ChurnFlag
,count(distinct finalaccount) as FinalAccount,count(distinct fixedaccount) as FixedAccount,count(distinct mobile_account) as Mobile_Account,avg(cast(round(mobile_mrc_diff,0) as int)) as Mobile_MRC_Diff,sum(cast(round(b_avgmobilemrc,0) as int)) as B_AvgMobileMRC,sum(cast(round(b_mobilemrc,0) as int)) as B_MobileMRC,sum(cast(round(b_total_mrc,0) as int)) as B_Total_MRC,sum(cast(round(e_total_mrc,0) as int)) as E_Total_MRC,sum(cast( round(e_avgmobilemrc,0) as int))	as E_AvgMobileMRC,sum(cast( round(e_mobilemrc,0) as int)) as E_MobileMRC
,count(distinct household_id) as Household_id,count(distinct mobile_household_id) as Mobile_Household_id
,count(distinct fixedaccount_bom) as fixedaccount_bom,count(distinct b_bb) as b_bb,count(distinct b_tv) as b_tv,count(distinct b_vo) as b_vo,count(distinct e_bb) as e_bb,count(distinct e_tv) as e_tv,count(distinct e_vo) as e_vo
from "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where month = date(dt) 
group by 1,2,3,4,5,6,7 --,8
,9,10,11,12,13,14,15,16,17,18,19,20,21--,22
,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43--,44
,45,46,47--,48
,49,50--,51
,52,53,54--,55
,56,57--,58
,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93
