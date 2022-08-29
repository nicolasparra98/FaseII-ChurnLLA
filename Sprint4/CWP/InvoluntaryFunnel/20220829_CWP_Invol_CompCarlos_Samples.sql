/*
select distinct date(dt) as dt,act_acct_cd,fi_outst_age
FROM "db-analytics-prod"."fixed_cwp" 
WHERE act_cust_typ_nm = 'Residencial'
--and date(dt) between date('2022-02-27') and date('2022-04-01')
and date(dt) = date('2022-03-02')
and act_acct_cd in('308025170000','316073400000','318075050000','298076970000','279033160000','313063190000')
order by 2,1
*/
select distinct date(dt) as dt,act_acct_cd,fi_outst_age
FROM "db-analytics-prod"."fixed_cwp" 
WHERE act_cust_typ_nm = 'Residencial'
and date(dt) in(date('2022-07-01'),date('2022-07-31'))
and act_acct_cd --in('316074300000','306080230000','313054040000','203066980000','313008950000')
--in('329004940000','304055430000','326005150000','324004490000','307023470000')
in('317017920000','328007770000','287062650000','323016880000','321028610000')
order by 2,1
