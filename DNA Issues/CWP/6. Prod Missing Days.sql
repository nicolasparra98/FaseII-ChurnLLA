select distinct date(load_dt) as Date,count(distinct act_acct_cd) as Users
from "db-analytics-prod"."fixed_cwp"
group by 1 order by 1
