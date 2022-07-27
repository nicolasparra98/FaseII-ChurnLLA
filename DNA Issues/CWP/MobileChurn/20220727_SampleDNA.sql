select distinct *
FROM "db-analytics-dev"."tbl_postpaid_cwp"
where accountno in ('521954') --,'1674797','1818077','1834878','1666754')
and date_trunc('month',date(dt)) in (date('2022-04-01'),date('2022-05-01'),date('2022-06-01'),date('2022-07-01'))
order by 1
