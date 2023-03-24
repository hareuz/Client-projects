select
	a.*,
	l.count as Leads,
	l.count/a.clicks*100 as Conv,
	a.Budget/l.count as CPL
from
	"LasVegas".lead_count l
inner join(
	select
		extract(year from date) as year,
		extract(month from date) as month,
		sum(impressions) as Impressions,
		sum(clicks) as Clicks,
		sum(clicks)/ sum(impressions)* 100 as CTR,
		round(sum(cost_micros)/1000000,2) as Budget,
		round((sum(cost_micros)/1000000)/sum(clicks),2) as CPC
	from
		rewaa_google_ads.ad_group_performance_report apr
	where campaign_name like '%Discovery%'
	group by
		1,
		2
	order by
		1 desc,
		2 asc) a
	
on
	a.year = l.year
	and a.month = l.month
where
	l.source_or_campaign like '%Discovery%'
