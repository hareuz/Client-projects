select
	a.*,
	l.count as Leads,
	l.count/a.clicks*100 as Conv,
	a.Budget/l.count as CPL
from
	"LasVegas".lead_count l
inner join(
	select
		extract(year from date_start) as year,
		extract(month from date_start) as month,
		sum(impressions) as Impressions,
		sum(clicks) as Clicks,
		sum(clicks)/ sum(impressions)* 100 as CTR,
		sum(spend) as Budget,
		sum(spend)/sum(clicks) as CPC
	from
		rewaa_facebook_ads.ads_insights apr
	where
		objective in ('CONVERSIONS', 'LEAD_GENERATION')
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
	l.source_or_campaign like '%Facebook%'
