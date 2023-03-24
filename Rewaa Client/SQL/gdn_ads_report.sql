select
	a.*,
	l.count as Leads,
	round(l.count/a.clicks*100,2) as Conv,
	round(a.Budget/l.count,2) as CPL
from
	"LasVegas".lead_count l
inner join(
	select
		extract(year from date) as year,
		extract(month from date) as month,
		sum(impressions) as Impressions,
		sum(clicks) as Clicks,
		round((sum(clicks)/nullif(sum(impressions),0))*100,2) as CTR,
		round(sum(cost_micros)/1000000,2) as Budget,
		round((sum(cost_micros)/1000000)/nullif(sum(clicks),0),2) as CPC
	from
		rewaa_google_ads.campaign_performance_report apr
	where
		campaign_name like '%Display_Network%'
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
	l.source_or_campaign like '%GDN%'