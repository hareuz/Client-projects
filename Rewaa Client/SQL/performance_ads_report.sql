select
	'Performance Max' as Campaign,
	a.*,
	c.lead_count as Leads,
	c.deal_count as Deals,
	c.customer_count as Customers,
	c.junk_lead_count as Junk_Leads,
	c.lead_count-c.duplicated_lead_count-c.junk_lead_count as MQL,
	round(c.lead_count/a.clicks*100,2) as Conv,
	round(a.Budget/c.lead_count,2) as CPL
from
	"LasVegas".crm_counts c
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
		campaign_name like '%Leads-Performance Max%'
	group by
		1,
		2
	order by
		1 desc,
		2 asc) a
	
on
	a.year = c.year
	and a.month = c.month
where
	c.source_or_campaign like '%Performance Max%'