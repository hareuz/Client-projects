select
	extract(year
from
	date_start) as year,
	extract(month
from
	date_start) as month,
	sum(impressions),
	sum(spend)
from
	rewaa_facebook_ads.ads_insights
where
	objective in ('CONVERSIONS', 'LEAD_GENERATION')
group by
	1,
	2
order by
	1 desc,
	2 asc





select extract(year from date_start),extract(month from date_start),sum(impressions) from rewaa_facebook_ads.ads_insights
where objective in ('CONVERSIONS','LEAD_GENERATION') and (campaign_name like '%Instagram%' or campaign_name like '%IG%')
group by 1,2
order by 1 desc, 2 asc


select distinct objective from rewaa_facebook_ads.ads_insights
where campaign_name not like '%Facebook%' and campaign_name not like '%Instagram%'