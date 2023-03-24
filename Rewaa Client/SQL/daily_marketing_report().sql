CREATE OR REPLACE FUNCTION dwh.daily_marketing_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
	BEGIN

drop table if exists tuned_lead_table;
create temp table tuned_lead_table as
select
	id,
	created_time,
	COALESCE(lead_source, custom_campaign) AS source_or_campaign,
	lead_status 
from
	rewaa_zoho_crm."lead";


drop table if exists lead_refined_campaign;
create temp table lead_refined_campaign as
select	
	id,
	created_time,
	case 
		when source_or_campaign like '%Instagram%' or source_or_campaign like '%instagram%' or source_or_campaign like '%Facebook%' or source_or_campaign like '%facebook%' then 'Facebook & Instagram'
		when source_or_campaign like '%Snapchat%' or source_or_campaign like '%snapchat%' then 'Snapchat'
		when source_or_campaign like '%Discovery%' then 'Discovery'
		when source_or_campaign like '%Performance_Max%'  then 'Performance Max'
		when source_or_campaign like '%Youtube%' or source_or_campaign like '%YouTube%' then 'Youtube'
		when source_or_campaign like '%GDN%' or source_or_campaign like '%Display%' then 'GDN'
		when source_or_campaign like '%Search%' or source_or_campaign like '%search%' then 'Search Ads'
		else null 
	end as source_or_campaign,
	lead_status
from tuned_lead_table;


drop table if exists lead_count;
create temp table lead_count as 
select
	date(created_time) as date,
	source_or_campaign ,
	count(id) as lead_count
from
	lead_refined_campaign
group by
	1,
	2
order by
	1 desc;

-- Junk Leads --
drop table if exists junk_lead_count;
create temp table junk_lead_count as 
select
	date(created_time) as date,
	source_or_campaign ,
	count(id) as junk_lead_count
from
	lead_refined_campaign
where
	lead_status='Junk Lead'
group by
	1,
	2
order by
	1 desc;

-- Duplicated Leads --
drop table if exists duplicated_lead_count;
create temp table duplicated_lead_count as 
select
	date(created_time) as date,
	source_or_campaign ,
	count(id) as duplicated_lead_count
from
	lead_refined_campaign
where
	lead_status='Duplicated'
group by
	1,
	2
order by
	1 desc;
------------------------------------------------


-- Deal & Customer Count -------
drop table if exists tuned_deal_table;
create temp table tuned_deal_table as
select
	d.id,
	d.created_time,
	COALESCE(l.lead_source, l.custom_campaign) AS source_or_campaign,
	stage
from
	rewaa_zoho_crm.deal d
LEFT JOIN 
	rewaa_zoho_crm.lead l ON l.converted_detail_deal::text = d.id::text;


drop table if exists deal_refined_campaign;
create temp table deal_refined_campaign as
select	
	id,
	created_time,
	case 
		when source_or_campaign like '%Instagram%' or source_or_campaign like '%instagram%' then 'Instagram'
		when source_or_campaign like '%Facebook%' or source_or_campaign like '%facebook%' then 'Facebook'
		when source_or_campaign like '%Snapchat%' or source_or_campaign like '%snapchat%' then 'Snapchat'
		when source_or_campaign like '%Discovery%' then 'Discovery'
		when source_or_campaign like '%Performance_Max%' then 'Performance Max'
		when source_or_campaign like '%Youtube%' or source_or_campaign like '%YouTube%' then 'Youtube'
		when source_or_campaign like '%GDN%' or source_or_campaign like '%Display%' then 'GDN'
		when source_or_campaign like '%Search%' or source_or_campaign like '%search%' then 'Search Ads'
		else null 
	end as source_or_campaign,
	stage
from tuned_deal_table;

--Deal Count--
drop table if exists deal_count;
create temp table deal_count as 
select
	date(created_time) as date,
	source_or_campaign ,
	count(id) as deal_count
from
	deal_refined_campaign
group by
	1,
	2
order by
	1 desc;

-- Customers --
drop table if exists customer_count;
create temp table customer_count as 
select
	date(created_time) as date,
	source_or_campaign ,
	count(id) as customer_count
from
	deal_refined_campaign
where
	stage='Contract Signed-توقيع العقد'
group by
	1,
	2
order by
	1 desc;


drop table if exists crm_counts;
create temp table crm_counts as 
select l.*,j.junk_lead_count,d.duplicated_lead_count,dc.deal_count,c.customer_count from junk_lead_count j
left join lead_count l 
on j.date=l.date and j.source_or_campaign=l.source_or_campaign
left join duplicated_lead_count d
on d.date=l.date and d.source_or_campaign=l.source_or_campaign
left join deal_count dc
on dc.date=l.date and dc.source_or_campaign=l.source_or_campaign
left join customer_count c
on c.date=l.date and c.source_or_campaign=l.source_or_campaign;


-------------------------------Performance Max--------------------------------------------------------------
drop table if exists perf_max_report;
create temp table perf_max_report as
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
	crm_counts c
inner join(
	select
		date(date) as date,
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
		1
	order by
		1 desc) a
	
on
	a.date = c.date
where
	c.source_or_campaign like '%Performance Max%';

-------------------------------Youtube--------------------------------------------------------------
drop table if exists youtube_report;
create temp table youtube_report as
select
	'Youtube' as Campaign,
	a.*,
	c.lead_count as Leads,
	c.deal_count as Deals,
	c.customer_count as Customers,
	c.junk_lead_count as Junk_Leads,
	c.lead_count-c.duplicated_lead_count-c.junk_lead_count as MQL,
	round(c.lead_count/a.clicks*100,2) as Conv,
	round(a.Budget/c.lead_count,2) as CPL
from
	crm_counts c
inner join(
	select
		date(date) as date,
		sum(impressions) as Impressions,
		sum(clicks) as Clicks,
		round((sum(clicks)/nullif(sum(impressions),0))*100,2) as CTR,
		round(sum(cost_micros)/1000000,2) as Budget,
		round((sum(cost_micros)/1000000)/nullif(sum(clicks),0),2) as CPC
	from
		rewaa_google_ads.video_performance_report apr
	group by
		1
	order by
		1 desc) a
	
on
	a.date = c.date
where
	c.source_or_campaign like '%Youtube%';


-------------------------------GDN--------------------------------------------------------------
drop table if exists gdn_report;
create temp table gdn_report as
select
	'GDN' as Campaign,
	a.*,
	c.lead_count as Leads,
	c.deal_count as Deals,
	c.customer_count as Customers,
	c.junk_lead_count as Junk_Leads,
	c.lead_count-c.duplicated_lead_count-c.junk_lead_count as MQL,
	round(c.lead_count/a.clicks*100,2) as Conv,
	round(a.Budget/c.lead_count,2) as CPL
from
	crm_counts c
inner join(
	select
		date(date) as date,
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
		1
	order by
		1 desc) a
	
on
	a.date = c.date
where
	c.source_or_campaign like '%GDN%';

-------------------------------Search Ads--------------------------------------------------------------
drop table if exists search_report;
create temp table search_report as
select
	'Search Ads' as Campaign,
	a.*,
	c.lead_count as Leads,
	c.deal_count as Deals,
	c.customer_count as Customers,
	c.junk_lead_count as Junk_Leads,
	c.lead_count-c.duplicated_lead_count-c.junk_lead_count as MQL,
	round(c.lead_count/a.clicks*100,2) as Conv,
	round(a.Budget/c.lead_count,2) as CPL
from
	crm_counts c
inner join(
	select
		date(date) as date,
		sum(impressions) as Impressions,
		sum(clicks) as Clicks,
		round((sum(clicks)/nullif(sum(impressions),0))*100,2) as CTR,
		round(sum(cost_micros)/1000000,2) as Budget,
		round((sum(cost_micros)/1000000)/nullif(sum(clicks),0),2) as CPC
	from
		rewaa_google_ads.account_performance_report apr
	where
		ad_network_type in ('SEARCH', 'SEARCH_PARTNERS')
	group by
		1
	order by
		1 desc) a
	
on
	a.date = c.date
where
	c.source_or_campaign like '%Search Ads%';


-------------------------------Discovery--------------------------------------------------------------
drop table if exists discovery_report;
create temp table discovery_report as
select
	'Discovery' as Campaign,
	a.*,
	c.lead_count as Leads,
	c.deal_count as Deals,
	c.customer_count as Customers,
	c.junk_lead_count as Junk_Leads,
	c.lead_count-c.duplicated_lead_count-c.junk_lead_count as MQL,
	round(c.lead_count/a.clicks*100,2) as Conv,
	round(a.Budget/c.lead_count,2) as CPL
from
	crm_counts c
inner join(
	select
		date(date) as date,
		sum(impressions) as Impressions,
		sum(clicks) as Clicks,
		round((sum(clicks)/nullif(sum(impressions),0))*100,2) as CTR,
		round(sum(cost_micros)/1000000,2) as Budget,
		round((sum(cost_micros)/1000000)/nullif(sum(clicks),0),2) as CPC
	from
		rewaa_google_ads.ad_group_performance_report apr
	where 
		campaign_name like '%Discovery%'

	group by
		1
	order by
		1 desc) a
	
on
	a.date = c.date
where
	c.source_or_campaign like '%Discovery%';

-------------------------------FB & Insta--------------------------------------------------------------
drop table if exists fb_insta_report;
create temp table fb_insta_report as
select
	'Facebook & Instagram' as Campaign,
	a.*,
	c.lead_count as Leads,
	c.deal_count as Deals,
	c.customer_count as Customers,
	c.junk_lead_count as Junk_Leads,
	c.lead_count-c.duplicated_lead_count-c.junk_lead_count as MQL,
	c.lead_count/a.clicks*100 as Conv,
	a.Budget/c.lead_count as CPL
from
	crm_counts c
inner join(
	select
		date(date) as date,
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
		1
	order by
		1 desc) a
	
on
	a.date = c.date
where
	c.source_or_campaign like '%Facebook & Instagram%';


drop table if exists dwh.daily_marketing_report;
create table dwh.daily_marketing_report as
select * from search_report
union
select * from fb_insta_report
union
select * from gdn_report
union
select * from perf_max_report
union 
select * from discovery_report
union 
select * from youtube_report;




	END;
$function$
;
