
drop table if exists "LasVegas".tuned_lead_table;
create table "LasVegas".tuned_lead_table as
select
	id,
	created_time,
	COALESCE(lead_source, custom_campaign) AS source_or_campaign,
	lead_status 
from
	rewaa_zoho_crm."lead";


drop table if exists "LasVegas".lead_refined_campaign;
create table "LasVegas".lead_refined_campaign as
select	
	id,
	created_time,
	case 
		when source_or_campaign like '%Instagram%' or source_or_campaign like '%instagram%' then 'Instagram'
		when source_or_campaign like '%Facebook%' or source_or_campaign like '%facebook%' then 'Facebook'
		when source_or_campaign like '%Snapchat%' then 'Snapchat'
		when source_or_campaign like '%Discovery%' then 'Discovery'
		when source_or_campaign like '%Performance_Max%' then 'Performance Max'
		when source_or_campaign like '%Youtube%' or source_or_campaign like '%YouTube%' then 'Youtube'
		when source_or_campaign like '%GDN%' or source_or_campaign like '%Display%' then 'GDN'
		when source_or_campaign like '%Search%' or source_or_campaign like '%search%' then 'Search Ads'
		else null 
	end as source_or_campaign,
	lead_status
from "LasVegas".tuned_lead_table;


drop table if exists "LasVegas".lead_count;
create table "LasVegas".lead_count as 
select
	extract(year from created_time) as Year,
	extract(month from created_time) as Month ,
	source_or_campaign ,
	count(id) as lead_count
from
	"LasVegas".lead_refined_campaign
group by
	1,
	2,
	3
order by
	1 desc,
	2 asc;

-- Junk Leads --
drop table if exists "LasVegas".junk_lead_count;
create table "LasVegas".junk_lead_count as 
select
	extract(year from created_time) as Year,
	extract(month from created_time) as Month ,
	source_or_campaign ,
	count(id) as junk_lead_count
from
	"LasVegas".lead_refined_campaign
where
	lead_status='Junk Lead'
group by
	1,
	2,
	3
order by
	1 desc,
	2 asc;

-- Duplicated Leads --
drop table if exists "LasVegas".duplicated_lead_count;
create table "LasVegas".duplicated_lead_count as 
select
	extract(year from created_time) as Year,
	extract(month from created_time) as Month ,
	source_or_campaign ,
	count(id) as duplicated_lead_count
from
	"LasVegas".lead_refined_campaign
where
	lead_status='Duplicated'
group by
	1,
	2,
	3
order by
	1 desc,
	2 asc;
------------------------------------------------


-- Deal & Customer Count -------
drop table if exists "LasVegas".tuned_deal_table;
create table "LasVegas".tuned_deal_table as
select
	d.id,
	d.created_time,
	COALESCE(l.lead_source, l.custom_campaign) AS source_or_campaign,
	stage
from
	rewaa_zoho_crm.deal d
LEFT JOIN 
	rewaa_zoho_crm.lead l ON l.converted_detail_deal::text = d.id::text;


drop table if exists "LasVegas".deal_refined_campaign;
create table "LasVegas".deal_refined_campaign as
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
from "LasVegas".tuned_deal_table;

--Deal Count--
drop table if exists "LasVegas".deal_count;
create table "LasVegas".deal_count as 
select
	extract(year from created_time) as Year,
	extract(month from created_time) as Month ,
	source_or_campaign ,
	count(id) as deal_count
from
	"LasVegas".deal_refined_campaign
group by
	1,
	2,
	3
order by
	1 desc,
	2 asc;

-- Customers --
drop table if exists "LasVegas".customer_count;
create table "LasVegas".customer_count as 
select
	extract(year from created_time) as Year,
	extract(month from created_time) as Month ,
	source_or_campaign ,
	count(id) as customer_count
from
	"LasVegas".deal_refined_campaign
where
	stage='Contract Signed-توقيع العقد'
group by
	1,
	2,
	3
order by
	1 desc,
	2 asc;


drop table if exists "LasVegas".crm_counts;
create table "LasVegas".crm_counts as 
select l.*,j.junk_lead_count,d.duplicated_lead_count,dc.deal_count,c.customer_count from "LasVegas".junk_lead_count j
left join "LasVegas".lead_count l 
on j.year=l.year and j.month=l.month and j.source_or_campaign=l.source_or_campaign
left join "LasVegas".duplicated_lead_count d
on d.year=l.year and d.month=l.month and d.source_or_campaign=l.source_or_campaign
left join "LasVegas".deal_count dc
on dc.year=l.year and dc.month=l.month and dc.source_or_campaign=l.source_or_campaign
left join "LasVegas".customer_count c
on c.year=l.year and c.month=l.month and c.source_or_campaign=l.source_or_campaign;


