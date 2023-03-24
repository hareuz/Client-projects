create or replace
procedure summary.revenue_rank()
 language plpgsql
as $procedure$

begin


drop table if exists tem_revenue_rank;

create temp table tem_revenue_rank as
with query_1 as(
SELECT s.brand,
       s.store_id,
       ps.extraction_date,
       sum(ps.weighted_adjusted_discounted_price*ps.increment_sale)  as revenue_dis,
       sum(ps.avg_price*ps.increment_sale)  as revenue_ori     
from ecomm.product_stats ps
	join ecomm.product p on ps.product_id = p.product_id
	join ecomm.store s on p.store_id = s.store_id
group by s.brand,
	s.store_id,
	ps.extraction_date
),
query_2 as(
select
	brand.brand_id,
	brand.brand_name,
	brand.include,
	uid,
	type
from
	taxonomy.channel_coverage
join taxonomy.brand 
    on
	channel_coverage.brand_id = brand.brand_id
where
	source = 'Tmall'		
),
join_table as(
select
	*
from
	query_1
left join query_2
on
	query_1.store_id = query_2.uid
)
select
	*
from
	join_table;

drop table if exists type_wise;

create table type_wise as
with table1 as(
select
	date_trunc('month', extraction_date)::date as date,
	brand_name,
	brand_id,
	type as store_type,
	sum(revenue_ori) as revenue_ori,
	sum(revenue_dis) as revenue_dis
from
	tem_revenue_rank
where
	include = 'Y'
group by
	2,
	4,
	3,
	1
),
table2 as (
select
	*,
	dense_rank() over(partition by date,
	store_type
order by
	revenue_ori desc) as rank_ori,
	dense_rank() over(partition by date,
	store_type
order by
	revenue_dis desc) as rank_dis
from
	table1
order by
	date asc)
select
	*,
	row_number() over(partition by brand_id,
	store_type) row_number,
	lag(revenue_ori) over( partition by brand_id ,
	store_type
order by
	date) pre_revenue_ori,
	lag(revenue_dis) over( partition by brand_id ,
	store_type
order by
	date) pre_revenue_dis,
	lag(rank_ori) over( partition by brand_id ,
	store_type
order by
	date) pre_rank_ori,
	lag(rank_ori) over( partition by brand_id ,
	store_type
order by
	date) pre_rank_dis
from
	table2;

drop table if exists first_row_fixes;

create temp table first_row_fixes as
select
	a.date,
	a.brand_name,
	a.brand_id,
	a.store_type,
	a.revenue_ori,
	a.revenue_dis,
	a.rank_ori,
	a.rank_dis,
	a.row_number,
	b.pre_revenue_ori,
	b.pre_revenue_dis,
	b.pre_rank_ori,
	b.pre_rank_dis
from
	type_wise a
inner join(
	select
		*
	from
		type_wise
	where
		row_number = 2 ) b on
	a.brand_id = b.brand_id
	and a.store_type = b.store_type
where
	a.row_number = 1;

update
	type_wise a
set
	pre_revenue_ori = b.pre_revenue_ori,
	pre_revenue_dis = b.pre_revenue_dis,
	pre_rank_ori = b.pre_rank_ori,
	pre_rank_dis = b.pre_rank_dis
from
	first_row_fixes b
where
	a."row_number" = b."row_number"
	and a.brand_id = b.brand_id;

drop table if exists all_brand;

create table all_brand as
with change_column_value as (
select
	extraction_date,
	brand_name,
	brand_id,
	'all_brand' as store_type,
	revenue_ori,
	revenue_dis
from
	tem_revenue_rank
where
	include = 'Y'
),
table1_all_brand as(
select
	date_trunc('month', extraction_date)::date as date,
	brand_name,
	brand_id,
	store_type,
	sum(revenue_ori) as revenue_ori,
	sum(revenue_dis) as revenue_dis
from
	change_column_value
group by
	2,
	4,
	3,
	1
),
table2_all_brand as (
select
	*,
	dense_rank() over(partition by date,
	store_type
order by
	revenue_ori desc) as rank_ori,
	dense_rank() over(partition by date,
	store_type
order by
	revenue_dis desc) as rank_dis
from
	table1_all_brand
order by
	date asc)
select
	*,
	row_number() over(partition by brand_id,
	store_type) row_number,
	lag(revenue_ori) over(partition by brand_id,
	store_type
order by
	date) pre_revenue_ori,
	lag(revenue_dis) over(partition by brand_id,
	store_type
order by
	date) pre_revenue_dis,
	lag(rank_ori) over(partition by brand_id,
	store_type
order by
	date) pre_rank_ori,
	lag(rank_ori) over(partition by brand_id,
	store_type
order by
	date) pre_rank_dis
from
	table2_all_brand;

drop table if exists first_row_fixes;

create temp table first_row_fixes as
select
	a.date,
	a.brand_name,
	a.brand_id,
	a.store_type,
	a.revenue_ori,
	a.revenue_dis,
	a.rank_ori,
	a.rank_dis,
	a.row_number,
	b.pre_revenue_ori,
	b.pre_revenue_dis,
	b.pre_rank_ori,
	b.pre_rank_dis
from
	all_brand a
inner join(
	select
		*
	from
		all_brand
	where
		row_number = 2 ) b on
	a.brand_id = b.brand_id
	and a.store_type = b.store_type
where
	a.row_number = 1;

update
	all_brand a
set
	pre_revenue_ori = b.pre_revenue_ori,
	pre_revenue_dis = b.pre_revenue_dis,
	pre_rank_ori = b.pre_rank_ori,
	pre_rank_dis = b.pre_rank_dis
from
	first_row_fixes b
where
	a."row_number" = b."row_number"
	and a.brand_id = b.brand_id;

drop table if exists type_wise_rank;

create table type_wise_rank as 
with full_table as (
select
	date,
	brand_name as brand,
	store_type,
	brand_id,
	revenue_ori,
	revenue_dis,
	rank_ori,
	rank_dis,
	(revenue_ori-pre_revenue_ori)/ nullif(revenue_ori, 0) as revenue_ori_growth,
	(revenue_dis-pre_revenue_dis)/ nullif(revenue_dis, 0) as revenue_dis_growth,
	pre_rank_ori-rank_ori as rank_ori_change,
	pre_rank_dis-rank_dis as rank_dis_change
from
	type_wise
order by
	row_number)
,
original_table as (
select
	date,
	brand,
	store_type,
	brand_id ,
	revenue_ori as revenue,
	rank_ori as rank,
	round(cast(revenue_ori_growth as numeric), 2) revenue_growth,
	rank_ori_change rank_change,
	'Original' as type
from
	full_table)
,
discount_table as(
select
	date,
	brand,
	store_type,
	brand_id ,
	round(cast(revenue_dis as numeric), 2) as revenue,
	rank_dis as rank,
	revenue_dis_growth revenue_growth,
	rank_dis_change rank_change,
	'Discount' as type
from
	full_table
)
select
	*
from
	original_table
union
select
	*
from
	discount_table;

drop table if exists all_brand_rank;

create table all_brand_rank as 
with full_table as (
select
	date,
	brand_name as brand,
	store_type,
	brand_id,
	revenue_ori,
	revenue_dis,
	rank_ori,
	rank_dis,
	(revenue_ori-pre_revenue_ori)/ nullif(revenue_ori, 0) as revenue_ori_growth,
	(revenue_dis-pre_revenue_dis)/ nullif(revenue_dis, 0) as revenue_dis_growth,
	pre_rank_ori-rank_ori as rank_ori_change,
	pre_rank_dis-rank_dis as rank_dis_change
from
	all_brand
order by
	row_number)
,
original_table as (
select
	date,
	brand,
	store_type,
	brand_id ,
	revenue_ori as revenue,
	rank_ori as rank,
	round(cast(revenue_ori_growth as numeric), 2) revenue_growth,
	rank_ori_change rank_change,
	'Original' as type
from
	full_table)
,
discount_table as(
select
	date,
	brand,
	store_type,
	brand_id ,
	round(cast(revenue_dis as numeric), 2) as revenue,
	rank_dis as rank,
	revenue_dis_growth revenue_growth,
	rank_dis_change rank_change,
	'Discount' as type
from
	full_table
)
select
	*
from
	original_table
union
select
	*
from
	discount_table;

truncate
	table summary.revenue_rank;

insert
	into
	summary.revenue_rank
select
	brand,
	store_type,
	brand_id,
	date,
	revenue,
	rank,
	type,
	rank_change,
	revenue_growth
from
	type_wise_rank;

insert
	into
	summary.revenue_rank
select
	brand,
	store_type,
	brand_id,
	date,
	revenue,
	rank,
	type,
	rank_change,
	revenue_growth
from
	all_brand_rank;

drop table if exists df_normal;

create table df_normal as
with df as(
select
	brand,
	store_type,
	brand_id,
	date,
	revenue,
	rank,
	type,
	rank_change,
	revenue_growth,
	extract('year'
from
	date)|| '-' || date_part('quarter', date) as quarter
from
	summary.revenue_rank)
,
table2 as(
select
	brand,
	brand_id,
	store_type,
	quarter,
	type,
	sum(revenue) as quarterly_revenue
from
	df
group by
	1,
	2,
	3,
	4,
	5
),
df_quarter as(
select
	*,
	dense_rank() over(partition by store_type,
	quarter,
	type
order by
	quarterly_revenue desc) as quarterly_rank
from
	table2
),
full_table as (
select
	df.*,
	dq.quarterly_revenue,
	dq.quarterly_rank
from
	df
left join df_quarter dq
on
	df.brand = dq.brand
	and df.brand_id = dq.brand_id
	and df.store_type = dq.store_type
	and df.quarter = dq.quarter
	and df.type = dq.type)
select
	*
from
	full_table;

drop table if exists df_drop_dups;

create table df_drop_dups as 
with drop_dups as(
select
	*,
	row_number() over(partition by brand_id,
	store_type,
	quarter,
	type
order by
	brand_id,
	store_type,
	quarter,
	type) row_num
from
	df_normal
),
clean_data as (
select
	*
from
	drop_dups
where
	row_num = 1
order by
	quarter asc)
select
	*,
	row_number() over(partition by brand_id,
	store_type,
	type) row_number,
	lag(quarterly_revenue) over(partition by brand_id,
	store_type,
	type
order by
	quarter) pre_quarterly_revenue,
	lag(quarterly_rank) over(partition by brand_id,
	store_type,
	type
order by
	quarter) pre_quarterly_rank
from
	clean_data;

drop table if exists first_row_fixes;

create table first_row_fixes as
select
	a.date,
	a.brand_id,
	a.type,
	a.store_type,
	a.quarter,
	a.row_number,
	b.pre_quarterly_revenue,
	b.pre_quarterly_rank
from
	df_drop_dups a
inner join(
	select
		*
	from
		df_drop_dups
	where
		row_number = 2 ) b on
	a.brand_id = b.brand_id
	and a.store_type = b.store_type
	and a.type = b.type
where
	a.row_number = 1;

update
	df_drop_dups a
set
	pre_quarterly_revenue = b.pre_quarterly_revenue,
	pre_quarterly_rank = b.pre_quarterly_rank
from
	first_row_fixes b
where
	a."row_number" = b."row_number"
	and a.brand_id = b.brand_id
	and a.quarter = b.quarter
	and a.type = b.type
	and a.store_type = b.store_type
	;

drop table if exists df_1;

create temp table df_1 as 
with df_quarterly_change as (
select
	*,
	(quarterly_revenue-pre_quarterly_revenue)/ nullif(pre_quarterly_revenue, 0) as quarterly_revenue_growth,
	pre_quarterly_rank-quarterly_rank as quarterly_rank_change
from
	df_drop_dups
order by
	row_number)
,
df_no_change as (
select
	*
from
	df_normal)
,
df_merge as (
select
	df_no.*,
	df_yes.quarterly_revenue_growth,
	df_yes.quarterly_rank_change
from
	df_no_change df_no
left join df_quarterly_change df_yes
on
	df_no.brand_id = df_yes.brand_id
	and df_no.store_type = df_yes.store_type
	and df_no.quarter = df_yes.quarter
	and df_no.type = df_yes.type
)
select
	replace(brand, '''', '''''') as brand,
	store_type,
	brand_id,
	date,
	revenue,
	rank,
	type,
	rank_change,
	revenue_growth,
	quarterly_revenue,
	quarterly_rank,
	quarterly_rank_change,
	quarterly_revenue_growth,
	quarter
from
	df_merge
order by
	quarter;

insert
	into
	summary.revenue_rank (brand,
	store_type,
	brand_id,
	date,
	revenue,
	rank,
	type,
	rank_change,
	revenue_growth,
	quarterly_revenue,
	quarterly_rank,
	quarterly_rank_change,
	quarterly_revenue_growth,
	quarter )
select
	brand,
	store_type,
	brand_id,
	date,
	revenue,
	rank,
	type,
	rank_change,
	revenue_growth,
	quarterly_revenue,
	quarterly_rank,
	quarterly_rank_change,
	quarterly_revenue_growth,
	quarter
from
	df_1
on
	conflict (brand_id,
	store_type,
	type,
	date) do
update
set
	quarter = EXCLUDED.quarter,
	quarterly_revenue = EXCLUDED.quarterly_revenue,
	quarterly_rank = EXCLUDED.quarterly_rank,
	quarterly_rank_change = EXCLUDED.quarterly_rank_change,
	quarterly_revenue_growth = EXCLUDED.quarterly_revenue_growth;
end;

$procedure$
;