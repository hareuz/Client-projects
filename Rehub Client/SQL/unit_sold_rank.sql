CREATE OR REPLACE PROCEDURE summary.units_sold_rank()
 LANGUAGE plpgsql
AS $procedure$

begin

drop table if exists temp1;
create temp table temp1 as 
with table1 as (
select
	d.store_id,
	store_type,
	date,
	monthly_unait_sold,
	quarter
from
	(
	select
		store_id,
		monthly_unit_sold,
		date,
		concat(DATE_PART('year', date), '-', DATE_PART('quarter', date)) as quarter
	from
		(
		select
			store_id,
			sum(increment_sale) as monthly_unit_sold,
			to_date(concat(DATE_PART('year', extraction_date), '-', DATE_PART('month', extraction_date), '-01'), 'YYYY-MM-DD') as date
		from
			ecomm.product_stats
		join ecomm.product on
			product_stats.product_id = product.product_id
		group by
			store_id,
			date) as t) as d
join ecomm.store on
	d.store_id = store.store_id)
,
table2 as(
select
	channel_coverage.brand_id,
	channel_coverage.brand_name as brand,
	uid
from
	taxonomy.channel_coverage
join taxonomy.brand on
	channel_coverage.brand_id = brand.brand_id
where
	source = 'Tmall'
	and crawled = 'Y'
	and taxonomy.channel_coverage.decision = 'Include')
,
table3 as(
select
	table1.*,
	table2.*
from
	table1
left join table2 on
	table1.store_id = table2.uid)
select * from table3;

drop table if exists test_ranking;
create temp table test_ranking as 
with table01 as (
select
	brand_id,
	brand,
	date,
	quarter,
	'all' as store_type,
	cast(brand_id as INT) as store_id,
	monthly_unit_sold
from
	 temp1 ),
table1 as(

select
	brand_id,
	brand,
	date,
	quarter,
	store_type,
	store_id,
	sum(monthly_unit_sold) monthly_unit_sold
from
table01
group by
	brand,
	date,
	quarter,
	store_type,
	brand_id,
	store_id)
,
table2 as (
select
	*
from
	 temp1)
,
table3 as(
select
	table2.brand_id,
	table2.brand,
	table2.date,
	table2.quarter,
	table2.store_type,
	cast(table2.store_id as int) store_id,
	table2.monthly_unit_sold,
	uid
from
	table2
union 
select
	table1.brand_id,
	table1.brand,
	table1.date,
	table1.quarter,
	table1.store_type,
	cast(table1.store_id as int) store_id,
	table1.monthly_unit_sold,
	null as uid
from
	table1
)
,drop_null as (select store_id, store_type,	date,	monthly_unit_sold,	quarter,	brand_id,	brand,	uid from table3
where 
uid is not null
or brand_id is not null
or brand is not null
)
,
table4 as (
select
	store_id,
	store_type,
	date,
	monthly_unit_sold,
	quarter,
	brand_id,
	brand,
	uid,
	dense_rank() over (partition by store_type, date order by monthly_unit_sold desc) monthly_rank
from
	drop_null)	
,
table5 as (
select
	store_id,
	store_type,
	quarter,
	sum(monthly_unit_sold) quarterly_unit_sold
from
	drop_null
group by
	1,
	2,
	3)	
,
table6 as (
select
	table4.* ,
	table5.quarterly_unit_sold
from
	table4
left join table5 
on
	table4.store_id = table5.store_id
	and table4.store_type = table5.store_type
	and table4.quarter = table5.quarter)
, table7 as ( select *, 
dense_rank() over (partition by store_type, quarter order by quarterly_unit_sold desc) quarterly_rank
from table6)
select
	*,
	lag(monthly_rank) over(partition by store_id, store_type order by date) pre_monthly_rank,
	row_number() over(partition by store_id, store_type) row_number
from
	table7
;
drop table if exists first_row_fixes;
create temp table first_row_fixes as
select
	a.store_id,
	a.store_type,
	a.date,
	a.monthly_unit_sold,
	a.quarter,
	a.brand_id,
	a.brand,
	a.row_number,
	a.monthly_rank,
	a.quarterly_rank,
	b.pre_monthly_rank
from
	 test_ranking a
inner join(
	select
		*
	from
		 test_ranking
	where
		row_number = 2 ) b on
	a.store_id = b.store_id
	and a.store_type = b.store_type
where
	a.row_number = 1;

update
	 test_ranking a
set
	pre_monthly_rank = b.pre_monthly_rank
from
 first_row_fixes b
where 
	 a.row_number = b.row_number
	 and a.store_id = b.store_id;

drop table if exists monthly_ranking;
create temp table monthly_ranking as 
select
	*,
	pre_monthly_rank-monthly_rank as monthly_rank_change from  test_ranking
	order by row_number;


drop table if exists drop_dups;
create temp table drop_dups as 
with drop_dups as (
select store_id, store_type, date, monthly_unit_sold, quarter , brand_id, brand,  monthly_rank, quarterly_unit_sold, quarterly_rank, pre_monthly_rank, monthly_rank_change,
ROW_NUMBER() over(partition by store_id,store_type,date order by store_id,store_type,date, quarterly_rank) row_num
from  monthly_ranking
), clean_data as (
select *
from drop_dups
where row_num=1
order by date asc) 
select *, 
row_number() over(partition by store_id,store_type ) row_number,
lag(quarterly_rank) over(partition by store_id, store_type order by quarter) pre_quarterly_rank 
from clean_data;

drop table if exists dups_first_row_fixes;
create temp table dups_first_row_fixes as
select
	a.store_id,
	a.store_type,
	a.date,
	a.monthly_unit_sold,
	a.quarter,
	a.brand_id,
	a.monthly_rank_change,
	a.brand,
	a.row_number,
	a.monthly_rank,
	a.quarterly_rank,
	b.pre_quarterly_rank
from
	 drop_dups a
inner join(
	select
		*
	from
		 drop_dups
	where
		row_number = 2 ) b on
	a.brand_id = b.brand_id 
	and a.store_type = b.store_type
where
	a.row_number = 1;

update
	 drop_dups a
set
	pre_quarterly_rank = b.pre_quarterly_rank
from
 dups_first_row_fixes b
where 
	 a.row_number = b.row_number
	 and a.store_id = b.store_id;


drop table if exists df_return;
create temp table df_return as 
select store_id::text, store_type, brand_id, replace(brand, '''', '''''') as brand, date, 
monthly_unit_sold, monthly_rank,monthly_rank_change, quarter, 
quarterly_unit_sold, quarterly_rank, 
pre_quarterly_rank-quarterly_rank as quarterly_rank_change
from  drop_dups dd 
order by store_id, date;

truncate table summary.units_sold_rank;

insert into summary.units_sold_rank(store_id, store_type, brand_id, brand, date, monthly_unit_sold, monthly_rank,monthly_rank_change, quarter, 
quarterly_unit_sold, quarterly_rank, quarterly_rank_change  )
select * from  df_return
on conflict (brand_id,date,store_type) DO UPDATE set 
store_id=EXCLUDED.store_id, monthly_unit_sold=EXCLUDED.monthly_unit_sold,
quarter=EXCLUDED.quarter, brand=EXCLUDED.brand, monthly_rank=EXCLUDED.monthly_rank, 
quarterly_unit_sold=EXCLUDED.quarterly_unit_sold,quarterly_rank=EXCLUDED.quarterly_rank;

end;

$procedure$
;
