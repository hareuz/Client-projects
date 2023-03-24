create or replace
procedure summary.category_summary_and_product_rank()
 language plpgsql
as $procedure$

begin

create temp table initial_summary as
with join_table as(
select
	tmp2.brand,
	tmp2.brand_id,
	tmp2.store_id,
	tmp2.store_type,
	tmp2.rh_category,
	tmp2.rh_subcategory,
	tmp1.product_id,
	tmp1.extraction_date,
	tmp1.avg_disprice,
	tmp1.increment_sale
from
	(
	select
		tmp.product_id,
		tmp.extraction_date,
		tmp.avg_disprice,
		product_stats.increment_sale
	from
		(
		select
			product_id,
			extraction_date,
			sum(adjusted_discounted_price * adjusted_ratio) as avg_disprice
		from
			ecomm.sku_stats
		group by
			product_id,
			extraction_date) as tmp
	join ecomm.product_stats on
		tmp.product_id = product_stats.product_id
		and tmp.extraction_date = product_stats.extraction_date) as tmp1
join(
	select
		product_id,
		rh_category,
		rh_subcategory,
		brand,
		brand_id,
		store.store_id,
		store_type
	from
		ecomm.product
	join ecomm.store on
		product.store_id = store.store_id
	where
		rh_category is not null) as tmp2 on
	tmp1.product_id = tmp2.product_id)
 select
	date_trunc('month', join_table.extraction_date)::date as date,
	replace(join_table.brand, '''', '''''') as brand,
	brand.brand_name,
	join_table.brand_id,
	join_table.product_id,
	join_table.rh_category,
	join_table.rh_subcategory,
	join_table.extraction_date,
	join_table.avg_disprice,
	join_table.increment_sale,
	join_table.store_id,
	join_table.store_type,
	join_table.avg_disprice * join_table.increment_sale as revenue
from
	join_table
left join taxonomy.brand brand on
	brand.brand_id = join_table.brand_id
where
	brand.include = 'Y';
-- category summary part
truncate
	table summary.category_summary_1;

insert
	into
	summary.category_summary_1
(brand,
	brand_id,
	category,
	sub_category,
	"date",
	no_of_listings,
	units_sold,
	revenue,
	store_id,
	store_type)

select
	brand,
	brand_id,
	rh_category as category,
	rh_subcategory as sub_category,
	date,
	count( distinct product_id) as no_of_listings,
	sum(increment_sale) as units_sold,
	sum(revenue) as revenue,
	store_id,
	store_type
from
	initial_summary
group by
	1,
	2,
	3,
	4,
	5,
	9,
	10;

truncate
	table summary.category_summary_2;

insert
	into
	summary.category_summary_2
select
	*
from
	summary.category_summary_1;

update
	summary.category_summary_1 as t set
	brand = d.brand_name
from
	taxonomy.brand as d
where
	t.brand_id = d.brand_id;

update
	summary.category_summary_2 as t set
	brand = d.brand_name
from
	taxonomy.brand as d
where
	t.brand_id = d.brand_id;
-- -------------------------
-- hot product rank

drop table if exists summary_hot_product;

create temp table summary_hot_product as 
select
	product_id,
	brand,
	brand_id,
	date,
	sum(revenue) as revenue,
	sum(increment_sale) as units
from
	initial_summary
where
	extraction_date <date_trunc('month', current_date)
group by
	1,
	2,
	3,
	4;

create temp table tmp_hot_product_rank_rev as
select
	*,
	dense_rank() over (partition by date
order by
	revenue desc) as rank,
	'all' as category,
	'revenue' as type
from
	summary_hot_product;

truncate
	table summary.hot_product_rank;

insert
	into
	summary.hot_product_rank
select
	*
from
	tmp_hot_product_rank_rev
where
	rank <= 50;

create temp table tmp_hot_product_rank_units as
select
	*,
	dense_rank() over (partition by date
order by
	units desc) as rank,
	'all' as category,
	'units' as type
from
	summary_hot_product;

insert
	into
	summary.hot_product_rank
select
	*
from
	tmp_hot_product_rank_units
where
	rank <= 50;

drop table if exists summary_hot_product_cat;

create temp table summary_hot_product_cat as 
select
	product_id,
	brand,
	brand_id,
	rh_category as category,
	date,
	sum(revenue) as revenue,
	sum(increment_sale) as units
from
	initial_summary
where
	extraction_date <date_trunc('month', current_date)
group by
	1,
	2,
	3,
	4,
	5;

create temp table tmp_hot_product_rank_rev_cat as
select
	*,
	dense_rank() over (partition by category,
	date
order by
	revenue desc) as rank,
	'revenue' as type
from
	summary_hot_product_cat;

insert
	into
	summary.hot_product_rank
select
	product_id,
	brand,
	brand_id,
	date,
	revenue,
	units,
	rank,
	category,
	type
from
	tmp_hot_product_rank_rev_cat
where
	rank <= 50;

create temp table tmp_hot_product_rank_units_cat as
select
	*,
	dense_rank() over (partition by category,
	date
order by
	units desc) as rank,
	'units' as type
from
	summary_hot_product_cat;

insert
	into
	summary.hot_product_rank
select
	product_id,
	brand,
	brand_id,
	date,
	revenue,
	units,
	rank,
	category,
	type
from
	tmp_hot_product_rank_units_cat
where
	rank <= 50;
end;

$procedure$
;
