-- back up data is in table explore.sku_stats_since_july_2021 from 2021-07-01 till today
-- delete data from 2021-07-01 to 2022-08-19 as discussed on the call. Change the date parameters accordingly
delete from ecomm.sku_stats 
where extraction_date >= '2021-07-01' and extraction_date<='2022-08-19';

-- insert into sku_stats prod table from the fixed data table in explore for the deleted time period
insert
	into
	ecomm.sku_stats
(product_id,
	sku_id,
	discounted_price,
	price,
	festival_price,
	inventory,
	extraction_date,
	create_date,
	update_date,
	created_at,
	deposit_price,
	delta_price,
	delta_disprice,
	promotion_price,
	adjusted_discounted_price,
	adjusted_ratio,
	adjust_price_rule_id,
	price_backup,
	discounted_price_backup)
select
	product_id,
	sku_id,
	discounted_price,
	price,
	festival_price,
	inventory,
	extraction_date,
	create_date,
	update_date,
	created_at,
	deposit_price,
	delta_price,
	delta_disprice,
	promotion_price,
	adjusted_discounted_price,
	adjusted_ratio,
	adjust_price_rule_id,
	price_backup,
	discounted_price_backup
from
	explore.sku_stats_since_july_2021_fixed
where 
extraction_date >= '2021-07-01' and extraction_date<='2022-08-19';


-- update ecomm.product_stats now 


TRUNCATE TABLE ecomm_staging.product_price_staging;
insert into ecomm_staging.product_price_staging
select
	product_id,
	extraction_date,
	avg(price) as avg_price,
	avg(discounted_price) as avg_discount,
	avg(promotion_price) as avg_promotion,
	avg(adjusted_discounted_price) as avg_adjusted,
	sum(adjusted_discounted_price * adjusted_ratio) as weighted_adjusted
from
	ecomm.sku_stats
where
	extraction_date >= '2021-07-01' and extraction_date<='2022-08-19'
group by
	product_id,
	extraction_date;

update
	ecomm.product_stats as d
set
	avg_price = t.avg_price,
	avg_discounted_price = t.avg_discount,
	avg_promotion_price = t.avg_promotion,
	avg_adjusted_discounted_price = t.avg_adjusted,
	weighted_adjusted_discounted_price = t.weighted_adjusted
from
	ecomm_staging.product_price_staging as t
where
	d.product_id = t.product_id
	and d.extraction_date = t.extraction_date;