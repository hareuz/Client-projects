create or replace procedure summary.upload_increment_to_productstats()
language plpgsql
as $$

begin 
 
--### main query starting 
drop table if exists data_table;  

create temp table data_table as

with copy_product_stats as(
select
	*
from
	(
	select
		ps.*,
		row_number() over(partition by ps.product_id
	order by
		ps.extraction_date desc),
		p.rh_subcategory
	from
		ecomm.product_stats ps
	join ecomm.product p on
		p.product_id = ps.product_id
	where
		ps.product_id in (
		select
			product_id
		from
			ecomm.product_stats
		where
			extraction_date =(
			select
				distinct(extraction_date)
			from
				ecomm.product_stats
			order by
				extraction_date desc
			limit 1 offset 0) and increment_sale is null )
		and extraction_date != '20220501'
	order by
		ps.product_id desc,
		ps.extraction_date desc) a
where
	row_number = 1
	or row_number = 2
),

table2 as(
select product_id,extraction_date,row_number,favorites,total_reviews_review_api,total_sales,rh_subcategory,
lag(total_sales) over(partition by product_id order by row_number ) as pre_value_sales,
lag(favorites) over(partition by product_id order by row_number ) as pre_value_favorites,
lag(total_reviews_review_api) over(partition by product_id order by row_number) as pre_value_review
from copy_product_stats 
)
select *,
case when pre_value_sales is not null then total_sales - pre_value_sales 
else total_sales - 0 end
as increment_sale,
0 as adjust_sales_rule_id,
'' as min_review
from table2
order by product_id;

-- line number 820 to 824

update data_table
set increment_sale = 0 ,
	adjust_sales_rule_id = 10
where product_id in (select product_id  from data_table group by 1,rh_subcategory having count(row_number)<2 and  rh_subcategory!='Coupon');
	
--line  number 835 to 839 
update data_table
set increment_sale = 0,
	adjust_sales_rule_id = 4
where row_number =1 and  rh_subcategory='Coupon' ;

--#increment_favorites #after line 842
update data_table
set pre_value_favorites = favorites,
pre_value_review = total_reviews_review_api
where row_number =1;


drop table if exists data_table2;

create temp table data_table2 as
select *,
favorites - pre_value_favorites as increment_favorites ,
total_reviews_review_api - pre_value_review as increment_reviews
from data_table ;

--#increment_favorites  #line 846
update data_table2
set increment_favorites =  0
where increment_favorites is null;


--#increment_reviews #line 854
update data_table2
set increment_reviews = 0
where increment_reviews is null;

update data_table2
set increment_reviews = increment_reviews::bigint
where increment_reviews is not null;

--##creating table where row_number = 2
drop table if exists data_table3;

create temp table data_table3 as
select * from data_table2
where row_number = 2;

--#creating table where product_id count =1
drop table if exists data_table4;

create temp table data_table4 as
select * from data_table2
where product_id in (select product_id  from data_table group by 1 having count(row_number)<2);

--##updating columns with updated data

update summary.product_stats p
set increment_sale   = dt.increment_sale::bigint,
increment_reviews    = dt.increment_reviews,
increment_favorites  = dt.increment_favorites,
adjust_sales_rule_id = dt.adjust_sales_rule_id,
update_date          = current_date
from data_table3 dt
where p.product_id=dt.product_id and p.extraction_date=dt.extraction_date and p.increment_sale is null;

--##updating columns with updated data where count =1

update summary.product_stats p
set increment_sale   = dt.increment_sale::bigint,
increment_reviews    = dt.increment_reviews,
increment_favorites  = dt.increment_favorites,
adjust_sales_rule_id = dt.adjust_sales_rule_id,
update_date          = current_date
from data_table4 dt
where p.product_id=dt.product_id and p.extraction_date=dt.extraction_date and p.increment_sale is null;


--###end of query

end; $$