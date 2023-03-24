CREATE OR REPLACE PROCEDURE ecomm.upload_gender()
 LANGUAGE plpgsql
AS $procedure$

begin

with product_table as(
select
	product_id,
	product_name,
	product_details
from
	ecomm.product
),
gender_table as(
select
	product_id,
	product_name ,
	product_details ,
	case
		when product_name like '%婴儿%'
		or product_name like '%儿童%'
		or product_name like '%幼儿%'
		or product_details::text like '%儿童%'
		or product_details::text like '%婴儿%'
		or product_details::text like '%幼儿%' then 'Baby'
		when product_name like '%男女%'
		or product_details::text like '%男女%'
		or product_details::text like '%情侣%'
		or product_details::text like '%中性%'
		or product_details::text like '%通用%' then 'Neutral'
		when product_name like '%女%'
		or product_details::text like '%女%' then 'Female'
		when product_name like '%男%'
		or product_details::text like '%男%' then 'Male'
		else 'Unknown'
	end as gender
from
	product_table
) 
update ecomm.product 
set gender = g.gender
from gender_table g
where ecomm.product.product_id=g.product_id;

end; $procedure$
;