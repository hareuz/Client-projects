create or replace
function dwh.sales_per_owner_per_product ()
 returns void
 language plpgsql
as $function$
	begin
-- deal with the rows where we have discount on invoice level
drop table if exists invoice_to_product_discount;
create temp table invoice_to_product_discount as
select
	date(i.invoice_date)as invoice_date,
	case
		when custom_canceled_invoice is null then false
		else custom_canceled_invoice
	end as custom_canceled_invoice,
	p.product_category,
	i.id,
	i.owner_name as owner,
	p.product_name as Product,  
	ip.list_price as Product_Price,
	i.grand_total,
	ip.quantity as Quantity,
	i.tax as invoice_tax,
	ip.tax as product_tax,
	(cast(i.discount as float)/ b.sum)* ip.quantity as custom_product_discount,
	ip.total_after_discount as Product_Total_After_Discount
from
	rewaa_zoho_crm.invoice i
inner join rewaa_zoho_crm.invoice_product ip 
on
	i.id = ip.invoice_id
inner join rewaa_zoho_crm.product p
on
	p.id = ip.product_id
inner join 
(
	select
		i.id,
		sum(ip.quantity)
	from
		rewaa_zoho_crm.invoice i
	inner join rewaa_zoho_crm.invoice_product ip 
on
		i.id = ip.invoice_id
	where
		cast(i.discount as float) <> 0
			and ip.discount = 0
		group by
			1
		order by
			i.id desc) b
on
	i.id = b.id;

drop table if exists all_invoices_disc_fixed;

create temp table all_invoices_disc_fixed as
select
	*
from
	invoice_to_product_discount
union
select
	date(i.invoice_date) as invoice_date,
	case
		when custom_canceled_invoice is null then false
		else custom_canceled_invoice
	end as custom_canceled_invoice,
	p.product_category,
	i.id,
	i.owner_name as owner,
	p.product_name as Product,  
	ip.list_price as Product_Price,
	i.grand_total,
	ip.quantity as Quantity,
	i.tax as invoice_tax,
	ip.tax as product_tax,
	ip.discount as custom_product_discount,
	ip.total_after_discount as Product_Total_After_Discount
from
	rewaa_zoho_crm.invoice i
inner join rewaa_zoho_crm.invoice_product ip 
on
	i.id = ip.invoice_id
inner join rewaa_zoho_crm.product p
on
	p.id = ip.product_id
where
	cast(i.discount as float) = 0;
-- deal with the rows where we have tax on invoice level
drop table if exists all_invoices_disc_tax_fixed;

create temp table all_invoices_disc_tax_fixed as
select
	*
from
	invoice_to_product_tax
union
select
	date(i.invoice_date)as invoice_date,
	case
		when custom_canceled_invoice is null then false
		else custom_canceled_invoice
	end as custom_canceled_invoice,
	i.product_category,
	i.id,
	i.Owner,
	i.product,  
	i.Product_Price,
	i.quantity,
	i.custom_product_discount,
	i.invoice_tax,
	i.product_tax as custom_product_tax,
	--i.product_tax as product_tax,
	i.grand_total
from
	all_invoices_disc_fixed i
where  
	i.invoice_tax = 0 ;

drop table if exists invoice_to_product_tax;

create temp table invoice_to_product_tax as
select
	date(i.invoice_date) as invoice_date,
	case
		when custom_canceled_invoice is null then false
		else custom_canceled_invoice
	end as custom_canceled_invoice,
	i.product_category,
	i.id,
	i.Owner,
	i.product,  
	i.Product_Price,
	i.quantity,
	i.custom_product_discount,
	i.invoice_tax,
	(i.invoice_tax / b.sum)* i.quantity as custom_product_tax,
	--i.product_tax as product_tax,
	i.grand_total
from
	all_invoices_disc_fixed i
inner join 
(
	select
		id,
		sum(quantity)
	from
		all_invoices_disc_fixed
	where
		invoice_tax <> 0
		-- only for condition when discount is on product level.
	group by
		1
	order by
		id desc) b
on
	i.id = b.id
order by
	i.id desc;

drop table if exists dwh.sales_per_owner_per_product;

create table dwh.sales_per_owner_per_product as
select
	owner,
	invoice_date,
	product_category,
	product,
	product_price,
	quantity,
	custom_product_discount as Discount,
	(product_price * quantity)-custom_product_discount as without_tax,
	(product_price * quantity)-custom_product_discount + custom_product_tax as with_tax,
	grand_total,
	custom_canceled_invoice,
	current_timestamp as Last_Run
from
	all_invoices_disc_tax_fixed;
--select
--	a.owner,
--	a.product,
--	avg(a.product_price) as Product_List_Price,
--	sum(a.quantity) as Total_Quantity,
--	sum(a.discount) as Total_Discount,
--	sum(a.without_tax) as Total_Without_Tax,
--	sum(a.with_tax) as Total_With_Tax
--	
--from "LasVegas".all_invoices_disc_tax_fixed;
end;

$function$
;
