--staging orders
create table order_staging(
	order_id character varying(300),
	customer_id character varying(300),
	order_status character varying(100),
	order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp,
	order_item_id float(25),
	product_id character varying(300),
	seller_id character varying(300),
	shipping_limit_date timestamp,
	price float(25),
	freight_value float(25),
	customer_unique_id character varying(300),
	customer_zip_code_prefix character varying(100),
	customer_city character varying(100),
	customer_state character varying(100),
	seller_zip_code_prefix character varying(100),
	seller_city character varying(100),
	seller_state character varying(100),
	payment_sequential float(25),
	payment_type character varying(100),
	payment_installments float(25),
	payment_value float(25));

--staging products
create table product_staging(
	product_id character varying(300),
	product_category_name character varying(100),
	product_name_lenght float(25),
	product_description_lenght float(25),
	product_photos_qty float(25),
	product_weight_g float(25),
	product_length_cm float(25),
	product_height_cm float(25),
	product_width_cm float(25),
	product_category_name_english character varying(100));


	
--staging marketing qualified leads
create table mql_staging(
	mql_id character varying(300),
	first_contact_date timestamp,
	landing_page_id character varying(300),
	origin character varying(100),
	seller_id character varying(300),
	sdr_id character varying(300),
	sr_id character varying(300),
	won_date timestamp,
	business_segment character varying(100),
	lead_type character varying(100),
	lead_behaviour_profile character varying(100),
	business_type character varying(100),
	declared_monthly_revenue float(25));
	
	
-- Importing datasets
COPY order_staging
FROM '/private/tmp/df_orderitems_customers_sellers_payments.csv'
DELIMITER ',' CSV HEADER;

-- product datasets
COPY product_staging
FROM '/private/tmp/df_productscategory.csv'
DELIMITER ',' CSV HEADER;

-- mql datasets
COPY mql_staging
FROM '/private/tmp/df_mqldeals.csv'
DELIMITER ',' CSV HEADER;


-- EXPLORING TABLE STRUCTURE
--order_staging
select * from order_staging limit 5;
select count(*) from order_staging;
select count(*) from order_staging where order_id is null;

-- product_staging
select * from product_staging limit 5;
select count(*) from product_staging;
select count(*) from product_staging where product_id is null;

--mql_staging
select * from mql_staging limit 5;
select count(*) from mql_staging;
select count(*) from mql_staging where mql_id is null;


--temp table creation
-- order and products
create table temp_order
as
(SELECT order_staging.order_id, product_staging.product_id,customer_id,
 order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,
 order_delivered_customer_date,order_estimated_delivery_date,order_item_id,seller_id,
 shipping_limit_date,price,freight_value,customer_unique_id,customer_zip_code_prefix,
 customer_city,customer_state,seller_zip_code_prefix,seller_city,seller_state,payment_sequential,
 payment_type,payment_installments,payment_value,product_staging.product_category_name,
 product_staging.product_name_lenght,product_staging.product_description_lenght,
 product_staging.product_photos_qty,product_staging.product_weight_g,product_staging.product_length_cm,
 product_staging.product_height_cm,product_staging.product_width_cm,product_staging.product_category_name_english 
 FROM order_staging
 JOIN product_staging
ON order_staging.product_id = product_staging.product_id);

select count(*) from temp_order;
select * from temp_order limit 5;


--mql and sellers
create table temp_mql
as
(SELECT mql_staging.mql_id,order_staging.seller_id,mql_staging.first_contact_date,mql_staging.origin,
 mql_staging.sdr_id,mql_staging.sr_id,mql_staging.won_date,mql_staging.business_segment,
 mql_staging.lead_type,mql_staging.lead_behaviour_profile,mql_staging.business_type,
 mql_staging.declared_monthly_revenue,seller_zip_code_prefix,seller_city,seller_state
 FROM order_staging
 JOIN mql_staging
ON mql_staging.seller_id = order_staging.seller_id);

select count(*) from temp_mql;
select * from temp_mql limit 5;


---Dimension table creation

--1.dim_order
create table dim_order(
	order_id character varying(300),
	order_item_id float(25),
	order_status character varying(100)
);

insert into dim_order (order_id,order_status)
select distinct order_id,order_status from order_staging;

ALTER TABLE dim_order
ADD CONSTRAINT order_id_pk PRIMARY KEY (order_id);


--data match
select count(distinct order_id) FROM order_staging;
select count(*) from dim_order;


--2. dim_time
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
	order_purchase_timestamp timestamp,
	order_purchase_year int,
	order_purchase_quarter int,
	order_purchase_month int,
	order_purchase_month_name character varying(100),
	order_purchase_day int,
	order_purchase_dow int,
	order_purchase_day_name character varying(100),
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp
	);

insert into dim_time (order_purchase_timestamp,order_approved_at,order_delivered_carrier_date
					  ,order_delivered_customer_date,order_estimated_delivery_date)
select distinct order_purchase_timestamp,order_approved_at,order_delivered_carrier_date
					  ,order_delivered_customer_date,order_estimated_delivery_date from order_staging;

update dim_time
set order_purchase_year = extract(YEAR from order_purchase_timestamp);

update dim_time
set order_purchase_quarter = extract(QUARTER from order_purchase_timestamp);

update dim_time
set order_purchase_month = extract(MONTH from order_purchase_timestamp);

update dim_time
set order_purchase_month_name = TO_CHAR(
    TO_DATE (order_purchase_month::text, 'MM'), 'Mon'
    );

update dim_time
set order_purchase_day = extract(DAY from order_purchase_timestamp);

update dim_time
set order_purchase_dow = extract(DOW from order_purchase_timestamp);

update dim_time
set order_purchase_day_name = to_char(order_purchase_timestamp, 'Dy');


--data check
select * from dim_time order by time_id;
select count(*) from dim_time;
select count(*) from dim_order;


--3.dim_payments
create table dim_payments(
	payment_id SERIAL PRIMARY KEY,
	payment_sequential float(25),
	payment_type character varying(100),
	payment_installments float(25),
	payment_value float(25)
);

insert into dim_payments (payment_sequential,payment_type,payment_installments,payment_value)
select distinct payment_sequential,payment_type,payment_installments,payment_value from order_staging;

--data check
select * from dim_payments limit 5;
select count(*) from dim_payments;

--4.dim_customer
create table dim_customer(
	customer_id character varying(300) PRIMARY KEY,
	customer_unique_id character varying(300),
	customer_zip_code_prefix character varying(100),
	customer_city character varying(100),
	customer_state character varying(100)
);

insert into dim_customer (customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state)
select distinct customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state from order_staging;

--data check
select * from dim_customer limit 5;
select count(*) from dim_customer;
select count(distinct customer_id) from order_staging;

alter table dim_customer
add previous_city character varying(100) default 'Unknown';

--5. dim_customer_location
create table dim_customer_location(
	customer_location_id SERIAL PRIMARY KEY,
	customer_zip_code_prefix character varying(300),
	customer_city character varying(100),
	customer_state character varying(100)
);

insert into dim_customer_location (customer_zip_code_prefix,customer_city,customer_state)
select distinct customer_zip_code_prefix,customer_city,customer_state from order_staging;




--data check
select * from dim_customer_location limit 5;
select count(*) from dim_customer_location;

--6.dim_seller_location
create table dim_seller_location(
	seller_location_id SERIAL PRIMARY KEY,
	seller_zip_code_prefix character varying(300),
	seller_city character varying(100),
	seller_state character varying(100)
);

insert into dim_seller_location (seller_zip_code_prefix,seller_city,seller_state)
select  distinct seller_zip_code_prefix,seller_city,seller_state from order_staging;


--7.dim_products (SCD type 2)

create table dim_products(
	product_unique_id Serial primary key,
	product_id character varying(300),
	product_category_name character varying(100),
	product_name_lenght float(25),
	product_description_lenght float(25),
	product_photos_qty float(25),
	product_weight_g float(25),
	product_length_cm float(25),
	product_height_cm float(25),
	product_width_cm float(25),
	product_category_name_english character varying(100),
	current_flag char(1) NOT NULL DEFAULT 'Y',
    RowStartDate date NOT NULL DEFAULT CURRENT_TIMESTAMP,
    RowEndDate date NOT NULL DEFAULT '12/31/9999'
);

insert into dim_products (product_id,product_category_name,product_name_lenght,
						  product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,
						  product_height_cm,product_width_cm,product_category_name_english
						 )
select distinct product_id,product_category_name,product_name_lenght,
						  product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,
						  product_height_cm,product_width_cm,product_category_name_english from product_staging;
						  
alter table dim_products
drop column current_flag and alter table dim_products drop column rowenddate and rowenddate
--data check

select * from dim_products limit 5;
select count(*) from dim_products;

--Scd Type 2 check (changing product_description_lenght from )

update dim_products
    set current_flag = 'N',
    RowEndDate = Current_timestamp
	where product_id = 'c2bd6687fd8abe764233efb9c4a3c27d' and current_flag = 'Y';
	
insert into dim_products (product_id,product_category_name,product_name_lenght,
						  product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,
						  product_height_cm,product_width_cm,
						  product_category_name_english,current_flag,rowstartdate,rowenddate) 
values('c2bd6687fd8abe764233efb9c4a3c27d','cool_stuff',36,1450,1,700,32,32,16,'cool_stuff','Y',Current_timestamp,'9999-12-31')
	
--verify rows
select * from dim_products where product_id = 'c2bd6687fd8abe764233efb9c4a3c27d';

--8.dim_cate_translation
create table dim_cate_translation(
	product_category_name character varying(100) PRIMARY KEY,
	product_category_name_english character varying(100)
);

insert into dim_cate_translation (product_category_name,product_category_name_english)
select  distinct product_category_name,product_category_name_english from product_staging;

--data check
select * from dim_cate_translation limit 5;
select count(*) from dim_cate_translation;
select  count(distinct product_category_name) from product_staging;


--9.dim_seller

create table dim_seller(
	seller_id character varying(300) PRIMARY KEY,
	seller_zip_code_prefix character varying(100),
	seller_city character varying(100),
	seller_state character varying(100)
);

insert into dim_seller (seller_id,seller_zip_code_prefix,seller_city,seller_state)
select  distinct seller_id,seller_zip_code_prefix,seller_city,seller_state from order_staging;

--data check
select * from dim_seller limit 5;
select count(*) from dim_seller;
select  count(distinct seller_id) from order_staging;


--10.dim_deals
create table dim_deals(
	deal_id serial PRIMARY KEY,
	sdr_id character varying(100),
	sr_id character varying(100)
);

insert into dim_deals (sdr_id,sr_id)
select  distinct sdr_id,sr_id from mql_staging;

--data check
select * from dim_deals limit 5;
select count(*) from dim_deals;

--11.dim_marketing_qualified_leads
create table dim_marketing_qualified_leads(
	mql_id character varying(300) Primary key,
	first_contact_date timestamp,
	landing_page_id character varying(300),
	origin character varying(100)
);

insert into dim_marketing_qualified_leads (mql_id,first_contact_date,landing_page_id,origin)
select  distinct mql_id,first_contact_date,landing_page_id,origin from mql_staging;

--data check
select * from dim_marketing_qualified_leads limit 5;
select count(*) from dim_marketing_qualified_leads;
select  count(distinct mql_id) from mql_staging;


--Fact table creation
--  fact_order
create table fact_order(
	order_id character varying(300),
	time_id int,
	customer_id character varying(300),
	customer_location_id int,
	seller_id character varying(300),
	seller_location_id int,
	payment_id int,
	product_unique_id int,
	order_item_id float(25),
	shipping_limit_date timestamp, 
	order_status character varying(100),
	payment_sequential float(25),
	payment_type character varying(100),
	payment_installments float(25),
	payment_value float(25),
	price float(25),
	freight_value float(25),
	product_photos_qty float(25),
	product_weight_g float(25),
	product_length_cm float(25),
	product_height_cm float(25),
	product_width_cm float(25)
);

insert into fact_order(
	order_id,time_id,customer_id,customer_location_id,seller_id,seller_location_id,payment_id,
	product_unique_id,order_item_id,shipping_limit_date,order_status,payment_sequential,payment_type,
	payment_installments,payment_value,price,freight_value,product_photos_qty,product_weight_g,
	product_length_cm,product_height_cm,product_width_cm)
	
select temp_order.order_id,dim_time.time_id,customer_id,dim_customer_location.customer_location_id,seller_id,
	dim_seller_location.seller_location_id,dim_payments.payment_id,temp_order.order_item_id,dim_products.product_unique_id,
	temp_order.shipping_limit_date,temp_order.order_status,dim_payments.payment_sequential,dim_payments.payment_type,
	dim_payments.payment_installments,dim_payments.payment_value,temp_order.price,temp_order.freight_value,
	dim_products.product_photos_qty,dim_products.product_weight_g,dim_products.product_length_cm,dim_products.product_height_cm,
	dim_products.product_width_cm
	from temp_order
	join dim_time on temp_order.order_purchase_timestamp = dim_time.order_purchase_timestamp
	join dim_seller_location on temp_order.seller_zip_code_prefix = dim_seller_location.seller_zip_code_prefix and
			temp_order.seller_city = dim_seller_location.seller_city and
			temp_order.seller_state = dim_seller_location.seller_state --seller_location_id
	
	join dim_customer_location on temp_order.customer_zip_code_prefix = dim_customer_location.customer_zip_code_prefix and
			temp_order.customer_city = dim_customer_location.customer_city and
			temp_order.customer_state = dim_customer_location.customer_state --customer_location_id
			
	join dim_payments on temp_order.payment_sequential = dim_payments.payment_sequential and
			temp_order.payment_type = dim_payments.payment_type and 
			temp_order.payment_installments = dim_payments.payment_installments and
			temp_order.payment_value = dim_payments.payment_value 
			
	right join dim_products on temp_order.product_id = dim_products.product_id
order by order_id;

--fact constraints
alter table fact_order
add constraint fk_order foreign key(order_id)
references dim_order(order_id);

alter table fact_order
add constraint fk_time foreign key(time_id)
references dim_time(time_id);

alter table fact_order
add constraint fk_customer foreign key(customer_id)
references dim_customer(customer_id);

alter table fact_order
add constraint fk_customer_location foreign key(customer_location_id)
references dim_customer_location(customer_location_id);

alter table fact_order
add constraint fk_seller_location foreign key(seller_location_id)
references dim_seller_location(seller_location_id);

alter table fact_order
add constraint fk_payment foreign key(payment_id)
references dim_payments(payment_id);

alter table fact_order
add constraint fk_product foreign key(product_unique_id)
references dim_products(product_unique_id);

alter table fact_order
add constraint fk_seller foreign key(seller_id)
references dim_seller(seller_id);

--data check
select * from fact_order limit 5;



--Fact marketing creation
create table fact_marketing(
	mql_id character varying(300),
	seller_id character varying(300),
	deal_id int,
	business_segment character varying(100),
	lead_type character varying(100),
	first_contact_date timestamp,
	won_date timestamp,
	lead_behaviour_profile character varying(100),
	business_type character varying(100),
	declared_monthly_revenue float(25)
);


insert into fact_marketing(
	mql_id,seller_id,deal_id,business_segment,lead_type,first_contact_date,won_date,
	lead_behaviour_profile,business_type,declared_monthly_revenue)
	
select temp_mql.mql_id,temp_mql.seller_id,dim_deals.deal_id,business_segment,temp_mql.lead_type,
		temp_mql.first_contact_date,temp_mql.won_date,temp_mql.lead_behaviour_profile,temp_mql.business_type,temp_mql.declared_monthly_revenue
	from temp_mql
	join dim_deals
	on dim_deals.sdr_id = temp_mql.sdr_id and
			dim_deals.sr_id = temp_mql.sr_id
	order by mql_id

--fact constraints
alter table fact_marketing
add constraint fk_mql foreign key(mql_id)
references dim_marketing_qualified_leads(mql_id);

alter table fact_marketing
add constraint fk_sell foreign key(seller_id)
references dim_seller(seller_id);

alter table fact_marketing
add constraint fk_deal foreign key(deal_id)
references dim_deals(deal_id);

--data check
select * from fact_marketing limit 5;



--Business queries

--What are the average order values of customers?
select
final_data.customer_unique_id,
count(final_data.order_id) as Total_Orders_By_Customers,
avg(final_data.payment_value) as Total_Payment_By_Customers,
final_data.customer_city,
final_data.customer_state
from (

select
delivery.customer_unique_id,
delivery.customer_id,
delivery.order_id,
delivery.customer_city,
delivery.customer_state,
delivery.order_status,
delivery.order_delivered_customer_date,
payment_details.payment_value
from
	(select
	dim_customer.customer_unique_id,
	fact_order.customer_id,
	fact_order.order_id,
	dim_customer.customer_city,
	dim_customer.customer_state,
	dim_order.order_status,
	dim_time.order_delivered_customer_date
	from fact_order
	join dim_customer on fact_order.customer_id=dim_customer.customer_id
	join dim_order on fact_order.order_id=dim_order.order_id
	join dim_time on fact_order.time_id=dim_time.time_id
	group by 1,2,3,4,5,6,7
	order by 1,2,3 asc) delivery

join (select
fact_order.customer_id,
fact_order.order_id,
dim_order.order_status,
dim_time.order_delivered_customer_date,
dim_payments.payment_value

from fact_order
join dim_order on fact_order.order_id=dim_order.order_id
join dim_time on fact_order.time_id=dim_time.time_id
join dim_payments on fact_order.payment_id=dim_payments.payment_id
group by 1,2,3,4,5) payment_details

on delivery.customer_id=payment_details.customer_id
and delivery.order_id=payment_details.order_id 
and delivery.order_status=payment_details.order_status
and delivery.order_delivered_customer_date=payment_details.order_delivered_customer_date
group by 1,2,3,4,5,6,7,8
order by 1,2,3,4,5,6,7,8 asc
) final_data
where final_data.order_status='delivered'
group by 1,4,5
order by 1;

--Top 5 Cities with highest revenue from 2016 to 2018

select 
result.customer_city,
result.customer_state,
result.Total_Orders_By_Customers as Total_Orders,
result.Total_Payment_By_Customers as Total_Customers_Payment
from (
	select
	raw_data.customer_city,
	raw_data.customer_state,
	count(distinct raw_data.order_id) as Total_Orders_By_Customers,
	sum(raw_data.payment_value) as Total_Payment_By_Customers
	from (

	select
	delivery_details.customer_unique_id,
	delivery_details.customer_id,
	delivery_details.order_id,
	delivery_details.customer_city,
	delivery_details.customer_state,
	delivery_details.order_status,
	delivery_details.order_delivered_customer_date,
	payment_details.payment_value
	from
	(select
	dim_customer.customer_unique_id,
	fact_order.customer_id,
	fact_order.order_id,
	dim_customer_location.customer_city,
	dim_customer_location.customer_state,
	dim_order.order_status,
	dim_time.order_delivered_customer_date
	from fact_order
	join dim_customer on fact_order.customer_id=dim_customer.customer_id
	join dim_order on fact_order.order_id=dim_order.order_id
	join dim_customer_location on fact_order.customer_location_id = dim_customer_location.customer_location_id
	join dim_time on fact_order.time_id = dim_time.time_id
	group by 1,2,3,4,5,6,7
	order by 1,2,3 asc) delivery_details

	join (select
	fact_order.customer_id,
	fact_order.order_id,
	dim_order.order_status,
	dim_time.order_delivered_customer_date,
	dim_payments.payment_value

	from fact_order
	join dim_order on fact_order.order_id = dim_order.order_id
	join dim_time on fact_order.time_id = dim_time.time_id
	join dim_payments on fact_order.payment_id = dim_payments.payment_id
	group by 1,2,3,4,5) payment_details
	on delivery_details.customer_id=payment_details.customer_id
	and delivery_details.order_id=payment_details.order_id 
	and delivery_details.order_status=payment_details.order_status
	and delivery_details.order_delivered_customer_date=payment_details.order_delivered_customer_date
	group by 1,2,3,4,5,6,7,8
	order by 1,2,3,4,5,6,7,8 asc
	) raw_data
	where raw_data.order_status='delivered'
	group by 1,2
	order by 1,2 desc
	) result

group by 1,2,3,4
order by 4 desc
limit 5



--Fact_cummulative
create table fact_cum_order(
	time_id int,
	payment_id int,
	order_id character varying(300),
	avg_freight_value float(25),
	avg_payment_value float(25),
	avg_price float(25)
)

alter table fact_cum_order
add constraint fk2_time foreign key(time_id)
references dim_time(time_id);

alter table fact_cum_order
add constraint fk2_payment foreign key(payment_id)
references dim_payments(payment_id);

alter table fact_cum_order
add constraint fk2_order foreign key(order_id)
references dim_order(order_id);


--inserting values in fact_cum_order
insert into fact_cum_order(time_id,payment_id,order_id,avg_freight_value,avg_payment_value,avg_price)
	select dim_time.time_id, dim_payments.payment_id,temp_order.order_id,
			avg(temp_order.freight_value) as avg_freight_value,
			avg(dim_payments.payment_value) as avg_payment_value,
			avg(temp_order.price) as avg_price
	from temp_order
	join dim_time on temp_order.order_purchase_timestamp = dim_time.order_purchase_timestamp
	join dim_payments on temp_order.payment_sequential = dim_payments.payment_sequential and
				temp_order.payment_type = dim_payments.payment_type and 
				temp_order.payment_installments = dim_payments.payment_installments and
				temp_order.payment_value = dim_payments.payment_value
	group by 1,2,3
	order by 3

--data check
select * from fact_cum_order limit 5;
select count(*) from fact_cum_order;

--
select * from product_staging limit 5;

--adding change column in product_staging
alter table product_staging
drop column 
add Change_date date default '9999-12-31';


--- scd2 trigger
--procedure for change
create or replace function new_date()returns trigger
 LANGUAGE plpgsql as 
$$
begin
      update dim_products
          set current_flag = 'N',
		  		
              rowstartdate = product_staging.change_date
      			from product_staging
    		  where product_staging.product_id = dim_products.product_id and
          	  		dim_products.current_flag = 'Y';
	insert into dim_products (product_id,product_category_name,product_name_lenght,product_description_lenght
							  ,product_photos_qty,product_weight_g,product_length_cm,product_height_cm
							  ,product_width_cm,product_category_name_english,current_flag, rowstartdate, rowenddate) 
     select distinct product_id,product_category_name,product_name_lenght,product_description_lenght
							  ,product_photos_qty,product_weight_g,product_length_cm,product_height_cm
							  ,product_width_cm,product_category_name_english, 'Y', change_date, '9999-12-31'::date
     from product_staging where product_staging.change_date !='9999-12-31';
	 return null;
end$$;



--trigger
create or replace trigger scd_type
	after insert
	on product_staging
	for each row
	execute procedure new_date();
	
	
-- inserting values in product_staging

insert into product_staging(product_id,product_category_name,product_name_lenght,product_description_lenght
							  ,product_photos_qty,product_weight_g,product_length_cm,product_height_cm
							  ,product_width_cm,product_category_name_english,Change_date)
values('03b29ca1beec2e03e15a3b980c1505ed','perfumaria',34,250,2,800,27,5,20,
	   'perfumery',current_timestamp);
	   -- changing
	   
	   
-- the above trigger should create a new record with the history saved
select * from product_staging where product_id = '03b29ca1beec2e03e15a3b980c1505ed'
select * from dim_products where product_id = '03b29ca1beec2e03e15a3b980c1505ed'


-- scd 3 on dim_customer

alter table order_staging
add change_date date default '9999-12-31';
alter table dim_customer
add present_city character varying(100) default 'Unknown' ;

--procedure for change
create or replace function scd_3newtrigg()returns trigger
 LANGUAGE plpgsql as 
$$
begin
      update dim_customer
          set previous_city = dim_customer.customer_city,
		  	present_city = order_staging.customer_city
		  from order_staging
    		where order_staging.customer_id = dim_customer.customer_id and
          	  		order_staging.change_date != '9999-12-31';
	 return null;
end$$;

--drop function scd_new()

--trigger
create or replace trigger scd_3
	after update
	on order_staging
	for each row
	execute procedure scd_3newtrigg();
	
--changing in order_staging
update order_staging
set customer_city = 'san paulo',change_date = current_timestamp
	where customer_id = 'd67b6cca5a87299f711a6961f579fe67'


-- the above trigger should create value in previous_city with the history saved
select * from order_staging where customer_id = 'd67b6cca5a87299f711a6961f579fe67'
select * from dim_customer where customer_id = 'd67b6cca5a87299f711a6961f579fe67'