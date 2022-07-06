create schema if not exists staging_prod;

drop table if exists staging_prod.user_order_log;

CREATE TABLE if not exists staging_prod.user_order_log(
   ID serial,
   date_time          TIMESTAMP,
   city_id            integer,
   city_name          varchar(100),
   customer_id        bigint,
   first_name         varchar(100),
   last_name          varchar(100),
   item_name		  varchar(100),
   item_id            integer,
   quantity           bigint,
   payment_amount     numeric(14, 2),
   status			  varchar(100),
   PRIMARY KEY (ID)
);

insert into staging_prod.user_order_log (date_time, city_id, city_name, customer_id,
	                                       first_name, last_name, item_id, item_name,
                                         quantity, payment_amount, status)
select distinct  date_time,
                 city_id,
                 city_name,
                 customer_id,
                 first_name,
                 last_name,
                 item_id,
                 item_name,
                 case when status = 'refunded' then - quantity else quantity end quantity,
                 case when status = 'refunded' then - payment_amount else payment_amount end payment_amount,
                 status
from staging.user_order_log
where date_time::Date = '{{ds}}';
