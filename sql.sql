SQL


-- текущая схема с таблицами --
DROP TABLE IF EXISTS public.orders_attributes;
CREATE TABLE public.orders_attributes (
	client_id int4 NULL,
	datetime varchar(32) NULL,
	payment_amount float4 NULL,
	order_id int4 NULL,
	stock bool NULL,
	category varchar(64) NULL,
	itemcode int4 NULL,
	vendorcode int4 NULL,
	description varchar(32) NULL,
	num float4 NULL,
	priceproduct float4 NULL 
);

DROP TABLE IF EXISTS public.user_activity_log;
CREATE TABLE public.user_activity_log (
	client_id int4 NULL,
	hitdatetime varchar(32) NULL,
	"action" varchar(16) NULL
);

DROP TABLE IF EXISTS public.user_attributes;
CREATE TABLE public.user_attributes (
	client_id int4 NULL,
	utm_campaign varchar(16) NULL,
	first_name varchar(16) NULL,
	last_name varchar(16) NULL
);

DROP table if EXISTS public.user_payment_log;
CREATE TABLE public.user_payment_log (
	client_id int4 NULL,
	hitdatetime varchar(32) NULL,
	"action" varchar(16) NULL,
	payment_amount float4 NULL,
	order_id int4 NULL
);
-- текущая схема с таблицами --

-- новая (целевая) схема с таблицами --
DROP TABLE IF EXISTS public.d_user_payment_log;
DROP TABLE IF EXISTS public.d_user_activity_log;
DROP TABLE IF EXISTS public.f_sales;
DROP TABLE IF EXISTS public.d_clients;
DROP TABLE IF EXISTS public.d_buckets;
DROP TABLE IF EXISTS public.d_orders;
DROP TABLE IF EXISTS public.d_products;
DROP TABLE IF EXISTS public.d_vendors;
DROP TABLE IF EXISTS public.d_categories;

--d_clients
CREATE TABLE public.d_clients(
   client_id    BIGINT ,
   first_name   text ,
   last_name    text ,
   utm_campaign VARCHAR(30),
   PRIMARY KEY  (client_id)
);
CREATE INDEX client_id_index ON public.d_clients(client_id);

--d_user_payment_log
CREATE TABLE public.d_user_payment_log(
   payment_log_id   BIGINT ,
   client_id      	BIGINT ,
   hit_date_time    TIMESTAMP ,
   action           VARCHAR(20),
   payment_amount   NUMERIC(14,2),
   PRIMARY KEY 		(payment_log_id),
   FOREIGN KEY 		(client_id) REFERENCES d_clients(client_id) ON UPDATE cascade
);
CREATE INDEX user_payment_log_id_index  ON public.d_user_payment_log (payment_log_id);

--d_user_activity_log
CREATE TABLE public.d_user_activity_log(
   activity_id    BIGINT ,
   client_id      BIGINT ,
   hit_date_time  TIMESTAMP ,
   action         VARCHAR(20),
   PRIMARY KEY  (activity_id),
   FOREIGN KEY  (client_id) REFERENCES d_clients(client_id) ON UPDATE cascade
);
CREATE INDEX user_activity_id_index ON public.d_user_activity_log (activity_id);

--d_vendors
CREATE TABLE public.d_vendors(
   ID           serial ,
   vendor_id    BIGINT ,
   name_vendor  text ,
   description  text ,
   PRIMARY KEY (ID)
);
CREATE INDEX vendor_id_index ON public.d_vendors (vendor_id);

--d_categories
CREATE TABLE public.d_categories(
   category_id   BIGINT ,
   name_category text ,
   description   text ,
   PRIMARY KEY  (category_id)
);
CREATE INDEX category_id_index ON public.d_categories (category_id);

--d_products
CREATE TABLE public.d_products(
   product_id   BIGINT ,
   category_id  BIGINT ,
   vendor_id    BIGINT,
   name_product text ,
   description  text ,
   stock        boolean,
   PRIMARY KEY (product_id),
   FOREIGN KEY (category_id) REFERENCES d_categories(category_id) ON UPDATE cascade,
   FOREIGN KEY (vendor_id) REFERENCES d_vendors(id) ON UPDATE cascade
);
CREATE INDEX product_id_index ON public.d_products (product_id);

--d_orders
CREATE TABLE public.d_orders(
   order_id      BIGINT ,
   payment       NUMERIC(14,2),
   hit_date_time TIMESTAMP,
   PRIMARY KEY (order_id)
);
CREATE INDEX order_id_index ON public.d_orders (order_id);

--d_buckets
CREATE TABLE public.d_buckets(
   bucket_id    BIGINT ,
   order_id     BIGINT ,
   product_id   BIGINT ,
   num          NUMERIC(14,2),
   PRIMARY KEY (bucket_id),
   FOREIGN KEY (product_id) REFERENCES d_products(product_id) ON UPDATE CASCADE,
   FOREIGN KEY (order_id) REFERENCES d_orders(order_id) ON UPDATE CASCADE
);
CREATE INDEX bucket_id ON public.d_buckets (bucket_id);

--sales information
CREATE TABLE public.f_sales(
   sale_id        BIGINT ,
   order_id       BIGINT ,
   client_id      BIGINT ,
   promotion_id   BIGINT ,
   PRIMARY KEY (sale_id),
   FOREIGN KEY (order_id) REFERENCES d_orders(order_id) ON UPDATE cascade,
   FOREIGN KEY (client_id) REFERENCES d_clients(client_id) ON UPDATE cascade
);
CREATE INDEX sales_order_id_index ON public.f_sales (order_id);
-- новая (целевая) схема с таблицами --
