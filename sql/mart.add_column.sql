 DO $$ BEGIN
        IF (select max(date_time) :: date from staging_prod.user_order_log) = (select (now() - INTERVAL '1 DAY') :: date)
        THEN
        truncate table staging_prod.user_order_log RESTART IDENTITY;
        truncate table mart.f_sales RESTART IDENTITY;
        END IF;
    END$$; 
   
alter table staging.user_order_log add IF NOT EXISTS status varchar(50);
