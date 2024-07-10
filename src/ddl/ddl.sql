CREATE TABLE IF NOT EXISTS stg.couriers (
_id varchar(100) not null,
name varchar(100) not null
);

CREATE TABLE IF NOT EXISTS stg.deliveries (
order_id varchar(100) NOT NULL,
order_ts timestamp NOT NULL,
delivery_id varchar(100) NOT NULL,
courier_id varchar(100) NOT NULL,
address text NOT NULL,
delivery_ts timestamp NOT NULL,
rate smallint NOT NULL,
sum numeric(14, 2) NOT NULL,
tip_sum numeric(14, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.orders(
	order_id serial PRIMARY key NOT NULL,
	order_id_source	varchar(30) unique NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.couriers(
	courier_id serial PRIMARY key NOT NULL,
	courier_id_source varchar(30) unique NOT NULL, 
	name varchar
);

CREATE TABLE IF NOT EXISTS dds.deliveries(
	delivery_id serial PRIMARY key NOT NULL,
	delivery_id_source varchar(30) unique NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries(
    order_id integer PRIMARY key NOT NULL,
	delivery_id	integer NOT NULL,
	courier_id	integer NOT NULL,
	order_ts timestamp NOT NULL,
    delivery_ts timestamp NOT NULL,
	address varchar NOT NULL,
	rate  integer NOT NULL,
	tip_sum numeric (14, 2) NOT NULL,
	total_sum numeric (14, 2) NOT null,
	CONSTRAINT fct_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.orders(order_id),
	CONSTRAINT fct_deliveries_delivery_id_fkey FOREIGN KEY (delivery_id) REFERENCES dds.deliveries(delivery_id),
	CONSTRAINT fct_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.couriers(courier_id)
);


create materialized view cdm.dm_courier_ledger as
with fact as (
    select 
        case 
            when f.rate_avg < 4 then case when fd.total_sum*0.05 < 100 then 100 else fd.total_sum*0.05 end 
            when f.rate_avg >=4 and f.rate_avg < 4.5 then case when fd.total_sum*0.07 < 150 then 150 else fd.total_sum*0.07 end 
            when f.rate_avg >= 4.5 and f.rate_avg < 4.9 then case when fd.total_sum*0.08 < 175 then 175 else fd.total_sum*0.08 end
            when f.rate_avg >= 4.9 then case when fd.total_sum*0.10 < 200 then 200 else fd.total_sum*0.1 end
        end as courier_order_cash,
        fd.*,
        extract(year from fd.order_ts) as settlement_year,
        extract(month from fd.order_ts) as settlement_month,
        f.rate_avg
    from 
        dds.fct_deliveries fd
    join (
            select 
                fct.courier_id,
                extract(year from fct.order_ts) as settlement_year,
                extract(month from fct.order_ts) as settlement_month,
                avg(fct.rate)::numeric(10,2) as rate_avg
            from
            dds.fct_deliveries fct
            group by
                fct.courier_id,
                extract(year from fct.order_ts),
                extract(month from fct.order_ts)
    ) f on fd.courier_id = f.courier_id and extract(year from fd.order_ts) = f.settlement_year and extract(month from fd.order_ts) = settlement_month
)
select 
    fct.courier_id as courier_id,
    c.name as courier_name,
    extract(year from fct.order_ts) as settlement_year,
    extract(month from fct.order_ts) as settlement_month,
    count(distinct fct.order_id) as orders_count,
    sum(fct.total_sum) as orders_total_sum,
    avg(fct.rate) as rate_avg,
    (sum(fct.total_sum)/100*25) as order_processing_fee,
    sum(courier_order_cash) as courier_order_sum,
    sum(fct.tip_sum) as courier_tips_sum,
    (sum(courier_order_cash)+sum(fct.tip_sum))*0.95 as courier_reward_sum
from
    dds.fct_deliveries fct
join 
    dds.couriers c on fct.courier_id = c.courier_id
join 
    fact f on f.courier_id = fct.courier_id and f.delivery_id = fct.delivery_id
group by
    fct.courier_id,
    c.name,
    extract(year from fct.order_ts),
    extract(month from fct.order_ts);
   





