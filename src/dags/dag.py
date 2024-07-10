from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


default_args = {
    'retries' : 3,
    'retry_delay' : timedelta(minutes=1)
}

@dag(
    schedule_interval='0/15 * * * *',  
    start_date=datetime(2024, 7, 10),  
    catchup=False,  
    is_paused_upon_creation=False,
    default_args=default_args

)

def main_pipeline():

    engine = PostgresHook('PG_WAREHOUSE_CONNECTION').get_sqlalchemy_engine()

    @task
    def courier_load_to_stg():
        offset = 0
        headers = {'X-Nickname':'Nikita', 'X-Cohort':'26', 'X-API-KEY':'25c27781-8fde-4b30-a22e-524044a7580f'}
        while True:    
            couriers_rep = requests.get(f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers/?sort_field=_id&sort_direction=asc&offset={offset}',\
                headers = headers).json()
            if len(couriers_rep) == 0:
                break
            
            df = pd.DataFrame(couriers_rep)
            df.to_sql('couriers', engine, schema='stg', if_exists='append', index=False)
            offset += len(couriers_rep)

    @task
    def deliveries_load_to_stg():
        offset = 0
        headers = {'X-Nickname':'Nikita', 'X-Cohort':'26', 'X-API-KEY':'25c27781-8fde-4b30-a22e-524044a7580f'}
        while True:    
            deliveries_rep = requests.get(f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries/?sort_field=_id&sort_direction=asc&offset={offset}',\
                headers = headers).json()
            if len(deliveries_rep) == 0:
                break
            
            df = pd.DataFrame(deliveries_rep)
            df.to_sql('deliveries', engine, schema='stg', if_exists='append', index=False)
            offset += len(deliveries_rep)

    sql_load_couriers = '''insert into dds.couriers(courier_id_source,name)
                select distinct
                    stg_c._id as courier_id_source, 
                    stg_c.name
                from 
                    stg.couriers stg_c
                where not exists (
                                    select 
                                        courier_id_source,
                                        name
                                    from 
                                        dds.couriers dds_c
                                    where stg_c._id = dds_c.courier_id_source
                                );
    '''
    load_couriers = PostgresOperator(task_id = 'load_couriers', sql = sql_load_couriers, postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    sql_load_orders =  """
                insert into dds.orders(order_id_source)
                select distinct
                    order_id as order_id_source
                from 
                    stg.deliveries stg_o
                where not exists (	
                                    select 
                                        order_id_source
                                    from 
                                        dds.orders dds_o
                                    where stg_o.order_id = dds_o.order_id_source
                                );
                """
    load_orders = PostgresOperator(task_id = 'load_orders', sql = sql_load_orders, postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    sql_load_deliveries =  """
                insert into dds.deliveries(delivery_id_source)
                select distinct
                    stg_d.delivery_id as delivery_id_source
                from
                    stg.deliveries stg_d
                where not exists (
                                    select 
                                        delivery_id_source
                                    from 
                                        dds.deliveries dds_d
                                    where stg_d.delivery_id = dds_d.delivery_id_source
                                );
                """
    load_deliveries = PostgresOperator(task_id = 'load_deliveries', sql = sql_load_deliveries, postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    sql_load_facts = """
                INSERT INTO dds.fct_deliveries (order_id, delivery_id, courier_id, order_ts, delivery_ts, address, rate, tip_sum, total_sum)
                select
                    dds_d.delivery_id,
                    dds_o.order_id,
                    dds_c.courier_id,
                    stg_d.order_ts,
                    stg_d.delivery_ts,
                    stg_d.address,
                    stg_d.rate,
                    stg_d.tip_sum,
                    stg_d.sum as total_sum
                from 
                    stg.deliveries stg_d
                join
                    dds.deliveries dds_d on stg_d.delivery_id = dds_d.delivery_id_source 
                join
                    dds.orders dds_o on stg_d.order_id = dds_o.order_id_source
                join
                    dds.couriers dds_c on stg_d.courier_id = dds_c.courier_id_source
                where not exists (select
                                    order_ts
                                  from
                                    dds.fct_deliveries dds_fd
                                  where stg_d.order_ts = dds_fd.order_ts);
                """
    load_facts = PostgresOperator(task_id = 'load_load_facts', sql = sql_load_facts, postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    sql_refresh_dm_courier_ledger = 'refresh materialized view cdm.dm_courier_ledger;'

    refresh_dm_courier_ledger = PostgresOperator(task_id = 'refresh_dm_courier_ledger', sql = sql_refresh_dm_courier_ledger, postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    courier_load_to_stg() >> deliveries_load_to_stg() >> [load_couriers, load_orders, load_deliveries] >> load_facts >> refresh_dm_courier_ledger

main_pipeline = main_pipeline()



