import datetime
import logging
import os
import string
import time

from sparkpost import SparkPost
from sqlalchemy import create_engine
import azure.functions as func
import pandas as pd
import sqlalchemy.dialects.sqlite



def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


    logging.info("Weekly Monday New Store Monitoring")

sp = SparkPost(os.environ["SPARKPOST_KEY"])

dir_path = os.getcwd()


engine = create_engine(
    'snowflake://{user}:{password}@{account}/'.format(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
    )
)


usedb_string = "use FIVETRAN;"


sql_string = """
with chain as (
        select company_id, count(distinct store_id) as num_stores_in_chain
        from base.stores
        where ACTIVE = 1
        group by 1
    )
    , new_stores as (
        select state
            , company_id
            , store_id
        from base.stores s
        where ACTIVE = 1
        and date_created between current_date() - 7 and current_date() -1
    )
    , state_sales_T30days as (
        select ps.state
            , sum(base_price - discount) as state_sales
        from staging.product_sales ps
        where transaction_date_local between current_date - 30 and current_date -1
        group by 1
    )
    , store_sales_T30days as (
        select ps.state
            , ps.COMPANY_ID
            , ps.store_id
            , sum(base_price-discount) as store_sales
            , count(distinct case when ps.customer_key is not null
                        and gender in ('M', 'F')
                        and dob is not null
                    then receipt_id
                    else 0
                end) as demographics_trxns
            , count(distinct receipt_id) as num_trxns
            , count(distinct case when is_delivery = 1 then receipt_id else null end) as delivery_trxns
            , count(distinct transaction_date_local::date) as num_days
        from staging.PRODUCT_SALES ps
        join new_stores ns on ps.STORE_ID = ns.STORE_ID
        join base.customers c on ps.CUSTOMER_KEY = c.CUSTOMER_KEY
        where transaction_date_local between current_date - 30 and current_date -1
        group by 1, 2, 3
        )
    , avg_store_sales as (
        select
            store.state
            , store.COMPANY_ID
            , store_id
            , case when c.num_stores_in_chain > 1 then 'Y' else 'N' end as is_chain
            , c.num_stores_in_chain
            , store_sales / num_days as avg_daily_store_sales
            , (state_sales/30) as avg_daily_state_sales
            , avg_daily_store_sales / avg_daily_state_sales as avg_daily_store_mkt_share
            , demographics_trxns / num_trxns as perc_demo_trxns
            , delivery_trxns / num_trxns as perc_delivery
        from store_sales_T30days store
        join state_sales_T30days state on store.state = state.state
        left join chain c on c.COMPANY_ID = store.COMPANY_ID
    )
    select * from avg_store_sales
    where avg_daily_store_mkt_share > 0.05
    or perc_demo_trxns > 0.25
    or perc_delivery > 0.1
    or num_stores_in_chain > 1
    order by 6 desc
"""

try:
    #create connection to DB and tell to use Fivetran
    connection = engine.connect()
    connection.execute(usedb_string) 

    print("executing query")
    sql_df = pd.read_sql_query(sql_string, connection)
    
finally:
    connection.close()
    engine.dispose()



filename = dir_path+r'\new_store_monitoring_results.csv'
sql_df.to_csv(filename, index = False)


# # print(data_to_email)    
if len(sql_df.count()) > 0:

    msgHtml = """
        <html>
    <body><p>Hello, friend.</p>
    <p>See attached for your data:</p>
    <p>Regards,</p>
    <p>Me</p>
    </body></html>
    """
    try:
        sp.transmissions.send(
        recipients=['mlaferla@headset.io', 'plong@headset.io', 'cooper@headset.io'],
        html=msgHtml,
        from_email='analytics-monitor@headset.io',
        subject="New Store Monitoring",
        attachments=[
            {
                "name": filename.split('\\')[-1],
                "type": "text/csv",
                "filename": filename
            }
        ]
        )
    except SparkPostAPIException as err:
        # http response status code
        print(err.status)
        # python requests library response object
        # http://docs.python-requests.org/en/master/api/#requests.Response
        print(err.response.json())
        # list of formatted errors
        print(err.errors)


# Finally, remove files
os.remove(filename)       

print("finished")