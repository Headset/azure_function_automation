import datetime
import logging
import os 
import azure.functions as func
import time

import sqlalchemy.dialects.sqlite
import pandas as pd
from sparkpost import SparkPost
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert



## Timer function, adjust in function.json ##
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    logging.info("Weekly Monday Insights Brand Monitoring")


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
usewarehouse_string = "use warehouse ANALYST_WH"


sql_string = """
        create or replace temp table lizland.LINDSEA.insights_brand_monitoring as
        with prior_sales as (
            select state
                , p.category
                , brand
                , sum(sales) as total_sales
                , sum(quantity) as total_quantity
                , sum(sales)/sum(quantity) as aip
                , rank() over (partition by state, p.CATEGORY order by total_sales desc, total_quantity desc) as rank
                , ratio_to_report(total_sales) over (partition by state, p.CATEGORY) as market_share
            from insights.fact_daily_product_sales ps
            join insights.DIM_PRODUCT p on ps.INSIGHTS_PRODUCT_KEY = p.INSIGHTS_PRODUCT_KEY
            where sold_on >= current_date() - 14 and sold_on < current_date() -7
            and brand <> 'House Brand'
            group by 1,2,3
        )
        , curr_sales as (
            select state
                , p.category
                , brand
                , sum(sales) as total_sales
                , sum(quantity) as total_quantity
                , sum(sales)/sum(quantity) as aip
                , rank() over (partition by state, p.CATEGORY order by total_sales desc, total_quantity desc) as rank
                , ratio_to_report(total_sales) over (partition by state, p.CATEGORY) as market_share
            from insights.fact_daily_product_sales ps
            join insights.DIM_PRODUCT p on ps.INSIGHTS_PRODUCT_KEY = p.INSIGHTS_PRODUCT_KEY
            where sold_on >= current_date() - 7
            and brand <> 'House Brand'
            group by 1,2,3
        )
        , first_sale as (
            select s.state
                    , s.CATEGORY
                    , s.brand
                    , any_value(total_sales) as total_sales
                    , any_value(total_quantity) as total_quantity
                    , any_value(aip) as aip
                    , any_value(rank) as rank
                    , any_value(market_share) as market_share
                    , min(sold_on) as first_sale
                    , max(sold_on) as last_sale
            from insights.FACT_DAILY_PRODUCT_SALES ps
            join curr_sales s on ps.state = s.state
            join insights.DIM_PRODUCT p on ps.INSIGHTS_PRODUCT_KEY = p.INSIGHTS_PRODUCT_KEY and p.category = s.category and p.brand = s.brand

            group by 1,2,3
        )
        select current_date() as week
                , c.state
                , c.category
                , c.BRAND
                , c.total_sales as current_sales
                , c.total_quantity as current_quantity
                , c.market_share as current_brand_category_share
                , c.market_share - p.market_share as brand_category_share_change
                , c.aip as current_aip
                , f.first_sale
                , f.last_sale
        from curr_sales c
        join prior_sales p on c.state = p.state
                            and c.CATEGORY = p.CATEGORY
                            and c.brand = p.brand
        join first_sale f on c.state = f.state
                            and c.category = f.CATEGORY
                            and c.brand = f.brand
        where (c.category in ('Flower', 'Edible', 'Pre-Roll', 'Concentrates', 'Vapor Pens') and
                ((abs(brand_category_share_change) >= .05 and p.rank <= 10  ) or (abs(brand_category_share_change) >= .1 and p.rank <= 30  )) -- top 10 > 5% change or top 30 > 10% change
                    )
            or (c.CATEGORY in ('Oil', 'Tincture & Sublingual', 'Topical', 'Beverage', 'Capsules') and abs(brand_category_share_change) >= .1 and p.rank <= 10)
        order by abs(brand_category_share_change) desc;
"""


sql_string_1 = """ select * from lizland.LINDSEA.insights_brand_monitoring """
sql_string_2 = """
        with brands as (
            select distinct
                state
                , category
                , brand
            from lizland.LINDSEA.insights_brand_monitoring
        ),
        current_stores as (
            select
                distinct ps.state, b.brand, ps.store_id, sum(ps.BASE_PRICE - ps.DISCOUNT) as new_sales
                from brands b
                join insights.DIM_PRODUCT p on b.category = p.category and b.brand = p.brand
                join staging.product_sales ps on ps.state = b.state and ps.INSIGHTS_PRODUCT_KEY = p.INSIGHTS_PRODUCT_KEY
                where exclude_from_insights <> 1
                and ps.transaction_date_local >= current_date() - 7
                group by 1,2,3
            )
        , prior_stores as (
            select
                distinct ps.state, b.brand, ps.store_id
                from brands b
                join insights.DIM_PRODUCT p on b.category = p.category and b.brand = p.brand
                join staging.product_sales ps on ps.state = b.state and ps.INSIGHTS_PRODUCT_KEY = p.INSIGHTS_PRODUCT_KEY
                where exclude_from_insights <> 1
                and ps.transaction_date_local >= current_date() - 14 and ps.transaction_date_local < current_date() -7
            )
        , diff as (
            select state, brand, store_id
            from current_stores
            minus
            select *
            from prior_stores
        )
        select c.*
            from current_stores c
            join diff d on c.state = d.state
                        and c.brand = d.brand
                        and c.STORE_ID = d.STORE_ID
        order by state, brand;
"""


try:
    #create connection to DB and tell to use Fivetran
    connection = engine.connect()
    connection.execute(usedb_string) 
    connection.execute(usewarehouse_string)

    print("executing query")
    connection.execute(sql_string)
    sql_df = pd.read_sql_query(sql_string_1, connection)
    new_brand_stores_df = None
    if len(sql_df) >0:
        new_brand_stores_df = pd.read_sql_query(sql_string_2, connection)
    print("query executed")
    
finally:
    connection.close()
    engine.dispose()



filename = dir_path+r'\insights_brand_monitoring_results.csv'
sql_df.to_csv(filename, index = False)
filenames = [filename]

if new_brand_stores_df is not None: 
    filename2 = dir_path+r'\insights_brand_monitoring_new_brand_stores_results.csv'
    new_brand_stores_df.to_csv(filename2, index = False)
    filenames.append(filename2)



if len(sql_df) > 0:

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
        #recipients=['mlaferla@headset.io', 'plong@headset.io', 'cooper@headset.io'],
        recipients=['mlaferla@headset.io'],
        html=msgHtml,
        from_email='analytics-monitor@headset.io',
        subject="Insights Brand Monitoring",
        attachments=[
            {
                "name": filename.split('\\')[-1],
                "type": "text/csv",
                "filename": filenames[0]
            },
            {
                "name": filename2.split('\\')[-1],
                "type": "text/csv",
                "filename": filenames[1]
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
os.remove(filename2)
        
print("finished")