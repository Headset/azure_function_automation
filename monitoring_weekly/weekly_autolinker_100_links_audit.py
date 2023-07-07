import datetime
import logging
import os 
import azure.functions as func
import time

from sqlalchemy import create_engine
from sparkpost import SparkPost
import sqlalchemy.dialects.sqlite
import pandas as pd


sp = SparkPost(os.environ["SPARKPOST_KEY"])

dir_path = os.getcwd()


engine = create_engine(
    'snowflake://{user}:{password}@{account}/'.format(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
    )
)

set_cached_results_string = "alter session set use_cached_result = FALSE;"
usedb_string = "use FIVETRAN;"

sql_string = """
with recent_links as (
    select imported_product_id
        , ip.name                                                     as pos_product_name
        , ip.category                                                 as pos_category
        , c.category                                                  as predicted_category
        , ip.brand                                                    as pos_brand
        , ip.vendor                                                   as pos_vendor
        , ip.unit                                                     as pos_package_size
        , p.product                                                   as tagged_product_name
        , p.CATEGORY                                                  as tagged_category
        , p.ORIGINAL_BRAND_ID                                         as tagged_brand
        , p.PRIMARY_TRAIT_VALUE_NUMBER || ' ' || p.PRIMARY_TRAIT_NAME as tagged_package_size
        , LINKED_BY
        , LINK_DATE_FILLED
    FROM staging.IMPORTED_PRODUCTS ip
            JOIN staging.CATEGORIES c on ip.PREDICTED_CATEGORY_ID = c.CATEGORY_ID
            JOIN staging.products p on ip.PRODUCT_ID = p.PRODUCT_ID
    where LINKED_BY ilike '%autolinker%'
    and LINK_DATE_FILLED >= dateadd('day', -7, current_date)
)
select
    *
from recent_links
sample (100 rows);
"""

try:
    #create connection to DB and tell to use Fivetran
    connection = engine.connect()
    connection.execute(usedb_string) 
    sql_df = pd.read_sql_query(sql_string, connection)

finally:
    connection.close()
    engine.dispose()


if len(sql_df.count()) >0:
    filename = dir_path+r'\autolinker_100_sample_audit.csv'
    sql_df.to_csv(filename, index = False)

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
        #recipients=['mfelipe@headset.io', 'rmontenigro@headset.io'],
        recipients=['mlaferla@headset.io'],
        html=msgHtml,
        from_email='analytics-monitor@headset.io',
        subject="Autolinker 100 link audit",
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
    
    finally:
        pass

# Finally, remove files
os.remove(filename)

print("finished")