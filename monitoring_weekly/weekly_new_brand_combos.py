import datetime
import logging
import os
import string
import time

from sqlalchemy import create_engine
from sparkpost import SparkPost
import azure.functions as func
import pandas as pd
import sqlalchemy.dialects.sqlite



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


sql_string_s1 = """
create or replace temp table lizland.public.new_state as
select --b.category,
        d.state,
        c.name as brand,
        min(link_date) as link_dt
from staging.imported_products a
join staging.products b
on a.product_id = b.product_id
join insights.dim_brand c
on b.brand_id = c.brand_id
join base.stores d
on a.store_id = d.store_id
group by 1, 2
having min(link_date) >= current_date()-7;"""

sql_string_s2 = """
--new brands in a state file
select bb.*, listagg(imported_product_id, ', ') as variations
from staging.imported_products a
join staging.products b
on a.product_id = b.product_id
join insights.dim_brand c
on b.brand_id = c.brand_id
join base.stores d
on a.store_id = d.store_id
join lizland.public.new_state bb
on d.state = bb.state
and c.name = bb.brand
group by 1, 2, 3
order by state;
"""

sql_string_c1 = """
create or replace temp table lizland.public.new_cat as
select b.category,
        --d.state,
        c.name as brand,
        min(link_date) as link_dt
from staging.imported_products a
join staging.products b
on a.product_id = b.product_id
join insights.dim_brand c
on b.brand_id = c.brand_id
join base.stores d
on a.store_id = d.store_id
group by 1, 2
having min(link_date) >= current_date()-7;
"""

sql_string_c2 = """
--new brand in a category file
select bb.*, listagg(imported_product_id, ', ') as variations
from staging.imported_products a
join staging.products b
on a.product_id = b.product_id
join insights.dim_brand c
on b.brand_id = c.brand_id
join lizland.public.new_cat bb
on b.category = bb.category
and c.name = bb.brand
group by 1, 2, 3
order by category;
"""


try:
    #create connection to DB and tell to use Fivetran
    connection = engine.connect()
    connection.execute(usedb_string) 

    print("executing query")
    connection.execute(sql_string_c1)
    connection.execute(sql_string_s1)
    state_df = pd.read_sql_query(sql_string_s2, connection)
    cat_df = pd.read_sql_query(sql_string_c2, connection)
    print("query executed")
    
finally:
    connection.close()
    engine.dispose()


filename_s = dir_path+r'\new_store_brand_state_combos.csv'
filename_c = dir_path+r'\new_store_brand_cat_combos.csv'
state_df.to_csv(filename_s, index = False)
cat_df.to_csv(filename_c, index = False)


if len(state_df.count()) > 0 or len(cat_df.count()) > 0:
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
        subject="New Brand Combos (from azure)",
        attachments=[
            {
                "name": filename_c.split('\\')[-1],
                "type": "text/csv",
                "filename": filename_c
            }
            , {
                "name": filename_s.split('\\')[-1],
                "type": "text/csv",
                "filename": filename_s
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
os.remove(filename_s)
os.remove(filename_c)       

print("finished")