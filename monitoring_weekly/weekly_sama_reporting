import datetime
import logging

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    import pandas as pd
    from tabulate import tabulate
    from sqlalchemy import create_engine
    import os
    from sparkpost import SparkPost
    from sparkpost.exceptions import SparkPostAPIException



# Create connection to snowflake db
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/'.format(
            user=os.environ["SNOWFLAKE_USER"] ,
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
        )
    )


    # Generate strings of SQL
    usewh_string = "use warehouse ANALYST_WH;"
    usedb_string = "use FIVETRAN;"

    sql_string_1 = """
    select b.STATE
        , a.name as PRODUCT_NAME
        , BRAND
        , VENDOR
        , CATEGORY
        ,  PRODUCT_TYPE
        ,  UNIT
        ,  FLAGGED_DATE
        ,  FLAG_COMMENTS
        ,  FLAGGED_BY
        ,   concat('https://headset.azurewebsites.net/importedproducts/',IMPORTED_PRODUCT_ID) as link_to_imported_product
        ,   imported_product_id
        ,  a.STORE_ID
        ,  FLAGGED
    from STAGING.IMPORTED_PRODUCTS a
    join BASE.STORES b
    on a.STORE_ID = b.STORE_ID
    where FLAGGED = TRUE
    and FLAGGED_DATE::date > current_date() - 8
    and FLAGGED_BY not ilike '%headset.io%'
    order by FLAGGED_DATE desc
    ;
    """

    sql_string_2 = """
    select d.NAME as user
        ,  a.ACTION
        ,  a.EVENTDATEPST
        ,  a.UNLINKSOURCE
        ,  a.IMPORTEDPRODUCTID
        ,  f.state
        ,  b.NAME as imported_product_name
        ,  b.VENDOR as IP_vendor
        ,  b.PRODUCT_TYPE as IP_product_type
        ,  b.CATEGORY as IP_category
        ,  b.brand as IP_category
        ,  b.unit as IP_unit
        ,  b.LINK_DATE
        ,  b.LINKED_BY
        ,  c.PRODUCT as headset_product
        ,  e.name as headset_brand
        ,  c.PRIMARY_TRAIT_VALUE
        ,  c.CATEGORY as headset_category
        ,  c.SEGMENT as headset_segment
        ,  case when LINK_DATE < EVENTDATEPST then 1 else 0 end as link_first_flag
    from HS_DBO.LINKEVENTS a
    join STAGING.IMPORTED_PRODUCTS b
        on a.IMPORTEDPRODUCTID = b.IMPORTED_PRODUCT_ID
    left join STAGING.PRODUCTS c
        on a.PRODUCTID = c.PRODUCT_ID
    join HS_DBO.ASPNETUSERS d
        on a.USERID = d.id
    left join BASE.BRANDS e
        on e.BRAND_ID = c.BRAND_ID
    join base.stores f
        on b.STORE_ID = f.STORE_ID
    where a.ACTION = 'Unlink'
    and a.EVENTDATEPST >= current_date() - 8
    order by EVENTDATEPST
    ;
    """


    try:
        # Create connection to Snowflake
        connection = engine.connect()
        # Use analyst warehouse
        connection.execute(usewh_string)
        # Use FIVETRAN database
        connection.execute(usedb_string)

        # Execute strings 1 & 2 for requested SAMA reporting data
        connection.execute(sql_string_1)
        connection.execute(sql_string_2)

        # Create a dataframe to hold the flagged products export
        sql_df_1 = pd.read_sql_query(sql_string_1, connection)
        # Create a dataframe to hold the unlink raw data
        sql_df_2 = pd.read_sql_query(sql_string_2, connection)

    finally:
        connection.close()
        engine.dispose()


    dir_path = os.getcwd()

    filename_1 = dir_path+r'\azure_results\headset_weekly_flag_export.csv'
    filename_2 = dir_path+r'\azure_results\headset_weekly_unlink_export.csv'

    # Send the dfs to csvs
    sql_df_1.to_csv(filename_1,index=False)
    sql_df_2.to_csv(filename_2,index=False)


    # Create a sparkpost instance and enter API key
    sp = SparkPost('bb5f62efad9fefc6bb39a849ad5dd5248550ce35')

    try:
        response = sp.transmissions.send(
            recipients=['mlaferla@headset.io'],
            html='Please find the Headset team\'s unlinks and flagged products attached. Reach out to cooper@headset.io with issues or questions.',
            from_email='data@headset.io',
            subject='Headset Weekly Flag and Unlink Export',
            attachments=[
                {
                    "name": filename_1.split('\\')[-1],
                    "type": "text/csv",
                    "filename": filename_1
                },
                {
                    "name": filename_2.split('\\')[-1],
                    "type": "text/csv",
                    "filename": filename_2
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
    os.remove(filename_1)
    os.remove(filename_2)