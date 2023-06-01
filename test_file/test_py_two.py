import azure.functions as func
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import os
import datetime
import logging


#add snowflake.sqlachelmy depependencies
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

# Create connection to snowflake db
engine = create_engine(
    'snowflake://{user}:{password}@{account}/'.format(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
    ))


# set environment
usewh_string = "use warehouse ANALYST_WH;"
usedb_string = "use FIVETRAN;"
userole_string = "use role etl_user"



# truncate table 
sql_string_0 = """

Insert into lizland.dukeofne.azure_function_one
values
(
'azure function two'
, current_date
, current_timestamp
)

"""


# create connection to DB and tell to use Fivetran
connection = engine.connect()
connection.execute(usewh_string)
connection.execute(usedb_string)
connection.execute(userole_string)

connection.execute(sql_string_0)


# close connection
connection.close()
engine.dispose()