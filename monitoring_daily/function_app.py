import azure.functions as func
import os
import datetime
import logging
import sys
import importlib.util


# Azure function timer. To alter timer cadence see function.json file
app = func.FunctionApp()

@app.function_name(name="monitoring_daily")
@app.schedule(schedule="0 5 * * 1-5", 
              arg_name="mytimer",
              run_on_startup=False,
              use_monitor=True) 

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
        
    logging.info('Python timer trigger function ran at %s', utc_timestamp)