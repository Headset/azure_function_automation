import azure.functions as func
import os
import datetime
import logging
import sys
import importlib.util


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


    # call function with script name in folder that you wish to run. Do not include file extension ie: ".py"
    #run_script('weekly_sama_reporting')
    #run_script('weekly_autolinker_100_links_audit')
    run_script('weekly_insights_brand_monitoring')


def run_script(script_name):
    script_path = os.path.join(os.path.dirname(__file__), f'{script_name}.py')

    s = importlib.util.spec_from_file_location(script_name, script_path)
    m = importlib.util.module_from_spec(s)
    sys.modules[s.name] = m
    s.loader.exec_module(m)