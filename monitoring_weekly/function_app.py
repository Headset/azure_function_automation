import azure.functions as func
import logging


# Azure function timer. To alter timer cadence see function.json file
app = func.FunctionApp()

@app.function_name(name="monitoring_weekly")
@app.schedule(schedule="0 5 * * 1", 
              arg_name="mytimer",
              run_on_startup=True,
              use_monitor=True) 

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
        
    logging.info('Python timer trigger function ran at %s', utc_timestamp)