import azure.functions as func
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import os
import datetime
import logging
import subprocess


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


filepaths = ["AutoAnalytics\test_file\test_py_one.py",
    "AutoAnalytics\test_file\test_py_two.py",
]

for filepath in filepaths:
    subprocess.call(["python", filepath])