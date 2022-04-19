import schedule
import time
import threading
import warnings

import os

from dotenv import load_dotenv

from alphadefi_fund import apiUpdate
from defiData1 import job as defiData1Job
from defiData2 import job as defiData2Job
from defiData3 import job as defiData3Job
from defiData4 import job as defiData4Job

warnings.filterwarnings("ignore")


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


if __name__ == "__main__":

    print("Collecting Data")

    schedule.every().day.at("00:00").do(run_threaded, apiUpdate)
    schedule.every(5).minutes.at(":00").do(run_threaded, defiData1Job)
    schedule.every().minute.at(":00").do(run_threaded, defiData2Job)
    #schedule.every(3).minutes.at(":30").do(run_threaded, defiData3Job)
    schedule.every(30).minutes.at(":15").do(run_threaded, defiData4Job)

    while True:
        schedule.run_pending()
        time.sleep(30)
