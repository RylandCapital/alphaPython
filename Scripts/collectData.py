import schedule
import time
import pymongo

import os

from alphaTerra import alphaTerra

from dotenv import load_dotenv

from alphadefi_fund import apiUpdate
from defiData1 import job as defiData1Job
from defiData2 import job as defiData2Job
from defiData3 import job as defiData3Job

load_dotenv()

ALPHADEFI_MONGO = os.getenv("ALPHADEFI_MONGO")

m = alphaTerra()

"""define database"""
client = pymongo.MongoClient(ALPHADEFI_MONGO, ssl=True, ssl_cert_reqs="CERT_NONE")
db = client.alphaDefi


if __name__ == "__main__":

    schedule.every().day.at("08:05").do(apiUpdate)
    schedule.every().minute.at(":00").do(defiData1Job)
    schedule.every().minute.at(":00").do(defiData2Job)
    schedule.every(3).minutes.at(":30").do(defiData3Job)
    print("running")
    while True:
        schedule.run_pending()
        time.sleep(60)
