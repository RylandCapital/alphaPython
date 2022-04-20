import datetime as dt
import schedule
import time
import pymongo
import pandas as pd
from pytz import timezone
import os

from terrahelper import terraHelper
from alphaTerra import alphaTerra

from dotenv import load_dotenv
from contextlib import redirect_stdout
from printPrepender import PrintPrepender

load_dotenv()

ALPHADEFI_MONGO = os.getenv("ALPHADEFI_MONGO")

"""define database"""
client = pymongo.MongoClient(ALPHADEFI_MONGO,
ssl=True,
ssl_cert_reqs='CERT_NONE')
db = client.alphaDefi


## Run every 4 hours
def job():

    buf = PrintPrepender("[Defi Data Aprs]: ")
    with redirect_stdout(buf):
        print("Starting job")
        try:

            ### Update APRs in Mongo ###

            """master apr data and farmers market grid"""
            apr_updates = alphaTerra().masterAPR()
            final_grid = apr_updates[0]
            aprs = apr_updates[1]

            """pools by dex informational call"""
            collection = db.dexpoolDICT
            collection.drop()
            blankdict = {}
            for i in ["Terraswap", "Astroport", "Loop", "PRISM Swap"]:
                blankdict[i] = aprs[aprs["dex"] == i]["masterSymbol"].unique().tolist()
            collection.insert_one({"dex": blankdict})

            """appends historical data with newest timestamped data"""
            collection = db.aprs
            aprs["timestamp"] = pd.to_datetime(aprs["timestamp"])
            collection.insert_many(aprs.to_dict(orient="records"))

            """drops and replaces farmers market gird data"""
            collection = db.aprCompare
            collection.drop()
            collection.insert_many(final_grid.to_dict(orient="records"))

        except Exception as e:
            print("Data_Aprs Error", e)
            pass
