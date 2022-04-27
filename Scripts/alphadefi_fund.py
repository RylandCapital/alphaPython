import numpy as np
import pandas as pd
import time
import pymongo
import requests
import datetime as dt

import os

from alphaTerra import alphaTerra
from terrahelper import terraHelper

from dotenv import load_dotenv
from contextlib import redirect_stdout
from printPrepender import PrintPrepender


load_dotenv()

ALPHADEFI_MONGO = os.getenv("ALPHADEFI_MONGO")

m = alphaTerra()


"""define database"""
client = pymongo.MongoClient(ALPHADEFI_MONGO,
ssl=True,
ssl_cert_reqs='CERT_NONE')
db = client.alphaDefi


def apiUpdate():
    """daily data update, anything not being updated live in defiData 1,2,3 or 4"""
    buf = PrintPrepender("[API-Update]: ")
    with redirect_stdout(buf):

        

        
        #saves top 100 coinmarket cap top 100 tokens market cap info, need to deprecate
        collection = db.coinmarketcaps
        collection.create_index("id", unique=True)
        coincaps = terraHelper.coinmarketcaps()
        coincaps["last_updated"] = pd.to_datetime(coincaps["last_updated"])
        coincaps["id"] = coincaps["slug"] + coincaps["last_updated"].astype(str)

        errors = []
        for document in coincaps.to_dict("rows"):
            try:
                collection.insert_one(document)
            except Exception as e:
                errors.append(e)

        #update spreadtracker 
        spreadtracker_data = m.alphatrackerUpdate()

        collection = db.tokenDICT
        collection.drop()
        time.sleep(2)
        collection.insert_one(spreadtracker_data[1])

        anchor_data = m.anchorAPY()  
        anchor_data[0]["id"] = anchor_data[0]["date"].astype(str) + anchor_data[0]["ticker"]
        anchor_data[0]["date"] = pd.to_datetime(anchor_data[0]["date"])

        #update anchor module data
        anchordatadf = anchor_data[0].to_dict("records")

        collection = db.HistoricalAnchor
        collection.create_index("id", unique=True)
        for i in anchordatadf:
            try:
                collection.insert_one(i)
            except:
                pass

        collection = db.anchor_dict
        collection.drop()
        time.sleep(2)
        collection.insert_one(anchor_data[1])

        #update nexus module
        print("nexus")
        nexus_update = m.nexus()

        collection = db.nexusDict
        collection.drop()
        time.sleep(2)
        collection.insert_one(nexus_update[1])

        nexusdf = nexus_update[0]
        nexusdf["date"] = pd.to_datetime(nexusdf["date"])
        nexusdf["id"] = nexusdf["date"].astype(str) + nexusdf["ticker"]
        collection = db.nexusVaults
        collection.create_index("id", unique=True)
        time.sleep(2)
        collection.insert_many(nexusdf.to_dict("records"))

        #update terra core
        marketcap_data = m.luna_staking()

        marketcapdf = marketcap_data[0]
        marketcapdf["id"] = marketcapdf["date"].astype(str) + marketcapdf["ticker"]
        marketcapdf = marketcapdf.groupby(["ticker"]).last().reset_index().to_dict("records")
        collection = db.dashboard
        collection.create_index("id", unique=True)
        for i in marketcapdf:
            try:
                collection.insert_one(i)
            except:
                print(i)
                pass

        collection = db.dashboardDict
        collection.drop()
        time.sleep(2)
        collection.insert_one(marketcap_data[1])

        #flipside daily data
        df = pd.DataFrame(
                requests.get(
                'https://api.flipsidecrypto.com/api/v2/queries/090ab251-7160-46da-ba84-5100e3ec7623/data/latest'
            ).json()
        )
        df['date'] = pd.to_datetime(df['DATES2'].astype(str) + '-' + df['DATES'].astype(str) + '-' + '1')
        df["id"] = df["date"].astype(str)
        df = df.drop(['DATES', 'DATES2'], axis=1)
        df = df.sort_values('date')

        collection = db.txFailRate
        collection.create_index("id", unique=True)
        collection.drop()
        time.sleep(2)
        collection.insert_many(df.to_dict("records"))

        #circulating supplies, marketcaps, and prices of terra pools by timestamp collected
        snapshot = terraHelper.terra_token_snapshot()
       
        collection = db.circulating
        collection.insert_many(snapshot.to_dict("records"))

        ####Rollups
        #data pulls for all days BEFORE the stop date, not on the stop date
        stop = pd.to_datetime(pd.datetime.now().date())
        start = pd.to_datetime((pd.datetime.now()-dt.timedelta(days=3)).date())

        collection = db.aprs
        data = collection.find({'timestamp': {'$lt': stop, '$gte': start}})
        datas = []
        for i in data:
            datas.append(i)

        insert = pd.DataFrame(datas)
        insert['date'] = pd.to_datetime(insert['timestamp']).dt.date
        insert2 = insert.groupby(['dex','date','masterSymbol'])['apr7d'].mean()
        insert2 = insert2.reset_index()
        insert2['date'] = pd.to_datetime(insert2['date'])

        collection = db.aprsRollD
        for i in insert2.reset_index().to_dict(orient="records"):
            try:
                collection.insert(i)
                print('!!!!!!!!!!!!!!!!!!!!!!!!!')
            except:
                print('error')
                pass

        ##backtester daily data
        collection = db.backtester
        data = m.backtest_data()
        tail = data.groupby('ticker').tail(5)

        for i in tail.to_dict(orient="records"):
            try:
                collection.insert(i)
                print('!!!!!!!!!!!!!!!!!!!!!!!!!')
            except:
                print('error')
                pass

