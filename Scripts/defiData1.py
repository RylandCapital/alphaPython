import requests
import json
import pandas as pd
import datetime as dt
import schedule
import time
import os
import pymongo
import numpy as np
from pytz import timezone

from dotenv import load_dotenv

from contextlib import redirect_stdout
from printPrepender import PrintPrepender

load_dotenv()

ALPHADEFI_MONGO = os.getenv("ALPHADEFI_MONGO")


"""this sripts pings mirror every minute but only saves data at end of the hour
so this seems like overkill, we can just run once per hour?"""

query = """{
  assets {
    symbol
    name
    prices {
      price
      oraclePrice
    }
    pair
    lpToken
    statistic {
      liquidity
      volume
      apr {
        long
        short
      }
    }
  }
}"""
url = "https://graph.mirror.finance/graphql"

"""define database"""
client = pymongo.MongoClient(ALPHADEFI_MONGO, ssl=True, ssl_cert_reqs="CERT_NONE")
db = client.alphaDefi


def job():
    buf = PrintPrepender("[Mirror Data]: ")
    with redirect_stdout(buf):
        print("Starting DefiData1 job")
        try:
            try:

                r = requests.post(url, json={"query": query})
                json_data = pd.DataFrame(json.loads(r.text)["data"]["assets"])
            except:
                time.sleep(2)
                r = requests.post(url, json={"query": query})
                json_data = pd.DataFrame(json.loads(r.text)["data"]["assets"])

            eastern = timezone("US/Eastern")
            now = dt.datetime.now(eastern)

            json_data["price"] = json_data["prices"].apply(
                lambda x: x["price"] if x["price"] is None else float(x["price"])
            )
            json_data["oralceprice"] = json_data["prices"].apply(
                lambda x: x["oraclePrice"] if x["oraclePrice"] is None else float(x["oraclePrice"])
            )
            json_data["liquidity"] = json_data["statistic"].apply(lambda x: float(x["liquidity"]))
            json_data["volume"] = json_data["statistic"].apply(lambda x: float(x["volume"]))
            json_data["apr"] = json_data["statistic"].apply(lambda x: float(x["apr"]["long"]))
            json_data["apr_short"] = json_data["statistic"].apply(lambda x: float(x["apr"]["short"]))
            json_data.drop(["statistic", "prices"], axis=1, inplace=True)
            json_data["index"] = now.strftime("%m/%d/%Y %H:%M:%S")
            json_data.set_index("index", inplace=True)

            # these are mirror protocol spreads every minute
            mycol = db["mirrorSpreads"]
            for i in np.arange(len(json_data)):
                ticker = json_data.iloc[i]["symbol"]
                value = json_data.iloc[i]["price"] / json_data.iloc[i]["oralceprice"] - 1
                mydict = {"date": now, "ticker": ticker, "value": value}
                mycol.insert_one(mydict)

            """insert new aprs every hour for app"""
            if dt.datetime.now().minute == 0:

                # these are mirror protocol aprs, on terraswap only
                mycol = db["HistoricalLongAPRs"]
                for i in np.arange(len(json_data)):
                    ticker = json_data.iloc[i]["symbol"]
                    apr = json_data.iloc[i]["apr"]
                    mydict = {"date": now, "ticker": ticker, "apr": apr}

                    mycol.insert_one(mydict)
                # these are mirror protocol short aprs, on terraswap only
                mycol = db["HistoricalShortAPRs"]
                for i in np.arange(len(json_data)):
                    ticker = json_data.iloc[i]["symbol"]
                    apr = json_data.iloc[i]["apr_short"]
                    mydict = {"date": now, "ticker": ticker, "apr": apr}
                    mycol.insert_one(mydict)

                """collect UST marketCap every hour"""
                try:
                    supply = int(requests.get("https://fcd.terra.dev/v1/circulatingsupply/uusd").json()) / 1000000
                    mycol = db["ustMC"]
                    ticker = "ust_circulating_supply"
                    mydict = {"date": now, "ust_circulating_supply": supply}
                    mycol.insert_one(mydict)
                except Exception as e:
                    pass

        except Exception as e:
            print('defiData1 Error', e)
            pass


if __name__ == "__main__":
    schedule.every().minute.at(":00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
