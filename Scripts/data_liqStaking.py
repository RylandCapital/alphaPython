import datetime as dt
import schedule
import time
import pymongo
import pandas as pd
import requests
import numpy as np
import os
from pytz import timezone
import json

from terrahelper import terraHelper

from contextlib import redirect_stdout
from printPrepender import PrintPrepender

from dotenv import load_dotenv

load_dotenv()

ALPHADEFI_MONGO = os.getenv("ALPHADEFI_MONGO")

"""this data collection script collects data on the currently available @terra_money
staking products like, standard staking, Stader LunaX, Nexus nLUNA, etc"""


"""define database"""
client = pymongo.MongoClient(ALPHADEFI_MONGO,
ssl=True,
ssl_cert_reqs='CERT_NONE')
db = client.alphaDefi


def job():
    buf = PrintPrepender("[Defi Liq Staking]: ")
    with redirect_stdout(buf):
        try:
            print("Starting job")
            """define date"""
            eastern = timezone("US/Eastern")
            now = dt.datetime.now(eastern)
            today = (now - dt.timedelta(days=1)).strftime("%Y-%m-%d")
            tomorrow = (now + dt.timedelta(days=1)).strftime("%Y-%m-%d")

            """collect data"""
            df = pd.DataFrame(
                [],
                columns=[
                    "Legacy_Staking_TerraStation_APR",
                    "Legacy_Staking_TerraStation_Value_Date",
                    "Stader_LunaX_Exrate",
                    "Nexus_nLuna_APR",
                    "Prism_yLuna_APR",
                    "Anchor_bLuna_APR",
                    "Lido_stLuna_APR",
                ],
            )

            df.loc[now, "Legacy_Staking_TerraStation_APR"] = float(
                requests.get("https://fcd.terra.dev/v1/dashboard/staking_return").json()[-1]["annualizedReturn"]
            )

            df.loc[now, "Legacy_Staking_TerraStation_Value_Date"] = dt.datetime.utcfromtimestamp(
                requests.get("https://fcd.terra.dev/v1/dashboard/staking_return").json()[-1]["datetime"] / 1000
            )

            #continue logging Stader price
            df.loc[now, "Stader_LunaX_Exrate"] = float(
                requests.get(
                    "https://fcd.terra.dev/terra/wasm/v1beta1/contracts/terra1xacqx447msqp46qmv8k2sq6v5jh9fdj37az898/store?query_msg=eyJzdGF0ZSI6e319"
                ).json()["query_result"]["state"]["exchange_rate"]
            )

            #pull historical from our API
            sdf = pd.DataFrame(requests.get('https://api.alphadefi.fund/historical/liquidstaking/Stader_LunaX_Exrate/?precision=day').json())
            sdf['Stader_LunaX_APR'] = (
                sdf['value']/sdf['value'].shift(7)
                )**(365/7)-1

            #create APR 7-Day from prices
            df['Stader_LunaX_APR'] = float(sdf['Stader_LunaX_APR'].iloc[-1])

            url = "https://api.nexusprotocol.app/graphql"

            query = """ {
                getBAssetVaultAprRecords(limit: 1, offset: 0) {
                    date
                    bEthVaultApr
                    bEthVaultManualApr
                    bLunaVaultApr
                    bLunaVaultManualApr
                }
                }"""
            r = requests.post(url, json={"query": query})
            hd = json.loads(r.text)["data"]["getBAssetVaultAprRecords"]

            df.loc[now, "Nexus_nLuna_APR"] = float(hd[0]["bLunaVaultApr"]) / 100

            df.loc[now, "Prism_yLuna_APR"] = float(
                requests.get("https://api.extraterrestrial.money/v1/api/prism/yluna_apr").json()
            )

            df.loc[now, "Anchor_bLuna_APR"] = (
                float(requests.get("https://terra.lido.fi/api/stats").json()["bluna"]) / 100
            )

            df.loc[now, "Lido_stLuna_APR"] = (
                float(requests.get("https://terra.lido.fi/api/stats").json()["stluna"]) / 100
            )

            # make dictionary for drop down options on app.
            blankdict = {}
            names = list(df.to_dict("records")[0].keys())
            names.remove("Legacy_Staking_TerraStation_Value_Date")
            names.remove("Stader_LunaX_Exrate")
            for i in names:
                blankdict[i] = i
            liqDict = {"token": blankdict}

            collection = db.liqStakingDict
            collection.drop()
            time.sleep(2)
            collection.insert_one(liqDict)

            temps = []
            for i in df.columns:
                temp = pd.DataFrame([], columns=["date", "ticker", "value"])
                temp.loc[0, "ticker"] = i
                temp.loc[0, "value"] = df[[i]].copy().iloc[0, 0]
                temp.loc[0, "date"] = df[[i]].copy().index[0]
                temps.append(temp)

            df = pd.concat(temps)

            stakedict = df.to_dict("records")

            collection = db.liquidStaking
            collection.insert_many(stakedict)

        except Exception as e:
            print("Defi Liq Staking", e)
