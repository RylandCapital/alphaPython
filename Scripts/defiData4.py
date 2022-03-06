import datetime as dt
import schedule
import time
import pymongo
import pandas as pd
import requests
import numpy as np
import os
from pytz import timezone

from terrahelper import terraHelper

from contextlib import redirect_stdout
from printPrepender import PrintPrepender

from dotenv import load_dotenv

load_dotenv()

ALPHADEFI_MONGO = os.getenv("ALPHADEFI_MONGO")

'''this data collection script collects data on the currently available @terra_money
staking products like, standard staking, Stader LunaX, Nexus nLUNA, etc'''



"""define database"""
client = pymongo.MongoClient(ALPHADEFI_MONGO, ssl=True, ssl_cert_reqs="CERT_NONE")
db = client.alphaDefi

def job():
    buf = PrintPrepender("[Defi Data 4]: ")
    with redirect_stdout(buf):
        try:
            print("Starting DefiData4 job")
            '''define date'''
            eastern = timezone("US/Eastern")
            now = dt.datetime.now(eastern)
            today = now.strftime("%Y-%m-%d")
            tomorrow = (now + dt.timedelta(days=1)).strftime("%Y-%m-%d")

            '''collect data'''
            df = pd.DataFrame([], columns=[
            'Legacy_Staking_TerraStation_APR',
            'Legacy_Staking_TerraStation_Value_Date',
            'Stader_LunaX_Exrate',
            'Stader_LunaX_Exrate_100k_Blocks_Ago',
            'Stader_LunaX_APR',
            'Nexus_nLuna_APR',
            'Prism_yLuna_APR',
            'Anchor_bLuna_APR',
            'Lido_stLuna_APR']
            )

            df.loc[now, 'Legacy_Staking_TerraStation_APR'] = float(
                requests.get(
                "https://fcd.terra.dev/v1/dashboard/staking_return"
                )
                .json()[-1]['annualizedReturn']
            )

            df.loc[now, 'Legacy_Staking_TerraStation_Value_Date'] = dt.datetime.utcfromtimestamp(
                requests.get(
                "https://fcd.terra.dev/v1/dashboard/staking_return"
                )
                .json()[-1]['datetime'] / 1000
            )

            df.loc[now, 'Stader_LunaX_Exrate'] = float(
                requests.get(
                "https://fcd.terra.dev/terra/wasm/v1beta1/contracts/terra1xacqx447msqp46qmv8k2sq6v5jh9fdj37az898/store?query_msg=eyJzdGF0ZSI6e319"
                )
                .json()['query_result']['state']['exchange_rate']
            )

            #get current and past block
            block = terraHelper.terra_current_block()-100000
            blockpast = block-100000

            blockpast_date = pd.to_datetime(
                    terraHelper.getBlockTimestamp(
                    height=blockpast
                    )
                ).to_pydatetime()
            days_between_blocks = (now - blockpast_date).total_seconds()/(
                            24 * 60 * 60)

            df.loc[now, 'Stader_LunaX_Exrate_100k_Blocks_Ago'] = float(
                requests.get(
                "https://fcd.terra.dev/terra/wasm/v1beta1/contracts/terra1xacqx447msqp46qmv8k2sq6v5jh9fdj37az898/store?query_msg=eyJzdGF0ZSI6e319&height={0}".format(
                    blockpast
                )
                )
                .json()['query_result']['state']['exchange_rate']
            )

            df['Stader_LunaX_APR'] = (
                df['Stader_LunaX_Exrate']/df['Stader_LunaX_Exrate_100k_Blocks_Ago']
                )**(365/days_between_blocks)-1
            
            df.loc[now, 'Nexus_nLuna_APR'] = float(
                requests.get(
                "https://api.alphadefi.fund/historical/nexus/bLunaVaultApr?from={0}&to={1}".format(
                    today,
                    tomorrow
                )
                )
                .json()[0]['value']
            )/100

            df.loc[now, 'Prism_yLuna_APR'] = float(
                requests.get(
                "https://api.extraterrestrial.money/v1/api/prism/yluna_apr"
                ).json()
            )

            df.loc[now, 'Anchor_bLuna_APR'] = float(
                requests.get(
                "https://terra.lido.fi/api/stats"
                ).json()['bluna']
            )/100

            df.loc[now, 'Lido_stLuna_APR'] = float(
                requests.get(
                "https://terra.lido.fi/api/stats"
                ).json()['stluna']
            )/100

            df.reset_index(inplace=True)
            df.rename(columns={
                'index':'collected_date'
            }, inplace=True)

            stakedict = df.to_dict('records')[0]
            
            collection = db.liquidStaking
            collection.create_index("collected_date", unique=True)
            collection.insert_one(stakedict)

        except Exception as e:
            print(e)


        














