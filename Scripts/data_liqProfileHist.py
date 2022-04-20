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


collateral_dict = {
    "terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun": "bETH",
    "terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp": "bLUNA",
}

## Run every 4 hours
def job():

    buf = PrintPrepender("[Defi Data Liq Profiles Hist]: ")
    with redirect_stdout(buf):
        print("Starting job")
        try:

            """liquidation nodes"""
            summary = terraHelper.get_kujira_summary()
            eastern = timezone("US/Eastern")
            now = dt.datetime.now(eastern)
            summary['ltv'] = summary["loan_amount_with_interest"].astype(float)/summary["deposit_amount_stable"].astype(float)
            summary["Date"] = now
            luna_price_last = terraHelper.coinhall_terra_latest_prices()["terra1m6ywlgn6wrjuagcmmezzz2a029gtldhey5k552"]
            summary.columns = ["borrow_percentage","collateral_value","loan_value", "ltv","Date"]
            summary["loan_value"] = summary["loan_value"].astype(float) / 1000000
            summary["collateral_value"] = summary["collateral_value"].astype(float) / 1000000
            summary["luna_price"] = luna_price_last
            summary["percent_of_loans"] = (summary["loan_value"] / summary["loan_value"].sum()) * 100
            summary = summary[summary["borrow_percentage"] <= .99]

            summary["luna_liq_level"] = summary["loan_value"] / 0.8 / summary["collateral_value"] * summary["luna_price"]

            summary["bigrisk"] = summary.sort_values(by="loan_value").iloc[-1]["luna_liq_level"]
            summary["areatowatch"] = summary.sort_values(by="loan_value").iloc[-2]["luna_liq_level"]

            summary.rename(
                columns={"luna_liq_level": "Luna_Liquidation_Price", "loan_value": "Loan_Value"}, inplace=True
            )

            summary = summary.sort_values(by="Luna_Liquidation_Price")



            summary["Luna_Liquidation_Price"] = summary['Luna_Liquidation_Price'].astype(int)
            mongo_summary = pd.DataFrame(summary.groupby("Luna_Liquidation_Price")["Loan_Value"].sum().sort_index())
            mongo_summary['collateral_value'] =  summary.groupby("Luna_Liquidation_Price")["collateral_value"].sum().sort_index()
            mongo_summary['ltv'] = mongo_summary["Loan_Value"].astype(float)/mongo_summary["collateral_value"].astype(float)
            mongo_summary["percent_of_loans"] =  (mongo_summary["Loan_Value"] / summary["Loan_Value"].sum()) * 100
            mongo_summary = mongo_summary.reset_index()
            mongo_summary["bigrisk"] = mongo_summary.sort_values(by="Loan_Value").iloc[-1]["Luna_Liquidation_Price"]
            mongo_summary["areatowatch"] = mongo_summary.sort_values(by="Loan_Value").iloc[-2]["Luna_Liquidation_Price"]
            mongo_summary["luna_price"] = luna_price_last
            mongo_summary['Date'] =  now
            mongo_summary = mongo_summary.sort_values(by="Luna_Liquidation_Price")
            mongo_summary["Luna_Liquidation_Price"] = mongo_summary['Luna_Liquidation_Price'].astype(float)


            mycol = db["historicalLiqProfiles"]
            mycol.insert_many(mongo_summary.to_dict("records"))



        except Exception as e:
            print("Defi Data Liq Profiles Hist Error", e)
            pass
