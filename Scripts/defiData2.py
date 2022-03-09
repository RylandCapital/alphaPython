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
client = pymongo.MongoClient(ALPHADEFI_MONGO, ssl=True, ssl_cert_reqs="CERT_NONE")
db = client.alphaDefi


collateral_dict = {
    "terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun": "bETH",
    "terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp": "bLUNA",
}


def job():

    buf = PrintPrepender("[Defi Data 2]: ")
    with redirect_stdout(buf):
        print("Starting DefiData2 job")
        try:

            """liquidation nodes"""
            summary = terraHelper.get_kujira_summary()
            eastern = timezone("US/Eastern")
            now = dt.datetime.now(eastern)
            summary["Date"] = now
            luna_price_last = terraHelper.coinhall_terra_latest_prices()["terra1m6ywlgn6wrjuagcmmezzz2a029gtldhey5k552"]
            summary.columns = ["collateral_value", "loan_value", "ltv", "Date"]
            summary["loan_value"] = summary["loan_value"].astype(float) / 1000000
            summary["collateral_value"] = summary["collateral_value"].astype(float) / 1000000
            summary["luna_price"] = luna_price_last
            summary["percent_of_loans"] = (summary["loan_value"] / summary["loan_value"].sum()) * 100
            summary = summary[summary["ltv"] <= 0.8]

            summary["luna_liq_level"] = ((summary["loan_value"] / 0.8) / summary["collateral_value"]) * summary[
                "luna_price"
            ]

            summary["bigrisk"] = summary.sort_values(by="loan_value").iloc[-1]["luna_liq_level"]
            summary["areatowatch"] = summary.sort_values(by="loan_value").iloc[-2]["luna_liq_level"]

            summary.rename(
                columns={"luna_liq_level": "Luna_Liquidation_Price", "loan_value": "Loan_Value"}, inplace=True
            )

            """update current liquidation profile live (1min)"""
            collection = db.liqprofile
            collection.drop()
            collection.insert_many(summary[["Date", "Luna_Liquidation_Price", "Loan_Value"]].to_dict("records"))
            """update liquidation stats grid live (1min)"""
            collection = db.liqprofileSTATS
            collection.drop()
            collection.insert_many(summary.to_dict("records"))

            """update historical liq profiles every hour on 30 min"""
            if dt.datetime.now().minute == 30:
                mycol = db["historicalLiqProfiles"]
                mycol.insert_many(summary.to_dict("records"))

            """master apr data and farmers market grid"""
            if dt.datetime.now().minute == 0:
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

                """drops and replaces farmers market gird data"""
                collection = db.aprCompare
                collection.drop()
                collection.insert_many(final_grid.to_dict(orient="records"))

                """appends historical data with newest timestamped data"""
                collection = db.aprs
                aprs["timestamp"] = pd.to_datetime(aprs["timestamp"])
                collection.insert_many(aprs.to_dict(orient="records"))

        except Exception as e:
            print('defiData2 Error', e)
            pass


# %%
if __name__ == "__main__":
    schedule.every().minute.at(":00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
