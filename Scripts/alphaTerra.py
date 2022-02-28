import pandas as pd
import numpy as np
import requests
import json
from os import listdir
from datetime import timedelta
import datetime as dt
from terrahelper import terraHelper
import time
import logging
import re
import math
import os


from dotenv import load_dotenv

load_dotenv()

EOD_API = os.getenv("EOD_API")


class alphaTerra(object):

    """tools for alphaDefi management"""

    def __init__(self):
        pass

    def pullMirrorHistory(self, start_date="2021-04-29", end_date=None):

        if end_date == None:
            end_date = (dt.datetime.now() + dt.timedelta(days=1)).strftime("%Y-%m-%d")

        mirror_tokens = list(requests.get("https://api.alphadefi.fund/info/tokendict").json()[0]["token"].keys())

        longs = []
        shorts = []
        spreads = []
        for tk in mirror_tokens:

            longAprs = (
                pd.DataFrame(
                    requests.get(
                        "https://api.alphadefi.fund/historical/mirror/longaprs/{0}/?to={1}&from={2}".format(
                            tk, end_date, start_date
                        )
                    ).json()
                )
                .drop("_id", axis=1)
                .set_index("date")
            )
            longAprs.index = pd.to_datetime(longAprs.index)
            shortAprs = (
                pd.DataFrame(
                    requests.get(
                        "https://api.alphadefi.fund/historical/mirror/shortaprs/{0}/?to={1}&from={2}".format(
                            tk, end_date, start_date
                        )
                    ).json()
                )
                .drop("_id", axis=1)
                .set_index("date")
            )
            shortAprs.index = pd.to_datetime(shortAprs.index)
            spread = (
                pd.DataFrame(
                    requests.get(
                        "https://api.alphadefi.fund/historical/mirror/spreads/{0}/?to={1}&from={2}&precision=day".format(
                            tk, end_date, start_date
                        )
                    ).json()
                )
                .drop("_id", axis=1)
                .set_index("date")
            )
            spread.index = pd.to_datetime(spread.index)
            spread["day"] = spread.index.date
            longs.append(longAprs)
            shorts.append(shortAprs)
            spreads.append(spread)

        return pd.concat(longs), pd.concat(shorts), pd.concat(spreads)

    def resampleOHLC(self, df, column, resample="30Min"):

        return df.groupby("symbol")[column].resample(resample).ohlc().interpolate(method="linear")

    # 4/29/21 is the first day Mirror APR/ Spread Data Begins
    def mirrorSpreadStats(self, days_back):

        start_date = (dt.datetime.now() - dt.timedelta(days_back)).strftime("%m/%d/%y")
        end_date = (dt.datetime.now() + timedelta(days=1)).strftime("%m-%d-%Y")
        df = self.pullMirrorHistory(start_date, end_date)[2]
        df.rename(columns={"ticker": "symbol", "value": "spread"}, inplace=True)

        statsy = df.groupby("symbol")["spread"].describe()[["mean", "std", "min", "max"]]

        statsy.columns = ["mean", "std", "min", "max"]

        return df, statsy

    def pullMirrorAssets(self):

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
                    token
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
        r = requests.post(url, json={"query": query})
        now = dt.datetime.now()
        json_data = pd.DataFrame(json.loads(r.text)["data"]["assets"])
        json_data["price"] = json_data["prices"].apply(lambda x: float(x["price"]))
        json_data["oralceprice"] = json_data["prices"].apply(
            lambda x: x["oraclePrice"] if x["oraclePrice"] == None else float(x["oraclePrice"])
        )
        json_data["liquidity"] = json_data["statistic"].apply(lambda x: float(x["liquidity"]))
        json_data["volume"] = json_data["statistic"].apply(lambda x: float(x["volume"]))
        json_data["apr"] = json_data["statistic"].apply(lambda x: float(x["apr"]["long"]))
        json_data.drop(["statistic", "prices"], axis=1, inplace=True)
        json_data["index"] = now.strftime("%m/%d/%Y %H:%M:%S")
        json_data.set_index("index", inplace=True)

        json_data["spread"] = (json_data["price"] - json_data["oralceprice"]) / json_data["oralceprice"]

        return json_data

    def getBlockTimestamp(self, height=""):

        value = pd.to_datetime(
            requests.get("https://lcd.terra.dev/blocks/{1}".replace("{1}", str(height))).json()["block"]["header"][
                "time"
            ]
        )
        return value

    def masterAPR(self):
        def decimals(x):
            try:
                return x["decimals"]
            except:
                pass

        latest = terraHelper.coinhall_terra_latest_prices()

        def getLatest(x):
            try:
                return latest[x]
            except:
                pass

        fees = {"Terraswap": 0.003, "Astroport": 0.002, "TerraFloki": 0.003, "Loop": 0.003, "Terraformer": 0.003}

        stableswaps = ["terra1j66jatn3k50hjtg2xemnjm8s7y8dws9xqa5y8w"]

        """bluna luna addy ts terra1jxazgm67et0ce260kvrpfv50acuushpjsz2y0p"""

        """bring in coinhall drop (has LP, token0, token1 addys)"""
        info = terraHelper.coinhall_terra_info()
        """need luna prices in all denoms"""
        luna_rates = terraHelper.terra_stables()
        luna_rates.loc["uluna", "exrate"] = 1 / luna_rates.loc["uusd"]["lunaRate"]

        df = pd.DataFrame.from_dict(info).T.iloc[:, :-4]

        def verify(x):
            try:
                return x["verified"]
            except:
                pass

        """Terraswap, Astrport, and Loop Only"""
        df = df[df["dex"].isin(["Terraswap", "Astroport", "Loop"])]

        df["verified0"] = df["asset0"].apply(lambda x: verify(x))
        df["verified1"] = df["asset1"].apply(lambda x: verify(x))
        df = df[(df["verified0"] == 1) & (df["verified1"] == True)]

        """get decimals"""
        df["decimals0"] = df["asset0"].apply(lambda x: decimals(x)).fillna(6)
        df["decimals1"] = df["asset1"].apply(lambda x: decimals(x)).fillna(6)
        df["decimalsMain"] = np.where(df["decimals0"] > df["decimals1"], df["decimals0"], df["decimals1"])

        df["decimalTest"] = df["decimals0"] == df["decimals1"]

        """get contract addresses"""
        df["addy0"] = df["asset0"].apply(lambda x: x["contractAddress"])
        df["addy1"] = df["asset1"].apply(lambda x: x["contractAddress"])

        """get symbols in pool"""
        df["symbol0"] = df["asset0"].apply(lambda x: x["symbol"])
        df["symbol1"] = df["asset1"].apply(lambda x: x["symbol"])

        """get amounts in pool"""
        df["amount0"] = df["asset0"].apply(lambda x: x["poolAmount"]).astype(float) / (10 ** df["decimals0"])
        df["amount1"] = df["asset1"].apply(lambda x: x["poolAmount"]).astype(float) / (10 ** df["decimals1"])

        """get volumes24 in pool"""
        df["volume24h0"] = df["asset0"].apply(lambda x: x["volume24h"]).astype(float) / (10 ** df["decimals0"])
        df["volume24h1"] = df["asset1"].apply(lambda x: x["volume24h"]).astype(float) / (10 ** df["decimals1"])

        """get volumes7 in pool"""
        df["volume7d0"] = df["asset0"].apply(lambda x: x["volume7d"]).astype(float) / (10 ** df["decimals0"])
        df["volume7d1"] = df["asset1"].apply(lambda x: x["volume7d"]).astype(float) / (10 ** df["decimals1"])

        """get dex swaprates"""
        df["swaprate"] = df["dex"].apply(lambda x: fees[x])

        """get prices"""
        df = df.reset_index().rename(columns={"index": "poolAddy"})
        df["poolPrice"] = df.reset_index()["poolAddy"].apply(lambda x: getLatest(x))
        df["poolPrice"] = df["poolPrice"].fillna(0)
        df.set_index("poolAddy", inplace=True)

        """clean up"""
        df = df[(df["volume24h0"] > 0) & (df["amount0"] > 0) & (df["amount1"] > 0)]

        """denominator test"""
        df["denomTest0"] = ((df["amount0"] / df["amount1"]) - df["poolPrice"]).abs()
        df["denomTest1"] = ((df["amount1"] / df["amount0"]) - df["poolPrice"]).abs()

        """get_denominator"""
        df["denom"] = np.where(df["denomTest0"] < df["denomTest1"], "asset0", "asset1")
        df["denom_symbol"] = np.where(df["denom"] == "asset1", df["symbol1"], df["symbol0"])
        df["numer_symbol"] = np.where(df["denom"] == "asset1", df["symbol0"], df["symbol1"])
        df["denom_addy"] = np.where(df["denom"] == "asset1", df["addy1"], df["addy0"])

        """calculate total liquidity in USD"""

        def convert_ust(x):
            try:
                return luna_rates.loc[x]["exrate"]
            except:
                pass

        ####################################################################
        """calculate ust liquiity"""
        df["liquidity"] = np.where(
            df["denom"] == "asset0", df["amount1"] * 2 * df["poolPrice"], df["amount0"] * 2 * df["poolPrice"]
        )

        df["ust_divisor"] = df["denom_addy"].apply(lambda x: convert_ust(x))
        df["ustLiquidity"] = df["liquidity"] / df["ust_divisor"]
        df["ustLiquidity"] = df["ustLiquidity"].fillna(1000000000000000)

        # need denom-ust prices for pairs with weirder denoms
        nodenoms = df[df["ustLiquidity"] == 1000000000000000]["denom_symbol"].tolist()
        nodenom_prices = df[df["symbol0"].isin(nodenoms) | df["symbol1"].isin(nodenoms)]
        nodenom_prices = nodenom_prices[(nodenom_prices["amount0"] > 0) & (nodenom_prices["amount1"] > 0)]
        nodenom_prices = nodenom_prices[nodenom_prices["denom_symbol"] == "UST"]
        nodenom_prices = nodenom_prices.drop_duplicates("numer_symbol", keep="last").set_index("numer_symbol")

        def fillNodenoms(x):

            try:

                return nodenom_prices.loc[x]["poolPrice"]
            except:
                pass

        df = df.reset_index()
        df["ustMultiplier"] = df["denom_symbol"].apply(lambda x: fillNodenoms(x))
        df["ustLiquidity"] = np.where(
            df["ustLiquidity"] == 1000000000000000, df["liquidity"] * df["ustMultiplier"], df["ustLiquidity"]
        )
        df["ustLiquidity"] = df["ustLiquidity"].astype(float)

        ####################################################################
        """get poolVolume in UST"""

        df["poolVolume7d"] = np.where(
            df["denom"] == "asset0", df["volume7d1"] * df["poolPrice"], df["volume7d0"] * df["poolPrice"]
        )

        df["ustPoolVolume7d"] = df["poolVolume7d"] / df["ust_divisor"]
        df["ustPoolVolume7d"] = df["ustPoolVolume7d"].fillna(1000000000000000)

        # adjust for no divisors
        df["ustPoolVolume7d"] = np.where(
            df["ustPoolVolume7d"] == 1000000000000000, df["poolVolume7d"] * df["ustMultiplier"], df["ustPoolVolume7d"]
        )

        ####################################################################
        """calculate APRs"""
        df["isStableSwap"] = np.where(df["poolAddy"].isin(stableswaps), True, False)
        df["apr7d"] = np.where(
            df["isStableSwap"] == False,
            df["swaprate"] * df["ustPoolVolume7d"] / df["ustLiquidity"] * 52,
            0.00025 * df["ustPoolVolume7d"] / df["ustLiquidity"] * 52,
        )

        final = df[
            [
                "dex",
                "decimals0",
                "symbol0",
                "symbol1",
                "poolPrice",
                "liquidity",
                "ustLiquidity",
                "volume7d0",
                "volume7d1",
                "poolVolume7d",
                "ustPoolVolume7d",
                "ust_divisor",
                "ustMultiplier",
                "apr7d",
            ]
        ]
        final = final.dropna(subset=["apr7d"])

        final["symbol_sort"] = (
            final["symbol0"].apply(lambda x: [x.lower()]) + final["symbol1"].apply(lambda x: [x.lower()])
        ).apply(lambda x: sorted(x))
        final["masterSymbol"] = final["symbol_sort"].apply(lambda x: x[0] + "-" + x[1])
        final["timestamp"] = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")

        final2 = final.copy()

        final2.drop(
            [
                "ust_divisor",
                "ustMultiplier",
                "symbol_sort",
            ],
            axis=1,
            inplace=True,
        )

        """create grid and calculate volume dominance"""
        ts = final[final["dex"] == "Terraswap"][
            [
                "dex",
                "masterSymbol",
                "symbol0",
                "symbol1",
                "ustPoolVolume7d",
                "apr7d",
            ]
        ]
        ts.set_index("masterSymbol", inplace=True)
        astro = final[final["dex"] == "Astroport"][
            ["dex", "masterSymbol", "symbol0", "symbol1", "ustPoolVolume7d", "apr7d"]
        ]
        astro.set_index("masterSymbol", inplace=True)
        loop = final[final["dex"] == "Loop"][["dex", "masterSymbol", "symbol0", "symbol1", "ustPoolVolume7d", "apr7d"]]
        loop.set_index("masterSymbol", inplace=True)

        grid = pd.concat([ts, astro, loop]).sort_index().reset_index()
        ids = grid["masterSymbol"]
        grid = grid[ids.isin(ids[ids.duplicated()])].sort_values("masterSymbol").set_index("masterSymbol")

        grids = []
        for i, l in zip(grid.index.unique(), np.arange(len(grid.index.unique()))):
            dup_check = grid.loc[i]["dex"].value_counts().max()

            if dup_check < 2:
                row = grid.loc[i].T.loc[["dex", "apr7d", "ustPoolVolume7d"]]
                sym = row.columns[0]
                row.columns = row.loc["dex"].values
                row.drop("dex", axis=0, inplace=True)

                try:
                    row.loc["apr7d", "Astroport Volume Dominance"] = (
                        row.loc["ustPoolVolume7d", "Astroport"] / row.loc["ustPoolVolume7d"].sum()
                    )
                except:
                    print("no astroport")

                try:
                    row.loc["apr7d", "Terraswap Volume Dominance"] = (
                        row.loc["ustPoolVolume7d", "Terraswap"] / row.loc["ustPoolVolume7d"].sum()
                    )
                except:
                    print("no terraswap")

                try:
                    row.loc["apr7d", "Loop Volume Dominance"] = (
                        row.loc["ustPoolVolume7d", "Loop"] / row.loc["ustPoolVolume7d"].sum()
                    )
                except:
                    print("no loop")

                row.loc[:, "Symbol"] = sym

                grids.append(pd.DataFrame(row.iloc[0]).T)

        final_grid = pd.concat(grids)
        final_grid = final_grid[list(final_grid.columns.drop("Symbol")) + ["Symbol"]]
        final_grid.iloc[:, :-1] = (final_grid.iloc[:, :-1].astype(float) * 100).round(2)
        final_grid = final_grid.reset_index(drop=True)

        final_grid["timestamp"] = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")

        return [final_grid, final2]

    def anchorAPY(self):

        today = (dt.datetime.now() + dt.timedelta(days=1)).strftime("%Y-%m-%d")

        anchor_data = []

        url = "https://api.anchorprotocol.com/api/v1/market/ust/1d"
        r = requests.get(url)

        df = pd.DataFrame([], columns=["timestamp", "height", "deposit_apr"])
        for i in r.json():

            height = i["height"]
            df.loc[height, "timestamp"] = i["timestamp"]
            df.loc[height, "height"] = i["height"]
            df.loc[height, "deposit_apr"] = i["deposit_apy"]

        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.utcfromtimestamp(x / 1000))
        df.rename(columns={"deposit_apr": "apr"}, inplace=True)
        df.drop("height", axis=1, inplace=True)
        df["ticker"] = "depositAPR"
        df["apr"] = df["apr"].astype(float)

        anchor_data.append(df)

        url = "https://api.anchorprotocol.com/api/v1/market/ust/1d"
        r = requests.get(url)

        url2 = "https://api.anchorprotocol.com/api/v1/anc/1d"
        r2 = requests.get(url2)

        df = pd.DataFrame([], columns=["timestamp", "height", "total_liabilities", "anc", "anc_emission_rate"])
        for i, i2 in zip(r.json(), r2.json()):

            height = i["height"]
            df.loc[height, "timestamp"] = i["timestamp"]
            df.loc[height, "height"] = i["height"]
            df.loc[height, "total_liabilities"] = i["total_liabilities"]
            df.loc[height, "anc_emission_rate"] = i["anc_emission_rate"]
            df.loc[height, "anc"] = i2["anc_price"]

        df["distribution_apy"] = (
            df["anc_emission_rate"].astype(float)
            * df["anc"].astype(float)
            * 4656810
            / df["total_liabilities"].astype(float)
        )
        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.utcfromtimestamp(x / 1000))

        df.drop("height", axis=1, inplace=True)
        distributiondf = df.copy()

        df.rename(columns={"distribution_apy": "apr"}, inplace=True)
        df["ticker"] = "distributionAPR"

        anchor_data.append(df[["timestamp", "ticker", "apr"]])

        url = "https://api.anchorprotocol.com/api/v1/market/ust/1d"
        r = requests.get(url)

        df = pd.DataFrame([], columns=["timestamp", "height", "borrow_rate"])
        for i in r.json():

            height = i["height"]
            df.loc[height, "timestamp"] = i["timestamp"]
            df.loc[height, "height"] = i["height"]
            df.loc[height, "borrow_rate"] = i["borrow_rate"]

        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.utcfromtimestamp(x / 1000))
        df["borrow_rate"] = df["borrow_rate"].astype(float) * 4656810

        df.drop("height", axis=1, inplace=True)
        borrowdf = df.copy()

        df.rename(columns={"borrow_rate": "apr"}, inplace=True)
        df["ticker"] = "borrowAPR"
        df["apr"] = df["apr"].astype(float)

        anchor_data.append(df)

        #############################

        distributiondf.set_index("timestamp", inplace=True)

        borrowdf.set_index("timestamp", inplace=True)

        final = borrowdf.join(distributiondf.drop("total_liabilities", axis=1)).dropna()
        final["net_apr"] = final["distribution_apy"] - final["borrow_rate"]

        final.drop("borrow_rate", axis=1, inplace=True)
        final.rename(columns={"net_apr": "apr"}, inplace=True)

        final["ticker"] = "netApr"

        anchor_data.append(final.reset_index()[["timestamp", "ticker", "apr"]])

        ###############################
        """Anchor bLUNA Supply"""
        df = pd.DataFrame([], columns=["height", "total_supply"])

        curr_block = terraHelper.terra_current_block()

        for i in np.arange(curr_block - 500000, curr_block, 10000):

            count = True
            while count == True:
                try:
                    time.sleep(1)

                    req = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp/store?query_msg=%7B%22token_info%22:%7B%7D%7D"
                    )

                    amount_current = float(json.loads(req.text)["result"]["total_supply"][:-6])

                    req = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp/store?query_msg=%7B%22token_info%22:%7B%7D%7D&height={0}".format(
                            i
                        )
                    )

                    height = json.loads(req.text)["height"]
                    ts = float(json.loads(req.text)["result"]["total_supply"][:-6])

                    req2 = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp/store?query_msg=%7B%22token_info%22:%7B%7D%7D"
                    )

                    amount_current2 = float(json.loads(req2.text)["result"]["total_supply"][:-6])

                except:
                    print("there is an error")
                    pass

                if (ts != amount_current) & (amount_current == amount_current2):
                    print("logic met, saving data")
                    count = False

                    df.loc[height, "height"] = height
                    df.loc[height, "total_supply"] = ts

        epoch = pd.to_datetime(dt.datetime.utcfromtimestamp(0), utc=True)
        df["date"] = df["height"].apply(lambda x: int((self.getBlockTimestamp(x) - epoch).total_seconds() * 1000))

        df.rename(columns={"total_supply": "apr"}, inplace=True)
        df["timestamp"] = df["date"].apply(lambda x: dt.datetime.utcfromtimestamp(x // 1000))
        df["ticker"] = "bLunaSupply"
        df.drop(["date", "height"], axis=1, inplace=True)

        anchor_data.append(df)

        ################################
        """Anchor bETH Supply"""
        df = pd.DataFrame([], columns=["height", "total_supply"])

        curr_block = terraHelper.terra_current_block()

        for i in np.arange(curr_block - 500000, curr_block, 10000):

            count = True
            while count == True:
                try:
                    time.sleep(1)

                    req = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun/store?query_msg=%7B%22token_info%22:%7B%7D%7D"
                    )

                    amount_current = float(json.loads(req.text)["result"]["total_supply"][:-6])

                    req = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun/store?query_msg=%7B%22token_info%22:%7B%7D%7D&height={0}".format(
                            i
                        )
                    )

                    height = json.loads(req.text)["height"]
                    ts = float(json.loads(req.text)["result"]["total_supply"][:-6])

                    req2 = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun/store?query_msg=%7B%22token_info%22:%7B%7D%7D"
                    )

                    amount_current2 = float(json.loads(req2.text)["result"]["total_supply"][:-6])

                except:
                    print("there is an error")
                    pass

                if (ts != amount_current) & (amount_current == amount_current2):
                    print("logic met, saving data")
                    count = False

                    df.loc[height, "height"] = height
                    df.loc[height, "total_supply"] = ts

        epoch = pd.to_datetime(dt.datetime.utcfromtimestamp(0), utc=True)
        df["date"] = df["height"].apply(lambda x: int((self.getBlockTimestamp(x) - epoch).total_seconds() * 1000))

        df.rename(columns={"total_supply": "apr"}, inplace=True)
        df["timestamp"] = df["date"].apply(lambda x: dt.datetime.utcfromtimestamp(x // 1000))
        df["ticker"] = "bETHSupply"
        df.drop(["date", "height"], axis=1, inplace=True)

        anchor_data.append(df)

        """added these calls to get bLuna and bEth Collateral numbers 
        which are different then the total supplies that have above"""

        req = requests.get("https://api.anchorprotocol.com/api/v1/collaterals/1d")
        data = req.json()
        data = pd.DataFrame.from_dict(data)
        data["bluna_collateral"] = data["collaterals"].apply(lambda x: float(x[0]["collateral"]) / 1000000)
        data["beth_collateral"] = data["collaterals"].apply(
            lambda x: (float(x[1]["collateral"]) / 1000000) if len(x) > 1 else 0
        )

        blunadf = data[["timestamp", "bluna_collateral"]]
        blunadf["ticker"] = "blunaCollateral"
        blunadf["timestamp"] = blunadf["timestamp"].apply(lambda x: dt.datetime.utcfromtimestamp(x // 1000))
        blunadf.rename(columns={"bluna_collateral": "apr"}, inplace=True)
        bethdf = data[["timestamp", "beth_collateral"]]
        bethdf["ticker"] = "bethCollateral"
        bethdf["timestamp"] = bethdf["timestamp"].apply(lambda x: dt.datetime.utcfromtimestamp(x // 1000))
        bethdf.rename(columns={"beth_collateral": "apr"}, inplace=True)

        anchor_data.append(blunadf)
        anchor_data.append(bethdf)

        """height = json.loads(r.text)['height']
                ib = float(json.loads(r.text)['result'][
                    'prev_interest_buffer'][:-6])"""

        #################################
        """Anchor Yeild Reserve"""
        df = pd.DataFrame([], columns=["height", "interest_buffer"])

        curr_block = terraHelper.terra_current_block()

        for i in np.arange(curr_block - 500000, curr_block, 10000):

            count = True
            while count == True:
                try:
                    time.sleep(1)

                    req = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8/store?query_msg=%7B%22epoch_state%22:%7B%7D%7D"
                    )

                    amount_current = float(json.loads(req.text)["result"]["prev_interest_buffer"][:-6])

                    req = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8/store?query_msg=%7B%22epoch_state%22:%7B%7D%7D&height={0}".format(
                            i
                        )
                    )

                    height = json.loads(req.text)["height"]
                    ib = float(json.loads(req.text)["result"]["prev_interest_buffer"][:-6])

                    req2 = requests.get(
                        "https://lcd.terra.dev/wasm/contracts/terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8/store?query_msg=%7B%22epoch_state%22:%7B%7D%7D"
                    )

                    amount_current2 = float(json.loads(req2.text)["result"]["prev_interest_buffer"][:-6])

                except:
                    print("there is an error")
                    pass

                if (ib != amount_current) & (amount_current == amount_current2):
                    print("logic met, saving data")
                    count = False

                    df.loc[height, "height"] = height
                    df.loc[height, "interest_buffer"] = ib

        epoch = pd.to_datetime(dt.datetime.utcfromtimestamp(0), utc=True)

        df["date"] = df["height"].apply(lambda x: int((self.getBlockTimestamp(x) - epoch).total_seconds() * 1000))

        df.rename(columns={"interest_buffer": "apr"}, inplace=True)
        df["timestamp"] = df["date"].apply(lambda x: dt.datetime.utcfromtimestamp(x / 1000))
        df["ticker"] = "yieldReserve"
        df.drop(["date", "height"], axis=1, inplace=True)

        anchor_data.append(df)

        anchordata = pd.concat(anchor_data)
        anchordata.rename(columns={"timestamp": "date"}, inplace=True)
        anchordata.rename(columns={"apr": "value"}, inplace=True)
        anchordata["value"] = anchordata["value"].astype(float)

        tickers = anchordata["ticker"].unique().tolist()
        blankdict = {}
        for i in tickers:
            blankdict[i] = i
        anchor_dict = {"token": blankdict}

        return [anchordata, anchor_dict]

    def alphatrackerUpdate(self):

        """Update mSpreads Rolling 30 Day and Symbol Dict for SpreadTracker"""
        raw, ss = self.mirrorSpreadStats(30)
        pcts = (
            pd.DataFrame(raw.groupby("symbol")["spread"].quantile(0.05))
            .rename(columns={"spread": "Historical 5th % Spread"})
            .join(
                pd.DataFrame(raw.groupby("symbol")["spread"].quantile(0.95)).rename(
                    columns={"spread": "Historical 95th % Spread"}
                )
            )
        )
        pcts = pcts.join(ss)
        pcts["Three SD"] = pcts["mean"] + (pcts["std"] * 3)
        pcts["Neg Three SD"] = pcts["mean"] - (pcts["std"] * 3)
        spreads = pcts.round(4)

        token_dict = (
            self.pullMirrorAssets()[["symbol", "token"]]
            .reset_index(drop=True)
            .set_index("symbol")
            .drop("MIR")
            .to_dict()
        )

        return [spreads, token_dict]

    def nexus(self):

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

        df = pd.DataFrame.from_dict(hd)
        temps = []
        for i in df.columns[1:]:
            temp = df[["date", i]]
            temp["ticker"] = i
            temp.rename(columns={i: "value"}, inplace=True)
            temps.append(temp)
        final = pd.concat(temps)

        final["date"] = pd.to_datetime(final["date"])
        final["value"] = final["value"].astype(float)

        tickers = final["ticker"].unique().tolist()
        blankdict = {}
        for i in tickers:
            blankdict[i] = i
        nexusDict = {"token": blankdict}

        return [final, nexusDict]

    def luna_staking(self):

        stakingdfs = []

        url = "https://fcd.terra.dev/v1/dashboard/staking_return"
        r = requests.get(url)

        df = pd.DataFrame([], columns=["timestamp", "daily_return", "annualized_return"])
        for i in r.json():

            timestamp = i["datetime"]
            df.loc[timestamp, "timestamp"] = i["datetime"]
            df.loc[timestamp, "daily_return"] = float(i["dailyReturn"])
            df.loc[timestamp, "annualized_return"] = float(i["annualizedReturn"])

        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.utcfromtimestamp(x / 1000))

        stakingdfs.append(df)

        dfs = []
        url = "https://fcd.terra.dev/v1/dashboard/tx_volume?"
        r = requests.get(url)

        for i in np.arange(len(r.json()["periodic"])):
            df = pd.DataFrame([], columns=["symbol", "timestamp", "transaction_volume"])
            l = r.json()["periodic"][i]
            symbol = l["denom"]
            data = l["data"]
            for d in data:
                timestamp = d["datetime"]
                print(timestamp)
                print(symbol)
                df.loc[timestamp, "timestamp"] = d["datetime"]
                df.loc[timestamp, "transaction_volume"] = float(d["txVolume"]) / 1000000
                df.loc[timestamp, "symbol"] = symbol

            def date_convert(x):
                try:
                    return dt.datetime.utcfromtimestamp(x / 1000)
                except:
                    return x

            df["timestamp"] = df["timestamp"].apply(lambda x: date_convert(x))
            dfs.append(df)

        stakingdfs.append(pd.concat(dfs))

        url = "https://fcd.terra.dev/v1/dashboard/registered_accounts?"
        r = requests.get(url)

        df = pd.DataFrame([], columns=["timestamp", "RegisteredAccounts"])
        for i in r.json()["periodic"]:

            timestamp = i["datetime"]
            df.loc[timestamp, "timestamp"] = i["datetime"]
            df.loc[timestamp, "RegisteredAccounts"] = float(i["value"])

        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.utcfromtimestamp(x / 1000))

        stakingdfs.append(df)

        url = "https://fcd.terra.dev/v1/dashboard/block_rewards?"
        r = requests.get(url)

        df = pd.DataFrame([], columns=["timestamp", "blockReward"])
        for i in r.json()["periodic"]:

            timestamp = i["datetime"]
            df.loc[timestamp, "timestamp"] = i["datetime"]
            df.loc[timestamp, "blockReward"] = i["blockReward"]

        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.utcfromtimestamp(x / 1000))

        stakingdfs.append(df)

        # ML info
        dashboard = pd.concat(
            [
                stakingdfs[0].set_index("timestamp"),
                stakingdfs[1][stakingdfs[1]["symbol"] == "uusd"].set_index("timestamp")["transaction_volume"],
                stakingdfs[2].set_index("timestamp"),
                stakingdfs[3].set_index("timestamp"),
            ],
            axis=1,
        )
        dashboard.index = dashboard.reset_index()["timestamp"].dt.date
        dashboard.columns = [
            "staking_return_daily",
            "staking_return_annualized",
            "daily_transaction_volume",
            "daily_registered_accounts",
            "daily_block_rewards",
        ]
        dashboard["staking_return_daily_rank"] = dashboard["staking_return_daily"].rank(pct=True)
        dashboard["staking_return_annualized_rank"] = dashboard["staking_return_annualized"].rank(pct=True)
        dashboard["daily_transaction_volume_rank"] = dashboard["daily_transaction_volume"].rank(pct=True)
        dashboard["daily_registered_accounts_rank"] = dashboard["daily_registered_accounts"].rank(pct=True)

        """ bring in ustmc via api rather than csv"""
        today = (dt.datetime.now() + dt.timedelta(days=1)).strftime("%Y-%m-%d")

        mc = requests.get("https://api.alphadefi.fund/historical/terra/ustmc/?to={0}&from=2020-12-22".format(today))
        mc = pd.DataFrame.from_dict(mc.json()).set_index("date").drop("_id", axis=1).sort_index().dropna()
        mc.index = pd.to_datetime(mc.index).date

        # get ust price
        req = requests.get(
            "https://eodhistoricaldata.com/api/eo"
            + "d/{0}.CC?api_token={1}&period=d&fmt=json".format("UST-USD", EOD_API)
        )
        df = pd.DataFrame.from_dict(req.json()).set_index("date")[["close"]]
        df.index = pd.to_datetime(df.index).date

        final = df.join(mc, how="outer")
        final["ustmc"] = final["close"] * final["value"]
        final["ustmc_1day_pct_change"] = final["ustmc"].pct_change()
        final["ustmc_1day_pct_change_rank"] = final["ustmc"].pct_change().rank(pct=True)
        final["ustmc_1day_pct_change_mean"] = final["ustmc"].pct_change().mean()
        final["ustmc_7day_pct_change"] = final["ustmc"].pct_change(7)
        final["ustmc_7day_pct_change_rank"] = final["ustmc"].pct_change(7).rank(pct=True)
        final["ustmc_7day_pct_change_mean"] = final["ustmc"].pct_change().mean()
        final["ustmc_1month_pct_change"] = final["ustmc"].pct_change(30)
        final["ustmc_1month_pct_change_rank"] = final["ustmc"].pct_change(30).rank(pct=True)
        final["ustmc_1month_pct_change_mean"] = final["ustmc"].pct_change(30).mean()
        final["ustmc_1year_pct_change"] = final["ustmc"].pct_change(365)
        final["ustmc_1year_pct_change_rank"] = final["ustmc"].pct_change(365).rank(pct=True)
        final["ustmc_1year_pct_change_mean"] = final["ustmc"].pct_change(365).mean()

        # get luna price
        req = requests.get(
            "https://eodhistoricaldata.com/api/eo"
            + "d/{0}.CC?api_token={1}&period=d&fmt=json".format("LUNA-USD", EOD_API)
        )
        df = pd.DataFrame.from_dict(req.json()).set_index("date")[["close"]]
        df.index = pd.to_datetime(df.index).date

        final.rename(columns={"close": "ust_price"}, inplace=True)
        final = final.join(df, how="inner")
        final.rename(columns={"close": "luna_price"}, inplace=True)

        # get luna / ust market cap ratio
        final["luna_ustmc_ratio"] = final["luna_price"] / final["ustmc"]
        # pct rank current ratio
        final["luna_ustmc_ratio_pct_rank"] = final["luna_ustmc_ratio"].rank(pct=True)
        # historical ratio average
        final["ratio_average"] = final["luna_ustmc_ratio"].mean()
        # luna 1 week returns
        final["luna_1week_returns"] = final["luna_price"].pct_change(7)
        # luna 1 week returns
        final["luna_1month_returns"] = final["luna_price"].pct_change(30)
        # luna 1 week returns
        final["luna_1year_returns"] = final["luna_price"].pct_change(365)
        # luna 1 week return ranks
        final["luna_1week_returns_pctrank"] = final["luna_price"].pct_change(7).rank(pct=True)
        # luna 1 week returns
        final["luna_1month_returns_pctrank"] = final["luna_price"].pct_change(30).rank(pct=True)
        # luna 1 week returns
        final["luna_1year_returns_pctrank"] = final["luna_price"].pct_change(365).rank(pct=True)

        final = final.join(dashboard, how="outer")

        final.columns = [
            "UST Price ($)",
            "UST Circulating Supply ($)",
            "UST Market Cap ($)",
            "UST Market Cap 1 Day Percent Change (%)",
            "UST Market Cap 1 Day Percent Change Percentile Rank (%)",
            "UST Market Cap 1 Day Percent Change Average (%)",
            "UST Market Cap 7 Day Percent Change (%)",
            "UST Market Cap 7 Day Percent Change Percentile Rank (%)",
            "UST Market Cap 7 Day Percent Change Average (%)",
            "UST Market Cap 1 Month Percent Change (%)",
            "UST Market Cap 1 Month Percent Change Percentile Rank (%)",
            "UST Market Cap 1 Month Percent Change Average (%)",
            "UST Market Cap 1 Year Percent Change (%)",
            "UST Market Cap 1 Year Percent Change Percentile Rank (%)",
            "UST Market Cap 1 Year Percent Change Average (%)",
            "LUNA Price ($)",
            "LUNA UST Market Cap Ratio",
            "LUNA UST Market Cap Ratio Percentile Rank",
            "LUNA UST Market Cap Ratio Average",
            "LUNA 1 Week Return (%)",
            "LUNA 1 Month Return (%)",
            "LUNA 1 Year Returns (%)",
            "LUNA 1 Week Return Percentile Rank (%)",
            "LUNA 1 Month Return Percentile Rank (%)",
            "LUNA 1 Year Returns Percentile Rank (%)",
            "LUNA Daily Staking Return (%)",
            "LUNA Staking Return Annualized (%)",
            "Daily UST Transaction Volume ($)",
            "Daily Registered Accounts",
            "Daily Terra Block Rewards",
            "LUNA Daily Staking Return Percentile Rank (%)",
            "LUNA Staking Return Annualized Percentile Rank (%)",
            "Daily UST Transaction Volume Percentile Rank($)",
            "Daily Registered Accounts Percentile Rank",
        ]
        final.drop(["UST Price ($)", "UST Circulating Supply ($)", "LUNA Price ($)"], axis=1, inplace=True)

        final.columns = [i.replace(" (%)", "") for i in final.columns]

        try:
            cmcs = []
            p = re.compile("(?<!\\\\)'")
            for i in ["BTC", "SOL", "ETH", "LUNA"]:
                cmc = requests.get("https://api.alphadefi.fund/historical/coinmarketcaps/{0}".format(i))
                cmc = pd.DataFrame(cmc.json())
                cmc["symbol"] = i
                cmc["quote_dict"] = cmc["market_cap"].apply(lambda x: json.loads(p.sub('"', str(x))))
                cmc["Market Cap"] = cmc["quote_dict"].apply(lambda x: x["USD"]["market_cap"])
                cmc["Market Cap Dominance"] = cmc["quote_dict"].apply(
                    lambda x: float(x["USD"]["market_cap_dominance"]) / 100
                )
                cmcs.append(cmc)
        except:
            cmcs = []
            p = re.compile("(?<!\\\\)'")
            for i in ["BTC", "SOL", "ETH", "LUNA"]:
                cmc = requests.get("https://api.alphadefi.fund/historical/coinmarketcaps/{0}".format(i))
                cmc = pd.DataFrame(cmc.json())
                cmc["symbol"] = i
                cmc["quote_dict"] = cmc["market_cap"].apply(lambda x: json.loads(p.sub('"', str(x))))
                cmc["Market Cap"] = cmc["quote_dict"].apply(lambda x: x["USD"]["market_cap"])
                cmc["Market Cap Dominance"] = cmc["quote_dict"].apply(
                    lambda x: float(x["USD"]["market_cap_dominance"]) / 100
                )
                cmcs.append(cmc)

        cmc = pd.concat(cmcs)

        temps = []
        for i in final.columns:
            temp = final[[i]].reset_index().rename(columns={i: "value", "index": "date"})
            temp["ticker"] = i
            temp = temp.dropna()
            temps.append(temp)

        for sym in ["BTC", "ETH", "SOL", "LUNA"]:
            for i in ["Market Cap", "Market Cap Dominance"]:
                temp = (
                    cmc[cmc["symbol"] == sym][["date", i]]
                    .rename(columns={i: "value", "last_updated": "date"})
                    .reset_index(drop=True)
                )
                temp["date"] = pd.to_datetime(temp["date"])
                temp["ticker"] = sym + " " + i
                temp = temp.dropna()
                temps.append(temp)

        for sym in ["BTC", "ETH", "SOL"]:
            for i in ["Market Cap"]:
                temp = cmc[cmc["symbol"] == sym][["date", i]].rename(columns={i: "value"}).reset_index(drop=True)
                temp["date"] = pd.to_datetime(temp["date"]).dt.date
                lunatemp = cmc[cmc["symbol"] == "LUNA"][["date", i]].rename(columns={i: "value"}).reset_index(drop=True)
                lunatemp["date"] = pd.to_datetime(lunatemp["date"]).dt.date
                temp = temp.set_index("date").join(lunatemp.set_index("date"), on="date", rsuffix="_luna")
                temp["value"] = (temp["value_luna"] / temp["value"]).round(4)
                temp = temp[["value"]].reset_index()
                temp["date"] = pd.to_datetime(temp["date"])
                temp["ticker"] = "LUNA - " + sym + " " + i + " " + "Ratio"
                temp = temp.dropna()
                temps.append(temp)

        master = pd.concat(temps)
        master["date"] = master["date"].apply(lambda x: pd.to_datetime(dt.datetime.strftime(x, "%Y-%m-%d")))

        terra_score = master.copy().groupby("ticker")["value"].last()
        terraHealth = (
            (
                terra_score.loc["Daily Registered Accounts Percentile Rank"]
                + terra_score.loc["Daily UST Transaction Volume Percentile Rank($)"]
                - terra_score.loc["LUNA UST Market Cap Ratio Percentile Rank"]
            )
            / 3
            * 100
        )

        master["terraHealth"] = terraHealth

        tickers = (
            master[
                ~master["ticker"].isin(
                    [
                        "UST Market Cap 7 Day Percent Change Average",
                        "UST Market Cap 1 Day Percent Change Average",
                        "UST Market Cap 1 Month Percent Change Average",
                        "UST Market Cap 1 Year Percent Change Average",
                        "LUNA UST Market Cap Ratio Average",
                        "LUNA UST Market Cap Ratio",
                        "Daily Terra Block Rewards",
                    ]
                )
            ]["ticker"]
            .unique()
            .tolist()
        )
        blankdict = {}
        for i in tickers:
            blankdict[i] = i
        dashboardDict = {"token": blankdict}

        return [master, dashboardDict]
