import requests
import time
import datetime as dt
import pandas as pd
import base64, json
import os

from terra_sdk.client.lcd import LCDClient
from terra_sdk.core.bank import MsgSend
from terra_sdk.core.coins import Coins
from terra_sdk.key.mnemonic import MnemonicKey
from terra_sdk.core.wasm import MsgExecuteContract

from terra_sdk.client.lcd.api.tx import CreateTxOptions
from terra_sdk.core.fee import Fee

from contextlib import redirect_stdout
from printPrepender import PrintPrepender

from dotenv import load_dotenv

load_dotenv()


client = LCDClient(
    "https://lcd.terra.dev", "columbus-5"
    )
bank = client.staking.delegations(validator='terravaloper1259cmu5zyklsdkmgstxhwqpe0utfe5hhyty0at')

