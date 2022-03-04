import pandas as pd
import numpy as np
import requests
import json
from datetime import timedelta
import datetime as dt
from terrahelper import terraHelper
import time
import re
import os


from dotenv import load_dotenv

load_dotenv()

EOD_API = os.getenv("EOD_API")


class mongoHelper(object):

    """mongo helper functions"""

    def __init__(self):
        pass
    
    def deletebyExample():
        
        return "db.aprs.delete_many({'timestamp': {'$gte': start, '$lt': end}})"

