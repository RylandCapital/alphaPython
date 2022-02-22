import numpy as np
import pandas as pd
import schedule 
import time
import math
import pymongo 

import os

from alphaTerra import alphaTerra
from terrahelper import terraHelper

from dotenv import load_dotenv

load_dotenv()

ALPHADEFI_MONGO = os.getenv('ALPHADEFI_MONGO')

m = alphaTerra()     


'''define database'''        
client = pymongo.MongoClient(ALPHADEFI_MONGO,
ssl=True,
ssl_cert_reqs='CERT_NONE')
db = client.alphaDefi


if __name__ == "__main__":
    
    def apiUpdate():
        
        
        '''update coin marketcaps required for other updates below'''
        collection = db.coinmarketcaps
        collection.create_index("id", unique=True)
        coincaps = terraHelper.coinmarketcaps()
        coincaps['last_updated']=pd.to_datetime(coincaps['last_updated'])
        coincaps['id'] = coincaps['slug'] + coincaps['last_updated'].astype(str)
        
        errors = []
        for document in coincaps.to_dict('rows'):
            try:
                collection.insert_one(document)
            except Exception as e:
                errors.append(e)
        
        
        '''update terraswap comission aprs modules drop down menu 
        and huistorical volatilties and statistics of aprs'''
        ### believe this is deprecated. can remove once confirmed.
        pool_dict = pd.DataFrame(terraHelper.terra_pairs_names(),
                                  index=np.arange(len(
                    terraHelper.terra_pairs_names()))).T[[0]].rename(
            columns={0: 'pool'}).reset_index().rename(columns={
                'index': 'token'}).set_index('pool') #.to_dict()

        
        print('running ')
        spreadtracker_data = m.alphatrackerUpdate()
        
        collection = db.spreadHISTSTATS
        collection.drop()
        time.sleep(2)
        collection.insert_many(spreadtracker_data[0][
            spreadtracker_data[0].index != 'MIR'].reset_index().to_dict(
                orient='rows'))

        collection = db.tokenDICT
        collection.drop()
        time.sleep(2)
        collection.insert_one(spreadtracker_data[1])
               
        print('anchor data')
        anchor_data = m.anchorAPY()
        
        try:
            collection = db.HistoricalAnchor
            anchor_data[0] = anchor_data[0].sort_values('date').iloc[-9:]
            anchor_data[0]['id'] = anchor_data[0]['date'].astype(str) + \
                anchor_data[0]['ticker']
            anchor_data[0]['date'] = pd.to_datetime(anchor_data[0]['date'])
            collection.create_index("id", unique=True)
            time.sleep(2)
            collection.insert_many(
                anchor_data[0].to_dict(orient='records'))
        except Exception as e:
            print(e)
            pass

        collection = db.anchor_dict
        collection.drop()
        time.sleep(2)
        collection.insert_one(anchor_data[1])
        
        print('nexus')
        nexus_update = m.nexus()
        
        collection = db.nexusDict
        collection.drop()
        time.sleep(2)
        collection.insert_one(nexus_update[1])

        
        nexusdf = nexus_update[0]
        nexusdf['date'] = pd.to_datetime(nexusdf['date'])
        nexusdf['id'] = nexusdf['date'].astype(str)+nexusdf['ticker']
        collection = db.nexusVaults
        collection.create_index("id", unique=True)
        time.sleep(2)
        collection.insert_many(
            nexusdf.to_dict(orient='records'))
        
        
        print('terra core')
        print('luna market cap info for Terra Core')
        marketcap_data = m.luna_staking()
        
        collection = db.dashboard
        collection.drop()
        time.sleep(2)
        collection.insert_many(
            marketcap_data[0].to_dict(orient='records'))
            
        collection = db.dashboardDict
        collection.drop()
        time.sleep(2)
        collection.insert_one(marketcap_data[1])
        print('complete')
    
    schedule.every().day.at("10:00").do(apiUpdate)
    print('running')
    while True:
        schedule.run_pending()
        time.sleep(60) 
        
        

