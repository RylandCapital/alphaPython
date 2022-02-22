import datetime as dt
import schedule
import time
import pymongo
import pandas as pd
from pytz import timezone
import requests
import numpy as np
import os

from terrahelper import terraHelper
from alphaTerra import alphaTerra

from dotenv import load_dotenv

load_dotenv()

ALPHADEFI_MONGO = os.getenv('ALPHADEFI_MONGO')


'''define database'''        
client = pymongo.MongoClient(ALPHADEFI_MONGO,
ssl=True,
ssl_cert_reqs='CERT_NONE')
db = client.alphaDefi

collateral_dict ={
    
    'terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun':'bETH',
    'terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp':'bLUNA'
    
    }


def job():
    
    
    try:
        
            liquidations = terraHelper.get_kujia_liquidations(limit=200)
            liquidations['executed_at'] = pd.to_datetime(
                liquidations['executed_at'])
            liquidations['day'] = pd.to_datetime(liquidations['executed_at'].dt.date)
            liquidations[['bid_fee', 'collateral_amount', 'liquidator_fee',
                          'repay_amount']] = liquidations[['bid_fee',
                'collateral_amount', 'liquidator_fee', 'repay_amount']]/1000000
            liquidations['id'] = liquidations['executed_height'].astype(str) + '-' +\
            liquidations['borrower'].str[-5:] + '-' + \
            liquidations['liquidator'].str[-5:] + \
            liquidations['executed_at'].astype(str) + \
            liquidations['repay_amount'].astype(str) + \
            liquidations['bid_fee'].astype(str)
            
            
            liquidations.rename(columns={'borrower': 'liquidatee',
                                         'executed_height':'height',
                                         'liquidator_fee': 'fee',
                                         'repay_amount':'Total_Amount_Paid_for_Collateral',
                                         'collateral_token':'token',
                                         'collateral_amount':'number_tokens_liquidated',
                                         'stable_denom':'denom'},
                                            inplace=True)
            liquidations['Discounted_Price_Per_Unit_Paid'] = liquidations[
                'Total_Amount_Paid_for_Collateral'] /\
                liquidations['number_tokens_liquidated']
            liquidations['symbol'] = liquidations['token'].apply(lambda x:
                                                    collateral_dict[x])
                
            'get DEX Prices'  
            #liquidatuions = UTC
            #round UTC to nearest minute
            uts_now = liquidations['executed_at'].iloc[0]
            uts_now_seconds_left = 60 - liquidations['executed_at'].iloc[0].second
            if uts_now_seconds_left<=30:
                uts_now = uts_now+dt.timedelta(seconds=uts_now_seconds_left)
            else:
                uts_now = uts_now-dt.timedelta(
                    seconds=liquidations['executed_at'].iloc[0].second)
            #get unix time for date for api call
            uts_now = int(dt.datetime.timestamp(uts_now))
            
            uts_start = liquidations['executed_at'].iloc[-1]
            uts_start_seconds_left = 60 - liquidations['executed_at'].iloc[-1].second
            if uts_start_seconds_left<=30:
                uts_start = uts_start+dt.timedelta(seconds=uts_start_seconds_left)
            else:
                uts_start = uts_start-dt.timedelta(
                    seconds=liquidations['executed_at'].iloc[-1].second)
            uts_start = int(dt.datetime.timestamp(uts_start))
            
            bluna = pd.DataFrame(requests.get(r'https://api.coinhall.org/api/v1/charts/terra/candles?bars=33&from={0}&interval=1m&pairAddress=terra18r6rdnkgrg74zew3d8l9nhk0m4xanpeukw3e20&quoteAsset=uusd&to={1}'.format(
                                     uts_start,uts_now)).json()) 
            bluna['datetime'] = (bluna.reset_index()['time']/1000).apply(
                lambda x: dt.datetime.utcfromtimestamp(x))
            beth = pd.DataFrame(requests.get(r'https://api.coinhall.org/api/v1/charts/terra/candles?bars=33&from={0}&interval=1m&pairAddress=terra1c0afrdc5253tkp5wt7rxhuj42xwyf2lcre0s7c&quoteAsset=uusd&to={1}'.format(
                                     uts_start,uts_now)).json())
            beth['datetime'] = (beth.reset_index()['time']/1000).apply(
                lambda x: dt.datetime.utcfromtimestamp(x))
            luna = pd.DataFrame(requests.get(r'https://api.coinhall.org/api/v1/charts/terra/candles?bars=33&from={0}&interval=1m&pairAddress=terra1j66jatn3k50hjtg2xemnjm8s7y8dws9xqa5y8w&quoteAsset=uusd&to={1}'.format(
                                     uts_start,uts_now)).json())
            luna['datetime'] = (luna.reset_index()['time']/1000).apply(
                lambda x: dt.datetime.utcfromtimestamp(x))
            lunaust = pd.DataFrame(requests.get(r'https://api.coinhall.org/api/v1/charts/terra/candles?bars=33&from={0}&interval=1m&pairAddress=terra1tndcaqxkpc5ce9qee5ggqf430mr2z3pefe5wj6&quoteAsset=uusd&to={1}'.format(
                                     uts_start,uts_now)).json()) 
            lunaust['datetime'] = (lunaust.reset_index()['time']/1000).apply(
                lambda x: dt.datetime.utcfromtimestamp(x))
            
            bluna.rename(columns={'high':'high_bluna'}, inplace=True)
            bluna.set_index('datetime', inplace=True)
            bluna.index = pd.DatetimeIndex(bluna.index, tz='UTC')
            beth.rename(columns={'high':'high_beth'}, inplace=True)
            beth.set_index('datetime', inplace=True)
            beth.index = pd.DatetimeIndex(beth.index, tz='UTC')
            luna.rename(columns={'high':'high_luna'}, inplace=True)
            luna.set_index('datetime', inplace=True)
            luna.index = pd.DatetimeIndex(luna.index, tz='UTC')
            lunaust.rename(columns={'high':'high_lunaust'}, inplace=True)
            lunaust.set_index('datetime', inplace=True)
            lunaust.index = pd.DatetimeIndex(lunaust.index, tz='UTC')
            
            def minuteRound(x):
                
                seconds_left = 60 - x.second
                if seconds_left<=30:
                    value = x+dt.timedelta(seconds=seconds_left)
                else:
                    value = x-dt.timedelta(seconds=x.second)
                return value
            
            
            liquidations['executed_at_round_timestamp'] = liquidations[
                'executed_at'].apply(lambda x: minuteRound(x))
            # liquidations.set_index('executed_at_round_timestamp', inplace=True)
            
            liquidations = liquidations.join(bluna[['high_bluna']], how='left',
                                             on='executed_at_round_timestamp')
            liquidations = liquidations.join(beth[['high_beth']], how='left',
                                             on='executed_at_round_timestamp')
            liquidations = liquidations.join(luna[['high_luna']], how='left',
                                             on='executed_at_round_timestamp')
            liquidations = liquidations.join(lunaust[['high_lunaust']], how='left',
                                             on='executed_at_round_timestamp')
            liquidations = liquidations.dropna()
            liquidations['Discount_vs_UST_DEX_Price_at_Liquidation'] = np.where(liquidations['symbol']=='bETH',
                                                        (liquidations['Discounted_Price_Per_Unit_Paid']/\
                                                            liquidations['high_beth'])-1,
                                                        (liquidations['Discounted_Price_Per_Unit_Paid']/\
                                                            liquidations['high_bluna'])-1)
                
            liquidations = liquidations.sort_values(
                'executed_at', ascending=True).iloc[100:]
            liquidations.dropna(inplace=True)
            liquidations.drop(['denom','liquidator','liquidatee'],
                              axis=1, inplace=True)
            
            liquidations['max_amount'] = liquidations[
                'Total_Amount_Paid_for_Collateral'].max()
            liquidations['best_discount'] = liquidations[
                'Discount_vs_UST_DEX_Price_at_Liquidation'].min()
            liquidations['average_discount'] = liquidations[
                'Discount_vs_UST_DEX_Price_at_Liquidation'].describe().loc['mean']

            
            
                                                           
            '''update current liquidation profile live (1min)'''
            collection = db.kujiraLiquidations
            collection.create_index("id", unique=True)
            errors = []
            for document in liquidations.to_dict('rows'):
                try:
                    collection.insert_one(document)
                except Exception as e:
                    errors.append(e)
             
             
            print('data collected, with {0} possible duplicates'.format(len(errors)))
        
        
        
        
    except Exception as e:
        print(e)
        pass
        
                  
       

        
#%%
schedule.every(3).minutes.at(":30").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

