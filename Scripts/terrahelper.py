import pandas as pd
from string import Template
import requests
import json
import datetime as dt
import os

#for coinmarketcap
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

from dotenv import load_dotenv

load_dotenv()

ALPHADEFI_WALLET = os.getenv('ALPHADEFI_WALLET')
CMC_API = os.getenv('CMC_API')


class terraHelper(object):
    
    def __init__(self):
        pass

    
    #returns ALL AVAILABLE mirror assets + statistics
    def mirror_assets(url='https://graph.mirror.finance/graphql'):
        query = '''{
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
            }'''
                
        return query
    
    #returns account transactions
    def account_transactions(self, offset,
                             url='https://graph.mirror.finance/graphql'):
        query =  '''{
              txs(account: {0}, offset: 0, limit: {1}) {
                createdAt
                id
                height
                txHash
                address
                type
                data
                token
                datetime
                fee
                tags
                memo
                __typename
              }
            }'''.replace('{0}', self.account).replace('{1}', str(offset))
              
        return query
    
    #returns terra_wallet coins (does not include mAssets)
    def terra_wallet(self, url='https://mantle.terra.dev/'):
        query =  '''{
             BankBalancesAddress(Address: {0}){
                Height
                Result {
                  Amount
                  Denom
                }
            }
            '''.replace('{0}', self.account)
              
        return query
    
    #returns major overall terra network stats
    def terra_network(url='https://graph.mirror.finance/graphql'):
        query =  '''{
              statistic(network:”TERRA”) {
                assetMarketCap
                totalValueLocked
                collateralRatio
                mirCirculatingSupply
                govAPR
                govAPY
                latest24h {
                  transactions
                  volume
                  feeVolume
                  mirVolume
                  __typename
                }
                __typename
              }
            }'''

              
        return query
  
    def terra_current_block():
        
        req = requests.get('https://lcd.terra.dev/wasm/contracts/terra1tn'+\
                     'dcaqxkpc5ce9qee5ggqf430mr2z3pefe5wj6/store?query_m'+\
                     'sg={%22pool%22:{}}')
        return int(req.json()['height'])
     
    def getBlockTimestamp(height=""):

        value = requests.get("https://lcd.terra.dev/blocks/{1}".replace("{1}", str(height))).json()["block"]["header"][
                "time"
            ]
        
        return value
    def terra_current_timestamp():
        req = requests.get('https://api.anchorprotocol.com/api/v1/market/ust')
        
        return req.json()['timestamp']
        
    def terra_current_ustmc():
    
        req = requests.get('https://fcd.terra.dev/v1/circulatingsupply/uusd')
        return int(req.json())
    
    def terra_stables():
        
        req = requests.get('https://lcd.terra.dev/terra/oracle/v1beta1/denoms/exchange_rates')
        exrates = req.json()['exchange_rates']
        df = pd.DataFrame([], columns=['exrate'])
        for i in exrates:
            denom = i['denom']
            exrate = i['amount']
            df.loc[denom, 'exrate'] = exrate
        df['lunaRate'] = df['exrate'].astype(float)
        df['exrate'] = df['exrate'].astype(float)/float(df['exrate'].loc['uusd'])
         
        df2 = pd.DataFrame([], columns=['circulating'])
        for i in df.index.values:
             req = requests.get('https://fcd.terra.dev/v1/circulatingsupply/{0}'.format(i))
             value = int(req.json())
             df2.loc[i,'circulating'] = value
             
        df3 = df.join(df2)
        df3['market_cap_inUST'] = df3['circulating']/df3['exrate']/1000000
         
        
        return df3
    
    def terra_current_luna_price(interval='1d'):
        #intervals = '1m', '5m', '15m','1hr', '1d'
        req = requests.get(
            'https://fcd.terra.dev/v1/market/price?denom=uusd&interval={0}'.format(interval))
        prices = req.json()['prices']
        prices = pd.DataFrame.from_dict(prices)

        return prices.set_index('datetime')
    
    #master pool information from coinhall
    def coinhall_terra_info():
        
        req = requests.get(
            'https://api.coinhall.org/api/v1/charts/terra/pairs')
        data = req.json()
        
        return data
    
    #saves token price and circulating supplies by pool address
    def terra_token_snapshot():
      
      now = dt.datetime.now()
      req = requests.get(
            'https://api.coinhall.org/api/v1/charts/terra/pairs')
      data = req.json()
      df = pd.DataFrame.from_dict(data).T.iloc[:, :-4][['asset0','asset1']]

      def circ(x):
        try:
          return x['circSupply']
          
        except:
          pass
      def address(x):
        try:
          return x['contractAddress']
        except:
          pass

      df0circ = pd.DataFrame(df['asset0'].apply(lambda x: circ(x))).dropna().rename(columns={'asset0':'circ_supply'})
      df0addy = pd.DataFrame(df['asset0'].apply(lambda x: address(x))).dropna().rename(columns={'asset0':'asset'})
      df0 = df0circ.join(df0addy, how='inner')
      df1circ = pd.DataFrame(df['asset1'].apply(lambda x: circ(x))).dropna().rename(columns={'asset1':'circ_supply'})
      df1addy = pd.DataFrame(df['asset1'].apply(lambda x: address(x))).dropna().rename(columns={'asset1':'asset'})
      df1 = df1circ.join(df1addy, how='inner')
      final = pd.DataFrame(pd.concat([df0,df1]))
      final['date'] = now
      final = final

      req = requests.get('https://api.coinhall.org/api/charts/terra/prices/latest')
      data = req.json()
      def price(x):
        try:
          return data[x]
        except:
          pass
      final = final.reset_index()
      final['pool_price'] = final.reset_index()['index'].apply(lambda x: price(x))
      final = final.dropna().rename(columns={'index':'pool'})
      final['circ_supply_of_asset'] = final['circ_supply'].astype('float').astype('int64')
      final['pool_price'] = final['pool_price'].astype('float')

      return final[['pool', 'asset', 'circ_supply_of_asset', 'pool_price']]

    
    def coinhall_terra_latest_prices():
        req = requests.get('https://api.coinhall.org/api/charts/terra/prices/latest')
        data = req.json()
        return data
    
    def coinhall_volumes():
        req = requests.get('https://api.coinhall.org/api/v1/charts/terra/pairs')
        data = req.json()
        return data
    
    def coinmarketcaps():
        
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map'
        parameters = {
          'start':'1',
          'limit':'1000',
        }
        headers = {
          'Accepts': 'application/json',
          'X-CMC_PRO_API_KEY': '{0}'.format(CMC_API),
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        try:
          response = session.get(url, params=parameters)
          ids = json.loads(response.text)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
          print(e)
          
        
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        parameters = {
          'start':'1',
          'limit':'100',
          'convert':'USD'
        }
        headers = {
          'Accepts': 'application/json',
          'X-CMC_PRO_API_KEY': '{0}'.format(CMC_API),
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        try:
          response = session.get(url, params=parameters)
          data = json.loads(response.text)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
          print(e)
          
          
        datadf = pd.DataFrame.from_dict(data['data']).set_index('id')
        idsdf = pd.DataFrame.from_dict(ids['data']).set_index('id')
        final2 = datadf.join(idsdf, rsuffix='_id')
        
       
        return final2
        
    def get_kujira_summary():
        
        req = requests.get('https://api.kujira.app/api/{0}/borrowers/summary'.format(ALPHADEFI_WALLET))
        data = req.json()
        data = pd.DataFrame.from_dict(data['summary'])
        
        return data
    
    def get_kujia_liquidations(limit=100):
         req = requests.get('https://api.kujira.app/api/{1}/liquidations?chain=terra&protocol=anchor&limit={0}'.format(limit, ALPHADEFI_WALLET))
         return pd.DataFrame.from_dict(req.json()['liquidations'])

    def cap_weighted_index(symbolids=[], mars_protocol_circ=90000000, nexus_protocol_circ=882213698, prism_protocol_circ=70000000):
      #ALTSZN  symbolids = ['anchor-protocol', 'pylon-protocol', 'mirror-protocol', 'astroport',  'prism-protocol', 'mars-protocol', 'nexus-governance-token']
      #LUST symbolids = ['anchorust', 'terrausd', 'terra-luna']
      datas = []
      for symbol in symbolids:
        data = requests.get(
          'https://api.coingecko.com/api/v3/coins/{0}/market_chart?vs_currency=usd&days=max&interval=daily'.format(symbol)
          ).json()
        
        data = pd.DataFrame.from_dict(data)
        data['timestamp'] = data['prices'].apply(lambda x:  dt.datetime.utcfromtimestamp(x[0] // 1000))
        data['prices'] = data['prices'].apply(lambda x:  float(x[1]))
        data['pct_change'] = data['prices'].pct_change()
        # add circ supplies for any tokens we dont have them for!
        if symbol == 'mars-protocol':
          data['market_caps'] = data['prices']*mars_protocol_circ
        elif symbol == 'prism-protocol':
          data['market_caps'] = data['prices']*prism_protocol_circ
        elif symbol == 'nexus-governance-token':
          data['market_caps'] = data['prices']*nexus_protocol_circ
        elif symbol == 'anchorust':
          data['market_caps'] = data['prices']*0
        else: 
          data['market_caps'] = data['market_caps'].apply(lambda x:  float(x[1]))
        data['total_volumes'] = data['total_volumes'].apply(lambda x:  float(x[1]))
        data['ticker'] = symbol

        datas.append(data)

        index = pd.concat(datas)
        mc_sums = index.groupby('timestamp')['market_caps'].sum().iloc[:-len(symbolids)]
        mc_sums.name = 'sum_mc_for_day'
        index = index.set_index('timestamp').join(mc_sums, how='left').dropna()
        index['weight_return'] = index['pct_change']*index['market_caps']/index['sum_mc_for_day']
        index = index[index.index<pd.to_datetime('4/6/22')]

        #index.reset_index().groupby('timestamp')['weight_return'].sum().to_csv(r'C:\Users\rmathews\Downloads\test.csv')

        





    

    
    
         
         
         
         
    








