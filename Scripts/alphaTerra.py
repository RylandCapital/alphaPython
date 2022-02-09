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



class alphaTerra(object):

    '''tools for alphaDefi management'''

    def __init__(self):
        self.path = r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Crypto\mAssets'        
        pass

    def pullHistory(self, start_date='4/29/21', end_date=None):

        if end_date == None:
            end_date = dt.datetime.now().strftime("%m-%d-%Y")

        d1 = pd.to_datetime(start_date)
        d2 = pd.to_datetime(end_date)
        dd = [d1 + timedelta(days=x) for x in range((d2-d1).days + 1)]
        days = [d.strftime("%m-%d-%Y") for d in dd]

        mypath = self.path

        files = []
        for day in days:
            onlyfiles = [f for f in listdir(mypath+r'\{0}'.format(day))]
            try:
                data = pd.concat([pd.read_csv(mypath+r'\{0}'.format(day) + "\\" +
                                              i, index_col=0, compression='gzip')
                                  for i in onlyfiles])
                files.append(data)
            except:
                print('error')

        df = pd.concat(files)
        df.index = pd.to_datetime(df.index)
        df['spread'] = (df['price']-df['oralceprice'])/df['price']
        df['day'] = df.index.date

        return df

    def resampleOHLC(self, df, column, resample='30Min'):

        return df.groupby('symbol')[column].resample(
            resample).ohlc().interpolate(method='linear')

    def spreadStats(self, start_date='4/29/21', end_date=None):

        df = self.pullHistory(start_date, end_date=None)

        if end_date == None:
            end_date = dt.datetime.now().strftime("%m-%d-%Y")

        d1 = pd.to_datetime(start_date)
        d2 = pd.to_datetime(end_date)
        dd = [d1 + timedelta(days=x) for x in range((d2-d1).days + 1)]
        days = [d.strftime("%m-%d-%Y") for d in dd]

        mypath = self.path

        files = []
        for day in days:
            onlyfiles = [f for f in listdir(mypath+r'\{0}'.format(day))]
            try:
                data = pd.concat([pd.read_csv(mypath+r'\{0}'.format(day) + "\\" +
                                              i, index_col=0, compression='gzip')
                                  for i in onlyfiles])
                files.append(data)
            except:
                logging.info('Missing Data On Day {0}'.format(day))

            

        df = pd.concat(files)
        df.index = pd.to_datetime(df.index)
        df['spread'] = (df['price']-df['oralceprice'])/df['price']

        statsy = df.groupby('symbol')['spread'].resample(
            '5Min').ohlc().interpolate(method='linear').reset_index().set_index(
            'index').groupby('symbol').describe()[[('close', 'mean'),
                                                   ('close', 'std'), ('close', 'min'), ('close', 'max')]].dropna()

        statsy.columns = ['mean', 'std', 'min', 'max']

        return df, statsy

    def pullMirrorAssets(self):

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
                }'''

        url = 'https://graph.mirror.finance/graphql'
        r = requests.post(url, json={'query': query})
        now = dt.datetime.now()
        json_data = pd.DataFrame(json.loads(r.text)['data']['assets'])
        json_data['price'] = json_data['prices'].apply(
            lambda x: float(x['price']))
        json_data['oralceprice'] = json_data['prices'].apply(
            lambda x: x['oraclePrice'] if x['oraclePrice'] == None else float(
                x['oraclePrice']))
        json_data['liquidity'] = json_data['statistic'].apply(
            lambda x: float(x['liquidity']))
        json_data['volume'] = json_data['statistic'].apply(
            lambda x: float(x['volume']))
        json_data['apr'] = json_data['statistic'].apply(
            lambda x: float(x['apr']['long']))
        json_data.drop(['statistic', 'prices'], axis=1, inplace=True)
        json_data['index'] = now.strftime("%m/%d/%Y %H:%M:%S")
        json_data.set_index('index', inplace=True)

        json_data['spread'] = (json_data['price']-json_data['oralceprice']) /\
            json_data['oralceprice']

        return json_data


    def adjustDict(self):

        dictionary = {
            'mIAU': {'2021-05-24 09:43:00': '2'},
            'mVIXY': {'2021-05-26 09:59:01': '4'}
        }

        df = pd.DataFrame.from_dict(dictionary).stack().reset_index()
        df.columns = ['date', 'symbol', 'ratio']
        return df

    def adjusted_close(self, historical_data):

        adjustdf = self.adjustDict()
        adjustdf['date'] = pd.to_datetime(adjustdf['date'])
        adjustdf.rename(columns={'date': 'index'}, inplace=True)
        adjustdf.set_index(['index', 'symbol'], inplace=True)
        df = historical_data.reset_index().set_index([
            'index', 'symbol']).join(adjustdf)
        df = df.reset_index()
        df['ratio'] = df.groupby('symbol')['ratio'].bfill()
        df = df.dropna(subset=['price', 'volume'])
        df['ratio'] = df['ratio'].fillna(1).astype(float)
        df['adjusted_close'] = df['price']*df['ratio']
        df = df.reset_index()

        return df

    def commissionAPRs(self, pairs): #terraswap aprs set to be deprecated
        
        # pairs_dict = terraHelper.terra_pairs_names()

        t = int(time.time())

        timestamps = list(np.arange(1596981097, t, 17280000)) + [t]
        
        dfsm = []
        for i in pairs:
            dfs= []
            for ts in np.arange(len(timestamps))[1:]:
            
                url = 'https://terraswap-graph.terra.dev/graphql'
                query = '''{
                  pair(pairAddress: "terra1tnd") {
                    token0{
                      tokenAddress,
                      symbol
                    },
                    token1{
                      tokenAddress,
                      symbol
                    },
                    historicalData(to: 1635181097a,from: 1595981097a,interval: DAY){
                      timestamp,
                      volumeUST,
                      liquidityUST,
                      token0Price,
                      token1Price,
                      token0Reserve,
                      token1Reserve
                      totalLpTokenShare
                    }
                  }
                }'''.replace('1635181097a', str(timestamps[ts])).replace(
                                                            '1595981097a', str(timestamps[ts-1])).replace(
                                                        'terra1tnd', i)
                      
                try:
                    r = requests.post(url, json={'query': query})
                    hd = json.loads(r.text)['data']['pair']['historicalData']
                    if len(hd) > 0:
                        df = pd.DataFrame.from_dict(hd)
                        df = df.sort_values('timestamp').reset_index(drop=True)
                        df['ticker'] = terraHelper.terra_pairs_names()[i]
                        df['date'] = df[
                            'timestamp'].apply(lambda x: dt.datetime.utcfromtimestamp(x))
                        dfs.append(df)
                except:
                    print('error')
                    pd.DataFrame([i]).to_csv(
                        r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\CommissionAPRs_Errors\{0}'.format(i))
                
            try: 
                dfsm.append(pd.concat(dfs))
            except:
                print('no data at all')
                print(i)
                
               
        for df in dfsm:   
            df['value1'] = np.sqrt((df['token0Reserve'].astype(float)*df[
                'token1Reserve'].astype(float)))/df['totalLpTokenShare'].astype(float)
            df['value2'] = df['value1'].shift(7)
            df['start_timestamp'] = pd.to_datetime(df['timestamp'].shift(7), unit='s')
            df['end_timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df['time_between'] = (df['end_timestamp']-df['start_timestamp']).apply(lambda x: x.total_seconds()/(60*60*24))
            
            
            df['commission_apr'] = ((df['value1']-df['value2'])/df['value2'])*(
                    365/df['time_between'])
            
            df.to_excel(r'P:\11_CWP Alternative\cwp alt\rese' +
                            r'arch\alphaRaw\Data\CommissionAPRs\commissionAPR_{0}.xlsx'.format(
                                df['ticker'].iloc[0]), index=False)

    def astrocommissionAPRs(self, pairs): #astro aprs set to be deprecated
        '''Astroport Commission Code'''
        
        def get_eod_token(symbols): #this wont work without token
    
            ds = []
            for d in symbols:
                req = requests.get('https://eodhistoricaldata.com/api/eo' +
                                   'd/{0}.CC?api_token=5f3e88'.format(d) +
                                   'ded83498.58125490&period=d&fmt=json')
                df = pd.DataFrame.from_dict(req.json())
                df['symbol'] = d
                ds.append(df)
    
            return pd.concat(ds)
        
        apr_time_period = 100000
        data_collection_rate = 10000
        pairs_dict = terraHelper.astroportLPs()[1]
        prices = pd.read_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\PoolData\Prices.xlsx').iloc[:,1:]
        prices['ticker2'] = prices['ticker'].apply(lambda x: x.split('-')[0])
        prices['ticker3'] = prices['ticker'].apply(lambda x: x.split('-')[1])
        prices.set_index('date', inplace=True)
        curr_block = terraHelper.terra_current_block()
        astro_emissions = terraHelper.astro_emissions()
        current_astro_price = terraHelper.coinhall_terra_latest_prices()[
             'terra1l7xu2rl3c7qmtx3r5sd2tz25glf6jh8ul7aag7']
        astro_prices = get_eod_token(['ASTRO1-USD']).set_index('date')
        astro_prices.loc[dt.datetime.now().strftime("%Y-%m-%d"),'close'] = current_astro_price
        
        dfs = []
        for pair in pairs:
            print('Currently On Pair: {0}'.format(pairs_dict[pair]))
            
            df = pd.DataFrame([], columns=['time','amount1','amount2',
                                        'total_share','value1'])
            
            for i in np.arange(5860000, curr_block, data_collection_rate):
                              
                count = True
                while count == True:
                    try:
                        time.sleep(1)
                        
                        req = requests.get(
                        'https://lcd.terra.dev/wasm/contracts/PAIR/store?query_msg={%22pool%22:{}}'.replace('PAIR', pair))

                        amount_current = int(req.json()['result']['assets'][0]['amount'])
    
                        req = requests.get(
                            'https://lcd.terra.dev/wasm/contracts/PAIR/store?query_msg={%22pool%22:{}}&height={1}'.replace(
                                '{1}', str(i)).replace('PAIR', pair))
                        
                        req2 = requests.get(
                        'https://lcd.terra.dev/wasm/contracts/PAIR/store?query_msg={%22pool%22:{}}'.replace('PAIR', pair))

                        amount_current2 = int(req2.json()['result']['assets'][0]['amount'])
    
                        
                        amount1 = int(req.json()['result']['assets'][0]['amount'])
                        amount2 = int(req.json()['result']['assets'][1]['amount'])
                        total_share = int(req.json()['result']['total_share'])
                        value1 = math.sqrt(amount1*amount2)/total_share
                        print(amount1)
                        print(amount_current)
                        
                        req = requests.get(
                            'https://fcd.terra.dev/blocks/{0}'.format(i))
                        timer = pd.to_datetime(req.json()[
                            'block']['header']['time'])
                    except:
                        print('there is an error')
                        pass
                        
                    if (amount1 != amount_current) & (amount1 != amount_current2):
                        print('logic met, saving data')
                        count = False
                    
                
                
                           
                        df.loc[i, 'time'] = timer
                        df.loc[i, 'amount1'] = amount1
                        df.loc[i, 'amount2'] = amount2
                        df.loc[i, 'total_share'] = total_share
                        df.loc[i, 'value1'] = value1
                        df.loc[i, 'name'] = pairs_dict[pair]
                        df.loc[i, 'amount1/amount2'] = amount1/amount2
                        
            dfs.append(df)
            
            dfs2 = []
            for x in dfs:
                
                print(x)
                
                x['value2(first)'] = x['value1'].shift(int(apr_time_period/data_collection_rate))
                x['time2(first)'] = x['time'].shift(int(apr_time_period/data_collection_rate))
                x['timedelta'] = (x['time'] - x['time2(first)']).dt.total_seconds()/(
                          24 * 60 * 60)
                
                x['commission_apr'] = ((x['value1']-x['value2(first)'])/x['value1'])*(
                          365/x['timedelta'])
                
                x['date'] = pd.to_datetime(pd.to_datetime(x['time']).dt.date)
                x['ticker2'] = x['name'].apply(lambda x: x.split('-')[0])
                x['ticker3'] = x['name'].apply(lambda x: x.split('-')[1].split(' ')[0])
                x['ticker_plain'] = x['name'].apply(lambda x: x.split(' ')[0])
                
                
                name1 = x['ticker2'].iloc[0]
                name2 = x['ticker3'].iloc[0]
                
                x = x.dropna()   
                x = x.set_index('date')
                
                
                '''join with prices'''
                'value2(first)'
                x = x.join(prices[(prices['ticker2']==name1) & \
                                  (prices['ticker3']==name2)][['token0Price']],
                            how='inner')
                    
                
                '''this is because you dont have prices for some''' 
                if (len(x)>0) and (x['ticker_plain'].iloc[0]!='LUNA-bLUNA'):
                    dfs2.append(x)
                    
                    
                
                df3 = []
                for x in dfs2:
                    
                    x['Liquidity'] = (x['amount1']/1000000)+((x['amount2']/1000000)*x['token0Price'])
                    
                    '''adjust for non-UST denominators to make all UST'''
                    if x['ticker_plain'].iloc[0] == 'LUNA-bLUNA': 
                        x = x.join(prices[(prices['ticker2']=='LUNA') & \
                                      (prices['ticker3']=='UST')][['token0Price']],
                                how='inner', rsuffix='_luna')
                        x['Liquidity']= x['Liquidity']*x['token0Price_luna']
                        
                    x['astro_emissions'] = x['ticker_plain'].apply(
                        lambda x: astro_emissions[x])
                    x['index_date'] = x.index
                    x['index_date'] = x['index_date'].dt.date
                    
                    
                    x['astro_prices'] = x['index_date'].apply(
                        lambda x: astro_prices.loc[str(x),'close'])
                    
                    x['annual_astro_emissions'] = x['astro_emissions']*x['astro_prices']
                    x['daily_astro_emissions'] = x['annual_astro_emissions'] / 365
                    x['daily_apr'] = x['daily_astro_emissions']/x['Liquidity']
                    x['annualized_reward_apr'] = (1+x['daily_apr'])**365-1
                    x['ALL_IN_APR'] = x['annualized_reward_apr']+x['commission_apr'] 
                    
                    df3.append(x[[
                        'name',
                        'time',
                        'commission_apr',
                        'astro_emissions',
                        'daily_astro_emissions',
                        'annualized_reward_apr',
                        'ALL_IN_APR'
                        ]].set_index('time'))
                    x.to_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\CommissionAPRs_Astroport\{0}.csv'.format(x['name'].iloc[0]))
                                            
    def masterAPR(self):
        
        
        def decimals(x):
            try:
                return x['decimals']
            except:
                pass
        
        latest = terraHelper.coinhall_terra_latest_prices()
        def getLatest(x):
            try:
                return latest[x]
            except:
                pass
            
        fees = {
            
            'Terraswap':.003,
            'Astroport':.002,
            'TerraFloki':.003,
            'Loop': .003,
            'Terraformer': .003
            
            
            }
        
        '''bluna luna addy ts terra1jxazgm67et0ce260kvrpfv50acuushpjsz2y0p'''
        
        '''bring in coinhall drop (has LP, token0, token1 addys)'''
        info = terraHelper.coinhall_terra_info() 
        '''need luna prices in all denoms'''
        luna_rates = terraHelper.terra_stables()
        luna_rates.loc['uluna','exrate'] = 1/luna_rates.loc['uusd']['lunaRate']

        df = pd.DataFrame.from_dict(info).T.iloc[:,:-4]
        
        def verify(x):
            try:
                return x['verified']
            except:
                pass
        
        '''Terraswap, Astrport, and Loop Only'''
        df=df[df['dex'].isin(['Terraswap', 'Astroport', 'Loop'])]
        df['verified0'] = df['asset0'].apply(lambda x: verify(x))
        df['verified1'] = df['asset1'].apply(lambda x: verify(x))
        df = df[(df['verified0']==1)&(df['verified1']==True)]
        
        '''get decimals'''
        df['decimals0'] = df['asset0'].apply(lambda x: decimals(x)).fillna(6)
        df['decimals1'] = df['asset1'].apply(lambda x: decimals(x)).fillna(6)
        df['decimalsMain'] = np.where(df['decimals0']>df['decimals1'],
                                      df['decimals0'],df['decimals1'])
        # df['decimalsMain'] = np.where(df['decimalsMain']==0,6,df['decimalsMain'])
        df['decimalTest'] = df['decimals0'] == df['decimals1']
        
        
        '''get contract addresses'''
        df['addy0'] = df['asset0'].apply(lambda x: x['contractAddress'])
        df['addy1'] = df['asset1'].apply(lambda x: x['contractAddress'])
        
        
        '''get symbols in pool'''
        df['symbol0'] = df['asset0'].apply(lambda x: x['symbol'])
        df['symbol1'] = df['asset1'].apply(lambda x: x['symbol'])
        
        '''get amounts in pool'''
        df['amount0'] = df['asset0'].apply(lambda x: x['poolAmount']).astype(
            float)/(10**df['decimals0'])
        df['amount1'] = df['asset1'].apply(lambda x: x['poolAmount']).astype(
            float)/(10**df['decimals1'])
        
        '''get volumes24 in pool'''
        df['volume24h0'] = df['asset0'].apply(lambda x: x['volume24h']).astype(
            float)/(10**df['decimals0'])
        df['volume24h1'] = df['asset1'].apply(lambda x: x['volume24h']).astype(
            float)/(10**df['decimals1'])
        
        '''get volumes7 in pool'''
        df['volume7d0'] = df['asset0'].apply(lambda x: x['volume7d']).astype(
            float)/(10**df['decimals0'])
        df['volume7d1'] = df['asset1'].apply(lambda x: x['volume7d']).astype(
            float)/(10**df['decimals1'])
        
        '''get dex swaprates'''
        df['swaprate'] = df['dex'].apply(lambda x: fees[x])
    
        
        '''get prices'''
        df = df.reset_index().rename(columns={'index':'poolAddy'})
        df['poolPrice'] = df.reset_index()['poolAddy'].apply(
            lambda x: getLatest(x))
        df['poolPrice'] = df['poolPrice'].fillna(0)
        df.set_index('poolAddy', inplace=True)
        
        '''clean up'''
        df=df[(df['volume24h0']>0) &\
              (df['amount0']>0) &\
              (df['amount1']>0)]
            
        
        '''denominator test'''  
        df['denomTest0'] = ((df['amount0']/df['amount1'])-df['poolPrice']).abs()
        df['denomTest1'] = ((df['amount1']/df['amount0'])-df['poolPrice']).abs()
        
    

        '''get_denominator'''
        df['denom'] =np.where(df['denomTest0']<df['denomTest1'],
                               'asset0','asset1')
        df['denom_symbol']  = np.where(df['denom']=='asset1',
                               df['symbol1'], df['symbol0'])
        df['numer_symbol']  = np.where(df['denom']=='asset1',
                               df['symbol0'], df['symbol1'])
        df['denom_addy']  = np.where(df['denom']=='asset1',
                               df['addy1'], df['addy0'])
        
        '''calculate total liquidity in USD'''
        
        def convert_ust(x):
            try:
                return luna_rates.loc[x]['exrate']
            except:
                pass
            
            
        ####################################################################   
        '''calculate ust liquiity'''
        df['liquidity'] = np.where(df['denom']=='asset0',
                        df['amount1']*2*df['poolPrice'],
                        df['amount0']*2*df['poolPrice'])
        
        
         
        df['ust_divisor'] = df['denom_addy'].apply(lambda x: convert_ust(x))
        df['ustLiquidity'] = (df['liquidity']/df['ust_divisor'])
        df['ustLiquidity'] = df['ustLiquidity'].fillna(1000000000000000)
        
        
        
        
        #need denom-ust prices for pairs with weirder denoms
        nodenoms = df[df['ustLiquidity']==1000000000000000]['denom_symbol'].tolist()
        nodenom_prices = df[df['symbol0'].isin(nodenoms) | df
                            ['symbol1'].isin(nodenoms)]
        nodenom_prices = nodenom_prices [(nodenom_prices ['amount0']>0) & \
                                         (nodenom_prices ['amount1']>0)]
        nodenom_prices = nodenom_prices[nodenom_prices['denom_symbol']=='UST']
        nodenom_prices = nodenom_prices.drop_duplicates('numer_symbol',
                                        keep='last').set_index('numer_symbol')
        
        def fillNodenoms(x):
            
            try:
               
                    return nodenom_prices.loc[x]['poolPrice']
            except:
                pass
                
        df = df.reset_index()      
        df['ustMultiplier'] = df['denom_symbol'].apply(lambda x: fillNodenoms(x))
        df['ustLiquidity'] = np.where(df['ustLiquidity']==1000000000000000,
                                       df['liquidity']*df['ustMultiplier'],
                                       df['ustLiquidity'])
        df['ustLiquidity'] = df['ustLiquidity'].astype(float)
        
    
        

        ####################################################################
        '''get poolVolume in UST'''
        
        df['poolVolume7d'] = np.where(df['denom']=='asset0',
                                       df['volume7d1']*df['poolPrice'],
                                       df['volume7d0']*df['poolPrice'])
        
       
        
        df['ustPoolVolume7d'] = (df['poolVolume7d']/df['ust_divisor'])
        df['ustPoolVolume7d'] = df['ustPoolVolume7d'].fillna(1000000000000000)
          
        
        #adjust for no divisors
        df['ustPoolVolume7d'] = np.where(df['ustPoolVolume7d']==1000000000000000,
                                       df['poolVolume7d']*df['ustMultiplier'],
                                       df['ustPoolVolume7d'])
        
        ####################################################################
        '''calculate APRs'''
        df['apr7d'] = (df['swaprate']*df['ustPoolVolume7d'])/df['ustLiquidity']*52
        final = df[['dex','decimals0','symbol0','symbol1', 'poolPrice', 'liquidity',
                   'ustLiquidity', 'volume7d0', 'volume7d1', 'poolVolume7d',
                   'ustPoolVolume7d', 'ust_divisor','ustMultiplier','apr7d']]
        final = final.dropna(subset=['apr7d'])
        
        final['symbol_sort'] = (final['symbol0'].apply(lambda x: [x.lower()]) + final[
            'symbol1'].apply(lambda x: [x.lower()])).apply(lambda x: sorted(x))
        final['masterSymbol'] = final['symbol_sort'].apply(lambda x: x[0]+'-'+x[1])
        final['timestamp'] = dt.datetime.now().strftime(
            "%m/%d/%Y %H:%M:%S")

        '''export raw data'''
        last = pd.read_csv(r'P:\11_CWP Alternative\cwp alt\resea'+ \
                          r'rch\alphaRaw\Crypto\aprMaster\allAprs.csv',
                          index_col=None)
        final2 = pd.concat([last, final])
        
        final2.drop([
            
            'ust_divisor',
            'ustMultiplier',
            'symbol_sort',
            
            
            
            ], axis=1).to_csv(r'P:\11_CWP Alternative\cwp alt\resea'+ \
                          r'rch\alphaRaw\Crypto\aprMaster\allAprs.csv',
                          index=False)
             
        '''create grid and calculate volume dominance'''
        ts = final[final['dex'] =='Terraswap'][['dex','masterSymbol', 'symbol0',
                                    'symbol1', 'ustPoolVolume7d', 'apr7d',
                                    ]]
        ts.set_index('masterSymbol', inplace=True)
        astro = final[final['dex'] =='Astroport'][['dex', 'masterSymbol', 'symbol0',
                                    'symbol1', 'ustPoolVolume7d', 'apr7d']]
        astro.set_index('masterSymbol', inplace=True)
        loop = final[final['dex'] =='Loop'][['dex','masterSymbol', 'symbol0',
                                    'symbol1', 'ustPoolVolume7d', 'apr7d']]
        loop.set_index('masterSymbol', inplace=True)
        

        grid = pd.concat([ts, astro, loop]).sort_index().reset_index()
        ids = grid['masterSymbol']
        grid = grid[ids.isin(ids[ids.duplicated()])].sort_values(
            'masterSymbol').set_index('masterSymbol')
        

        grids = []
        for i,l in zip(grid.index.unique(), np.arange(len(grid.index.unique()))):
            row = grid.loc[i].T.loc[['dex', 'apr7d', 'ustPoolVolume7d']]
            sym = row.columns[0]
            row.columns = row.loc['dex'].values
            row.drop('dex', axis=0, inplace=True)
            
        
            try:
                row.loc['apr7d', 'Astroport Volume Dominance'] = row.loc[
                    'ustPoolVolume7d', 'Astroport']/\
                    row.loc['ustPoolVolume7d'].sum()
            except:
                print('no astroport')
                
            
            try:
                row.loc['apr7d', 'Terraswap Volume Dominance'] = row.loc[
                    'ustPoolVolume7d', 'Terraswap']/\
                    row.loc['ustPoolVolume7d'].sum()
            except:
                print('no terraswap')
               
            
            try:
                row.loc['apr7d', 'Loop Volume Dominance'] = row.loc[
                    'ustPoolVolume7d', 'Loop']/\
                    row.loc['ustPoolVolume7d'].sum()
            except:
                print('no loop')
               
            
            row.loc[:, 'Symbol'] = sym
            
            grids.append(pd.DataFrame(row.iloc[0]).T)
            
        final_grid = pd.concat(grids)
        final_grid = final_grid[list(final_grid.columns.drop('Symbol'))+['Symbol']]
        final_grid.iloc[:,:-1] = ((final_grid.iloc[:,:-1].astype(
            float)*100).round(2))
        final_grid = final_grid.reset_index(drop=True)
        
        final_grid['timestamp'] = dt.datetime.now().strftime(
            "%m/%d/%Y %H:%M:%S")
        
    
        last = pd.read_csv(r'P:\11_CWP Alternative\cwp alt\resea'+ \
                          r'rch\alphaRaw\Crypto\aprMaster\aprs.csv',
                          index_col=None)
        final_grid_concat = pd.concat([last, final_grid])
        final_grid_concat.to_csv(r'P:\11_CWP Alternative\cwp alt\resea'+ \
                          r'rch\alphaRaw\Crypto\aprMaster\aprs.csv',
                          index=False)
        return final_grid
    
    def pool_info(self):


        dfsm = []
        errors = []
        t = int(time.time())

        url = 'https://terraswap-graph.terra.dev/graphql'

        timestamps = list(np.arange(1595981097, t, 18880000)) + [t]

        for i in list(terraHelper.terra_pairs_names().keys()):

            dfsa = []

            for ts in np.arange(len(timestamps))[1:]:

                time.sleep(2)
                query = '''{
                  pair(pairAddress: "terra1tnd") {
                          token0{
                            tokenAddress,
                            symbol},            
                  				token1{              
                            tokenAddress,              
                            symbol},
        historicalData(to: 1635181097a ,from: 1595981097a ,interval: DAY){ 
                            timestamp,
                    				volumeUST,
                    				liquidityUST,
                    				token0Price,
                    				token1Price
              		}}}'''.replace('1635181097a', str(timestamps[ts])).replace(
                        '1595981097a', str(timestamps[ts-1])).replace(
                    'terra1tnd', i)
                try:
                    r = requests.post(url, json={'query': query})
                    hd = json.loads(r.text)['data']['pair']['historicalData']
                    if len(hd) > 0:
                        df = pd.DataFrame.from_dict(hd)
                        df['ticker'] = terraHelper.terra_pairs_names()[i]
                        df['date'] = df[
                            'timestamp'].apply(lambda x: dt.datetime.utcfromtimestamp(x))
                        dfsa.append(df)
                except:
                    print('error')

                    errors.append(terraHelper.terra_pairs_names()[i])
            try:
                dfsm.append(pd.concat(dfsa))
            except:
                pass

        final = pd.concat(dfsm)
        final['liquidityUST'] = final['liquidityUST'].apply(
            lambda x: int(x)/1000000)
        final['token0Price'] = final['token0Price'].apply(
            lambda x: float(x))
        final['token1Price'] = final['token1Price'].apply(
            lambda x: float(x))
        #Value of LUNA in UST, Value of Luna in KRT, Value of 
        final['token0Price'] = np.where(final['ticker'].isin(['LUNA-UST', 'LUNA-KRT',
                                                              'LUNA-BLUNA']), final['token0Price'],
                                        final['token1Price'])
        final = final.sort_values(['ticker', 'date'])
        voldf = final[['date', 'ticker', 'volumeUST']]
        lqddf = final[['date', 'ticker', 'liquidityUST']]
        pricedf = final[['date', 'ticker', 'token0Price']]

        final = final.set_index('ticker')
        final['tvl_return'] = final.groupby(
            level=0)['liquidityUST'].pct_change(45)
        final['price_return'] = final.groupby(
            level=0)['token0Price'].pct_change(45)


        # lqdlasts = lqddf.groupby('ticker')[['liquidityUST']].last()
        # lqdlasts.to_excel(r'P:\11_CWP Alternative\cwp' +
        #                   r' alt\research\alphaRaw\Data\PoolData\lastTVLs.xlsx')
        # lqddf.to_excel(r'P:\11_CWP Alternative\cwp' +
        #                r' alt\research\alphaRaw\Data\PoolData\TVLs.xlsx')
        # voldf.to_excel(r'P:\11_CWP Alternative\cwp' +
        #                r' alt\research\alphaRaw\Data\PoolData\Volumes.xlsx')
        # pricedf.to_excel(r'P:\11_CWP Alternative\cwp' +
        #                  r' alt\research\alphaRaw\Data\PoolData\Prices.xlsx')
        
        return [lqddf, voldf, pricedf]
                                              
    def getBlockTimestamp(self, height=''):

        value = pd.to_datetime(requests.get(
            'https://lcd.terra.dev/blocks/{1}'.replace('{1}',
                                                       str(height))).json()['block']['header']['time'])
        return value

    def anchorAPY(self):

        anchor_data = []

        url = 'https://api.anchorprotocol.com/api/v1/market/ust/1d'
        r = requests.get(url)

        df = pd.DataFrame([], columns=['timestamp', 'height', 'deposit_apr'])
        for i in r.json():

            height = i['height']
            df.loc[height, 'timestamp'] = i['timestamp']
            df.loc[height, 'height'] = i['height']
            df.loc[height, 'deposit_apr'] = i['deposit_apy']
            
        last = pd.read_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\AnchorDepositAPY.xlsx')
        last['apr'] = last['apr'].astype(float)
        last['timestamp'] = pd.to_datetime(last['timestamp'])

        df['timestamp'] = df['timestamp'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x/1000))
        df.rename(columns={'deposit_apr': 'apr'}, inplace=True)
        df.drop('height', axis=1, inplace=True)
        df['ticker'] = 'depositAPR'
        df['apr'] = df['apr'].astype(float)
        
        df = pd.concat([last, df]).drop_duplicates(['timestamp'])
        df.to_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\AnchorDepositAPY.xlsx',
                    index=False)
        
        anchor_data.append(df)

        url = 'https://api.anchorprotocol.com/api/v1/market/ust/1d'
        r = requests.get(url)
        # const distributionAPY = ancEmissionRate.mul(blocksPerYear).mul(ancPrice).div(totalLiabilities)
        url2 = 'https://api.anchorprotocol.com/api/v1/anc/1d'
        r2 = requests.get(url2)

        df = pd.DataFrame([], columns=['timestamp', 'height',
                                       'total_liabilities', 'anc', 'anc_emission_rate'])
        for i, i2 in zip(r.json(), r2.json()):

            height = i['height']
            df.loc[height, 'timestamp'] = i['timestamp']
            df.loc[height, 'height'] = i['height']
            df.loc[height, 'total_liabilities'] = i['total_liabilities']
            df.loc[height, 'anc_emission_rate'] = i['anc_emission_rate']
            df.loc[height, 'anc'] = i2['anc_price']
        
        df['distribution_apy'] = df['anc_emission_rate'].astype(
            float)*df['anc'].astype(float)*4656810/df['total_liabilities'].astype(float)
        df['timestamp'] = df['timestamp'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x/1000))
            
        last = pd.read_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\AnchorDistributionAPY.xlsx')
        last['distribution_apy'] = last['distribution_apy'].astype(float)
        
        df = pd.concat([last, df]).drop_duplicates(['timestamp'])
        df.to_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\AnchorDistributionAPY.xlsx',
                    index=False)

        df.rename(columns={'distribution_apy': 'apr'}, inplace=True)
        df.drop('height', axis=1, inplace=True)
        df['ticker'] = 'distributionAPR'
    

        anchor_data.append(df[['timestamp', 'ticker', 'apr']])

     
        
        url = 'https://api.anchorprotocol.com/api/v1/market/ust/1d'
        r = requests.get(url)

        df = pd.DataFrame([], columns=['timestamp', 'height', 'borrow_rate'])
        for i in r.json():

            height = i['height']
            df.loc[height, 'timestamp'] = i['timestamp']
            df.loc[height, 'height'] = i['height']
            df.loc[height, 'borrow_rate'] = i['borrow_rate']
        

        last = pd.read_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\AnchorBorrowAPY.xlsx')
        last['apr'] = last['apr'].astype(float)
        last['timestamp'] = pd.to_datetime(last['timestamp'])

        df['timestamp'] = df['timestamp'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x/1000))
        df['borrow_rate'] = df['borrow_rate'].astype(float)*4656810
        df.rename(columns={'borrow_rate': 'apr'}, inplace=True)
        df.drop('height', axis=1, inplace=True)
        df['ticker'] = 'borrowAPR'
        df['apr'] = df['apr'].astype(float)
        
        df = pd.concat([last, df]).drop_duplicates(['timestamp'])
        df.to_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\AnchorBorrowAPY.xlsx',
                    index=False)
        
        anchor_data.append(df)
        

        #############################

        distributiondf = pd.read_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\AnchorDistributionAPY.xlsx')
        distributiondf['height'] = distributiondf['height'].round(decimals=-4)
        distributiondf.set_index('timestamp', inplace=True)
        borrowdf = pd.read_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\AnchorBorrowAPY.xlsx')
        borrowdf.set_index('timestamp', inplace=True)

        final = borrowdf.join(distributiondf.drop(
            'total_liabilities', axis=1)).dropna()
        final['net_apr'] = final['distribution_apy']-final['apr']
        final.to_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\Anchor_Net_Borrow_APY.xlsx')
        
        final.drop('apr', axis=1, inplace=True)
        final.rename(columns={'net_apr': 'apr'}, inplace=True)
        # final['timestamp'] = final['date'].apply(
        #     lambda x: dt.datetime.utcfromtimestamp(x//1000))
        final['ticker'] = 'netApr'
        # final.drop('date', axis=1, inplace=True)
        anchor_data.append(final.reset_index()[['timestamp', 'ticker', 'apr']])

        ###############################
        '''Anchor bLUNA Supply'''
        df = pd.DataFrame([], columns=['height', 'total_supply'])

        curr_block = terraHelper.terra_current_block()
        
        for i in np.arange(4810000, curr_block, 10000):
                          
            count = True
            while count == True:
                try:
                    time.sleep(1)
                    
                    req = requests.get(
                    'https://lcd.terra.dev/wasm/contracts/terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp/store?query_msg=%7B%22token_info%22:%7B%7D%7D')

                    amount_current = float(json.loads(req.text)['result'][
                        'total_supply'][:-6])

                    req = requests.get("https://lcd.terra.dev/wasm/contracts/terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp/store?query_msg=%7B%22token_info%22:%7B%7D%7D&height={0}".format(i))
                    
                    height = json.loads(req.text)['height']
                    ts = float(json.loads(req.text)['result'][
                    'total_supply'][:-6])
                    
                    req2 = requests.get('https://lcd.terra.dev/wasm/contracts/terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp/store?query_msg=%7B%22token_info%22:%7B%7D%7D')
                    
                    amount_current2 = float(json.loads(req2.text)['result'][
                    'total_supply'][:-6])
    
                except:
                    print('there is an error')
                    pass
                    
                if (ts != amount_current) & (amount_current == amount_current2):
                    print('logic met, saving data')
                    count = False
                
                       
                    df.loc[height, 'height'] = height
                    df.loc[height, 'total_supply'] = ts
                    
        epoch = pd.to_datetime(dt.datetime.utcfromtimestamp(0), utc=True)
        df['date'] = df['height'].apply(lambda x: int(
            (self.getBlockTimestamp(x)-epoch).total_seconds()*1000))

        col4_bluna = pd.read_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\_col4\Anchor\Anchor_bLUNA_Supply.xlsx', index_col=0)
        col4_bluna.index = col4_bluna['height']

        df = pd.concat([col4_bluna, df])

        df.to_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\Anchor_bLUNA_Supply.xlsx')

        df.rename(columns={'total_supply': 'apr'}, inplace=True)
        df['timestamp'] = df['date'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x//1000))
        df['ticker'] = 'bLunaSupply'
        df.drop(['date', 'height'], axis=1, inplace=True)

        anchor_data.append(df)

        ################################
        '''Anchor bETH Supply'''
        df = pd.DataFrame([], columns=['height', 'total_supply'])

        curr_block = terraHelper.terra_current_block()
        
        for i in np.arange(4810000, curr_block, 10000):
                          
            count = True
            while count == True:
                try:
                    time.sleep(1)
                    
                    req = requests.get(
                    'https://lcd.terra.dev/wasm/contracts/terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun/store?query_msg=%7B%22token_info%22:%7B%7D%7D')

                    amount_current = float(json.loads(req.text)['result'][
                        'total_supply'][:-6])

                    req = requests.get("https://lcd.terra.dev/wasm/contracts/terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun/store?query_msg=%7B%22token_info%22:%7B%7D%7D&height={0}".format(i))
                    
                    height = json.loads(req.text)['height']
                    ts = float(json.loads(req.text)['result'][
                    'total_supply'][:-6])
                    
                    req2 = requests.get('https://lcd.terra.dev/wasm/contracts/terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun/store?query_msg=%7B%22token_info%22:%7B%7D%7D')
                    
                    amount_current2 = float(json.loads(req2.text)['result'][
                    'total_supply'][:-6])
    
                except:
                    print('there is an error')
                    pass
                    
                if (ts != amount_current) & (amount_current == amount_current2):
                    print('logic met, saving data')
                    count = False
                
                       
                    df.loc[height, 'height'] = height
                    df.loc[height, 'total_supply'] = ts
        

        epoch = pd.to_datetime(dt.datetime.utcfromtimestamp(0), utc=True)
        df['date'] = df['height'].apply(lambda x: int(
            (self.getBlockTimestamp(x)-epoch).total_seconds()*1000))

        col4_beth = pd.read_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\_col4\Anchor\Anchor_bETH_Supply.xlsx', index_col=0)
        col4_beth.index = col4_beth['height']

        df = pd.concat([col4_beth, df])

        df.to_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\Anchor_bETH_Supply.xlsx')

        df.rename(columns={'total_supply': 'apr'}, inplace=True)
        df['timestamp'] = df['date'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x//1000))
        df['ticker'] = 'bETHSupply'
        df.drop(['date', 'height'], axis=1, inplace=True)

        anchor_data.append(df)


        '''added these calls to get bLuna and bEth Collateral numbers 
        which are different then the total supplies that have above'''
        
        req = requests.get('https://api.anchorprotocol.com/api/v1/collaterals/1d')
        data = req.json()
        data = pd.DataFrame.from_dict(data)
        data['bluna_collateral'] = data['collaterals'].apply(lambda x: float(
            x[0]['collateral'])/1000000)
        data['beth_collateral'] = data['collaterals'].apply(lambda x: (
            float(x[1]['collateral'])/1000000) if len(x)>1 else 0)
        
        
        
        data.to_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\Anchor_Collaterals.xlsx',
            index=False)
        
        blunadf = data[['timestamp', 'bluna_collateral']]
        blunadf['ticker'] = 'blunaCollateral'
        blunadf['timestamp'] = blunadf['timestamp'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x//1000))
        blunadf.rename(columns={'bluna_collateral':'apr'}, inplace=True)
        bethdf = data[['timestamp', 'beth_collateral']]
        bethdf['ticker'] = 'bethCollateral'
        bethdf['timestamp'] = bethdf['timestamp'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x//1000))
        bethdf.rename(columns={'beth_collateral':'apr'}, inplace=True)
        
        anchor_data.append(blunadf)
        anchor_data.append(bethdf)

    
        '''height = json.loads(r.text)['height']
                ib = float(json.loads(r.text)['result'][
                    'prev_interest_buffer'][:-6])'''

        #################################
        '''Anchor Yeild Reserve'''
        df = pd.DataFrame([], columns=['height', 'interest_buffer'])

        curr_block = terraHelper.terra_current_block()
        
        for i in np.arange(4810000, curr_block, 10000):
                          
            count = True
            while count == True:
                try:
                    time.sleep(1)
                    
                    req = requests.get(
                    'https://lcd.terra.dev/wasm/contracts/terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8/store?query_msg=%7B%22epoch_state%22:%7B%7D%7D')

                    amount_current = float(json.loads(req.text)['result'][
                    'prev_interest_buffer'][:-6])

                    req = requests.get("https://lcd.terra.dev/wasm/contracts/terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8/store?query_msg=%7B%22epoch_state%22:%7B%7D%7D&height={0}".format(i))
                    
                    height = json.loads(req.text)['height']
                    ib = float(json.loads(req.text)['result'][
                    'prev_interest_buffer'][:-6])
                    
                    req2 = requests.get('https://lcd.terra.dev/wasm/contracts/terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8/store?query_msg=%7B%22epoch_state%22:%7B%7D%7D')
                    
                    amount_current2 = float(json.loads(req2.text)['result'][
                    'prev_interest_buffer'][:-6])
    
                except:
                    print('there is an error')
                    pass
                    
                if (ib != amount_current) & (amount_current == amount_current2):
                    print('logic met, saving data')
                    count = False
                
                       
                    df.loc[height, 'height'] = height
                    df.loc[height, 'interest_buffer'] = ib
        
        epoch = pd.to_datetime(dt.datetime.utcfromtimestamp(0), utc=True)

        df['date'] = df['height'].apply(lambda x: int(
            (self.getBlockTimestamp(x)-epoch).total_seconds()*1000))
        col4_ayr = pd.read_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\_col4\Anchor\Anchor_Yield_Reserve.xlsx', index_col=0)
        col4_ayr.index = col4_ayr['height']

        df = pd.concat([col4_ayr, df])

        df.to_excel(
            r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Anchor\Anchor_Yield_Reserve.xlsx')

        df.rename(columns={'interest_buffer': 'apr'}, inplace=True)
        df['timestamp'] = df['date'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x/1000))
        df['ticker'] = 'yieldReserve'
        df.drop(['date', 'height'], axis=1, inplace=True)

        anchor_data.append(df)

        anchordata = pd.concat(anchor_data)
        anchordata.rename(columns={'timestamp': 'date'}, inplace=True)
        anchordata.rename(columns={'apr': 'value'}, inplace=True)
        anchordata['value'] = anchordata['value'].astype(float)

        # anchordata.to_excel(r'C:\Users\rmathews\Downloads\anchor.xlsx')
        # anchordata = pd.read_excel(r'C:\Users\rmathews\Downloads\anchor.xlsx')
        
        tickers = anchordata['ticker'].unique().tolist()
        blankdict = {}
        for i in tickers:
            blankdict[i] = i
        anchor_dict = {"token":blankdict}

        return [anchordata, anchor_dict]

    def mirrorAprStats(self):

        date = (dt.datetime.now()-dt.timedelta(30)).strftime('%m/%d/%y')
        hist = self.pullHistory(start_date=date)
    
        '''roling 21 day long and short APR volatility rankings'''
        df_orig = hist[hist['apr'] != 0]
        df = df_orig.groupby('symbol').apply(
            lambda x: x['apr'].resample(
                '1D').ohlc())['close'].unstack().T.rolling(
                    21, min_periods=5).std()
        df['mean_vol'] = df.mean()
        df.name = 'Long APR Vols'
        df = df.iloc[-1].T
        df.name = 'Rolling 21 Day APR Volatilities'
        df.loc['mean_vol'] = df.mean()

        

        short_df = df_orig.groupby('symbol').apply(
            lambda x: x['apr_short'].resample(
                '1D').ohlc())['close'].unstack().T.rolling(
                    21, min_periods=5).std()
        short_df['mean_vol'] = short_df.mean()
        short_df.name = 'Short APR Vols'
        short_df = short_df.iloc[-1].T
        short_df.name = 'Rolling 21 Day Short APR Volatilities'
        short_df.loc['mean_vol'] = short_df.mean()
        short_df.drop('MIR')
        
        

        return [df, short_df]

    def alphatrackerUpdate(self):
        
        '''Update mSpreads Rolling 30 Day and Symbol Dict for SpreadTracker'''
        now = (dt.datetime.now()-dt.timedelta(30)).strftime('%m-%d-%Y')
        raw, ss = self.spreadStats(start_date=now)
        # ss.to_csv(
            # r'P:\11_CWP Alternative\cwp alt\rese' +
            # r'arch\alphaRaw\mAssetspreadstats.csv')
        pcts = pd.DataFrame(raw.groupby('symbol')['spread'].quantile(.05)).rename(
            columns={'spread': 'Historical 5th % Spread'}).join(pd.DataFrame(
                raw.groupby('symbol')['spread'].quantile(.95)).rename(columns={
                    'spread': 'Historical 95th % Spread'}))
        pcts = pcts.join(ss)
        pcts['Three SD'] = pcts['mean']+(pcts['std'] * 3)
        pcts['Neg Three SD'] = pcts['mean']-(pcts['std'] * 3)
        # pcts.round(4).to_csv(
        #     r'P:\11_CWP Alternative\cwp alt\rese' +
        #     r'arch\alphaRaw\mAssetspreadstats_plus.csv')
        spreads = pcts.round(4)

        token_dict = self.pullMirrorAssets()[['symbol', 'token']].reset_index(
            drop=True).set_index('symbol').drop('MIR').to_dict()

        return [spreads, token_dict]

    def nexus(self):

        url = 'https://api.nexusprotocol.app/graphql'

        query = ''' {
            getBAssetVaultAprRecords(limit: 500, offset: 0) {
              date
              bEthVaultApr
              bEthVaultManualApr
              bLunaVaultApr
              bLunaVaultManualApr
            }
          }'''
        r = requests.post(url, json={'query': query})
        hd = json.loads(r.text)['data']['getBAssetVaultAprRecords']

        df = pd.DataFrame.from_dict(hd)
        temps = []
        for i in df.columns[1:]:
            temp = df[['date', i]]
            temp['ticker'] = i
            temp.rename(columns={i: 'value'}, inplace=True)
            temps.append(temp)
        final = pd.concat(temps)

        final['date'] = pd.to_datetime(final['date'])
        final['value'] = final['value'].astype(float)
        
        last = pd.read_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Nexus\vaults.csv')
        last['date'] = pd.to_datetime(last['date'])
        last['value'] = last['value'].astype(float)
        
        final = pd.concat([final, last]).drop_duplicates(['ticker','date'])
        final.to_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Nexus\vaults.csv',
                     index=False)
        
        
        tickers = final['ticker'].unique().tolist()
        blankdict = {}
        for i in tickers:
            blankdict[i] = i
        nexusDict = {"token":blankdict}
        
        return [final, nexusDict]
             
    def luna_staking(self):
        
        stakingdfs = []
            
        url = 'https://fcd.terra.dev/v1/dashboard/staking_return'
        r = requests.get(url)
        
        df = pd.DataFrame([], columns=['timestamp', 'daily_return',
                                       'annualized_return'])
        for i in r.json():
        
            timestamp = i['datetime']
            df.loc[timestamp, 'timestamp'] = i['datetime']
            df.loc[timestamp, 'daily_return'] = float(i['dailyReturn'])
            df.loc[timestamp, 'annualized_return'] = float(i['annualizedReturn'])
   
        df['timestamp'] = df['timestamp'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x/1000))
    
        df.to_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Terra\LunaStaking.xlsx',
                    index=False)
        stakingdfs.append(df)

        dfs=[]
        url = 'https://fcd.terra.dev/v1/dashboard/tx_volume?'
        r = requests.get(url)
        
        
        for i in np.arange(len(r.json()['periodic'])):
                df = pd.DataFrame([], columns=['symbol', 'timestamp',
                                       'transaction_volume'])
                l = r.json()['periodic'][i]
                symbol = l['denom']
                data = l['data']
                for d in data:
                    timestamp = d['datetime']
                    print(timestamp)
                    print(symbol)
                    df.loc[timestamp, 'timestamp'] = d['datetime']
                    df.loc[timestamp, 'transaction_volume'] = float(d['txVolume'])/1000000
                    df.loc[timestamp, 'symbol'] = symbol
                
                def date_convert(x):
                    try:
                       return dt.datetime.utcfromtimestamp(x/1000)
                    except:
                        return x
                    
                df['timestamp'] = df['timestamp'].apply(
                    lambda x: date_convert(x))
                dfs.append(df)
      
    
        pd.concat(dfs).to_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Terra\LunaTxVolume.xlsx',
                    index=False)     
        stakingdfs.append(pd.concat(dfs))
    
        url = 'https://fcd.terra.dev/v1/dashboard/registered_accounts?'
        r = requests.get(url)
        
        df = pd.DataFrame([], columns=['timestamp', 'RegisteredAccounts'])
        for i in r.json()['periodic']:
        
            timestamp = i['datetime']
            df.loc[timestamp, 'timestamp'] = i['datetime']
            df.loc[timestamp, 'RegisteredAccounts'] = float(i['value'])
   
        df['timestamp'] = df['timestamp'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x/1000))
    
        df.to_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Terra\RegisteredAccounts.xlsx',
                    index=False)
        stakingdfs.append(df)
        
        url = 'https://fcd.terra.dev/v1/dashboard/block_rewards?'
        r = requests.get(url)
        
        df = pd.DataFrame([], columns=['timestamp', 'blockReward'])
        for i in r.json()['periodic']:
        
            timestamp = i['datetime']
            df.loc[timestamp, 'timestamp'] = i['datetime']
            df.loc[timestamp, 'blockReward'] = i['blockReward']
   
        df['timestamp'] = df['timestamp'].apply(
            lambda x: dt.datetime.utcfromtimestamp(x/1000))
    
        df.to_excel(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Terra\TaxRewards.xlsx',
                    index=False)
        stakingdfs.append(df)
        
        
        #ML info
        dashboard = pd.concat([stakingdfs[0].set_index('timestamp'),
                  stakingdfs[1][stakingdfs[1]['symbol']=='uusd'].set_index(
                      'timestamp')['transaction_volume'],
                  stakingdfs[2].set_index('timestamp'),
                  stakingdfs[3].set_index('timestamp')], axis=1) 
        dashboard.index = dashboard.reset_index()['timestamp'].dt.date
        dashboard.columns = ['staking_return_daily', 'staking_return_annualized',
                             'daily_transaction_volume', 'daily_registered_accounts',
                             'daily_block_rewards']
        dashboard['staking_return_daily_rank'] = dashboard['staking_return_daily'].rank(pct=True)
        dashboard['staking_return_annualized_rank'] = dashboard['staking_return_annualized'].rank(pct=True)
        dashboard['daily_transaction_volume_rank'] = dashboard['daily_transaction_volume'].rank(pct=True)
        dashboard['daily_registered_accounts_rank'] = dashboard['daily_registered_accounts'].rank(pct=True)
        
        dashboard.to_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Terra\dashboard.csv')
        
        
        pricedf = pd.read_excel(r'P:\11_CWP Alternative\cwp' +
                          r' alt\research\alphaRaw\Data\PoolData\Prices.xlsx')
        luna = pricedf[pricedf['ticker']=='LUNA-UST'].loc[:,'date':]
        luna.index = luna['date'].dt.date
        
        dashboard = dashboard.join(luna, how='inner')
        
        return dashboard
        
    def lunaMcRatio(self):
        
            #get luna mc from database 
            mc = pd.read_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Crypto\Terra\ustMC.csv',
                             index_col=0).dropna(axis=1)
            mc.index = pd.to_datetime(mc.index).date
            mc = mc[~mc.index.duplicated(keep='last')]
            
            #get ust price
            req = requests.get('https://eodhistoricaldata.com/api/eo' +
                               'd/{0}.CC?api_token=5f3e88'.format('UST-USD') +
                               'ded83498.58125490&period=d&fmt=json')
            df = pd.DataFrame.from_dict(req.json()).set_index('date')[['close']]
            df.index = pd.to_datetime(df.index)
            
            final = df.join(mc, how='outer')
            final['ustmc'] = final['close']*final['ust_circulating_supply']
            final['ustmc_1day_pct_change'] = final['ustmc'].pct_change()
            final['ustmc_1day_pct_change_rank'] = final['ustmc'].pct_change().rank(pct=True)
            final['ustmc_1day_pct_change_mean'] = final['ustmc'].pct_change().mean()
            final['ustmc_7day_pct_change'] = final['ustmc'].pct_change(7)
            final['ustmc_7day_pct_change_rank'] = final['ustmc'].pct_change(7).rank(pct=True)
            final['ustmc_7day_pct_change_mean'] = final['ustmc'].pct_change().mean()
            final['ustmc_1month_pct_change'] = final['ustmc'].pct_change(30)
            final['ustmc_1month_pct_change_rank'] = final['ustmc'].pct_change(30).rank(pct=True)
            final['ustmc_1month_pct_change_mean'] = final['ustmc'].pct_change(30).mean()
            final['ustmc_1year_pct_change'] = final['ustmc'].pct_change(365)
            final['ustmc_1year_pct_change_rank'] = final['ustmc'].pct_change(365).rank(pct=True)
            final['ustmc_1year_pct_change_mean'] = final['ustmc'].pct_change(365).mean()
            
            #get luna price
            req = requests.get('https://eodhistoricaldata.com/api/eo' +
                               'd/{0}.CC?api_token=5f3e88'.format('LUNA-USD') +
                               'ded83498.58125490&period=d&fmt=json')
            df = pd.DataFrame.from_dict(req.json()).set_index('date')[['close']]
            df.index = pd.to_datetime(df.index)
            
            final.rename(columns={'close':'ust_price'}, inplace=True)
            final = final.join(df, how='inner')
            final.rename(columns={'close':'luna_price'}, inplace=True)
            
            #get luna / ust market cap ratio
            final['luna_ustmc_ratio'] = final['luna_price']/final[
                'ustmc']
            #pct rank current ratio
            final['luna_ustmc_ratio_pct_rank'] = final['luna_ustmc_ratio'].rank(pct=True)
            #historical ratio average
            final['ratio_average'] = final['luna_ustmc_ratio'].mean()
            #luna 1 week returns
            final['luna_1week_returns'] = final['luna_price'].pct_change(7)
            #luna 1 week returns
            final['luna_1month_returns'] = final['luna_price'].pct_change(30)
            #luna 1 week returns
            final['luna_1year_returns'] = final['luna_price'].pct_change(365)
            #luna 1 week return ranks
            final['luna_1week_returns_pctrank'] = final['luna_price'].pct_change(7).rank(pct=True)
            #luna 1 week returns
            final['luna_1month_returns_pctrank'] = final['luna_price'].pct_change(30).rank(pct=True)
            #luna 1 week returns
            final['luna_1year_returns_pctrank'] = final['luna_price'].pct_change(365).rank(pct=True)
            
            
            #bring in terrastation dashboard
            dashboard = pd.read_csv(
                r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\Terra\dashboard.csv',
                index_col=0)
            dashboard.index = pd.to_datetime(dashboard.index)
            final = final.join(dashboard, how='outer')
            
            final.columns = ['UST Price ($)',
                             'UST Circulating Supply ($)',
                             'UST Market Cap ($)',
                             'UST Market Cap 1 Day Percent Change (%)',
                             'UST Market Cap 1 Day Percent Change Percentile Rank (%)',
                             'UST Market Cap 1 Day Percent Change Average (%)',
                             'UST Market Cap 7 Day Percent Change (%)',
                             'UST Market Cap 7 Day Percent Change Percentile Rank (%)',
                             'UST Market Cap 7 Day Percent Change Average (%)',
                             'UST Market Cap 1 Month Percent Change (%)',
                             'UST Market Cap 1 Month Percent Change Percentile Rank (%)',
                             'UST Market Cap 1 Month Percent Change Average (%)',
                             'UST Market Cap 1 Year Percent Change (%)',
                             'UST Market Cap 1 Year Percent Change Percentile Rank (%)',
                             'UST Market Cap 1 Year Percent Change Average (%)',
                             'LUNA Price ($)',
                             'LUNA UST Market Cap Ratio',
                             'LUNA UST Market Cap Ratio Percentile Rank',
                             'LUNA UST Market Cap Ratio Average',
                             'LUNA 1 Week Return (%)',
                             'LUNA 1 Month Return (%)',
                             'LUNA 1 Year Returns (%)',
                             'LUNA 1 Week Return Percentile Rank (%)',
                             'LUNA 1 Month Return Percentile Rank (%)',
                             'LUNA 1 Year Returns Percentile Rank (%)',
                             'LUNA Daily Staking Return (%)',
                             'LUNA Staking Return Annualized (%)',
                             'Daily UST Transaction Volume ($)',
                             'Daily Registered Accounts',
                             'Daily Terra Block Rewards',
                             'LUNA Daily Staking Return Percentile Rank (%)',
                             'LUNA Staking Return Annualized Percentile Rank (%)',
                             'Daily UST Transaction Volume Percentile Rank($)',
                             'Daily Registered Accounts Percentile Rank']
            final.drop(['UST Price ($)', 'UST Circulating Supply ($)',
                        'LUNA Price ($)'], axis=1, inplace=True)
            
            final.columns = [i.replace(' (%)','') for i in final.columns]
            
            try:
                cmc = terraHelper.coinmarketcaps()
                cmc_prev = pd.read_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\CoinMarketCap\dashboard.csv', index_col=0)[cmc.columns]
                cmc = pd.concat([cmc_prev, cmc])
                cmc['last_updated_date_only'] = pd.to_datetime(cmc['last_updated']).dt.date
                cmc = cmc.drop_duplicates(['last_updated_date_only', 'symbol'])
                cmc.to_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\CoinMarketCap\dashboard.csv', index=True)
                
                time.sleep(1)
                p = re.compile('(?<!\\\\)\'')
                time.sleep(1)
                cmc['quote_dict'] = cmc['quote'].apply(lambda x: json.loads(p.sub('\"', x)))
                cmc['Market Cap'] = cmc['quote_dict'].apply(lambda x: x['USD']['market_cap'])
                cmc['Market Cap Dominance'] = cmc['quote_dict'].apply(lambda x: float(x['USD']['market_cap_dominance'])/100)
            except:
                cmc = terraHelper.coinmarketcaps()
                cmc_prev = pd.read_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\CoinMarketCap\dashboard.csv', index_col=0)[cmc.columns]
                cmc = pd.concat([cmc_prev, cmc])
                cmc['last_updated_date_only'] = pd.to_datetime(cmc['last_updated']).dt.date
                cmc = cmc.drop_duplicates(['last_updated_date_only', 'symbol'])
                cmc.to_csv(r'P:\11_CWP Alternative\cwp alt\research\alphaRaw\Data\CoinMarketCap\dashboard.csv', index=True)
                
                time.sleep(1)
                p = re.compile('(?<!\\\\)\'')
                time.sleep(1)
                cmc['quote_dict'] = cmc['quote'].apply(lambda x: json.loads(p.sub('\"', x)))
                cmc['Market Cap'] = cmc['quote_dict'].apply(lambda x: x['USD']['market_cap'])
                cmc['Market Cap Dominance'] = cmc['quote_dict'].apply(lambda x: float(x['USD']['market_cap_dominance'])/100)
                
            
            temps = []
            for i in final.columns:
                temp = final[[i]].reset_index().rename(columns={i:'value', 'index':'date'})
                temp['ticker'] = i
                temp = temp.dropna()
                temps.append(temp)
                
            
            for sym in ['BTC', 'ETH', 'SOL', 'LUNA']:
                for i in ['Market Cap', 'Market Cap Dominance']:
                    temp = cmc[cmc['symbol']==sym][['last_updated', i]].rename(columns={i:'value', 'last_updated':'date'}).reset_index(drop=True)
                    temp['date'] = pd.to_datetime(temp['date'])
                    temp['ticker'] = sym + ' ' + i
                    temp = temp.dropna()
                    temps.append(temp)
                    
            for sym in ['BTC', 'ETH', 'SOL']:
                for i in ['Market Cap']:
                    temp = cmc[cmc['symbol']==sym][['last_updated', i]].rename(columns={i:'value', 'last_updated':'date'}).reset_index(drop=True)
                    temp['date'] = pd.to_datetime(temp['date']).dt.date
                    lunatemp = cmc[cmc['symbol']=='LUNA'][['last_updated', i]].rename(columns={i:'value', 'last_updated':'date'}).reset_index(drop=True)
                    lunatemp['date'] = pd.to_datetime(lunatemp['date']).dt.date
                    temp = temp.set_index('date').join(lunatemp.set_index('date'), on='date', rsuffix = '_luna')
                    temp['value'] = (temp['value_luna']/temp['value']).round(4)
                    temp = temp[['value']].reset_index()
                    temp['date'] = pd.to_datetime(temp['date'])
                    temp['ticker'] = 'LUNA - '+ sym + ' ' + i + ' ' + 'Ratio'
                    temp = temp.dropna()
                    temps.append(temp)
    
                
            master = pd.concat(temps)
            
            
            tickers = master[~master['ticker'].isin(['UST Market Cap 7 Day Percent Change Average',
                                   'UST Market Cap 1 Day Percent Change Average',
                        'UST Market Cap 1 Month Percent Change Average',
                        'UST Market Cap 1 Year Percent Change Average',
                        'LUNA UST Market Cap Ratio Average',
                        'LUNA UST Market Cap Ratio',
                        'Daily Terra Block Rewards',
                        
                        ])]['ticker'].unique().tolist()
            blankdict = {}
            for i in tickers:
                blankdict[i] = i
            dashboardDict = {"token":blankdict}
        
            
            return [master, dashboardDict]
            
            

                    
           
            
                    
                
            
                 
         
                 


        
        
        
        
        
        
        
        
    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
