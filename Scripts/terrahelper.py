import pandas as pd
from string import Template
import requests
import json
import datetime as dt


#for coinmarketcap
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects


class terraHelper(object):
    
    def __init__(self, account):
        self.account = str(account)

    
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
    
    #this gets holdings from mirror my page
    def maccount_holding_info(self, mtoken_address,
                          url='https://mantle.terra.dev/'):        
        query =  Template('''{
                WasmContractsContractAddressStore(
                ContractAddress: $mtokenaddress
                QueryMsg: $msg
              ) {
                Height
                Result
                __typename
              }
            }
            ''')
            
        message = '''"{\"balance\":{\"address\":\"{0}\"}}"'''.replace(
                '{0}',self.account) 
        
        query = query.substitute(msg=message, mtokenaddress=mtoken_address)
              
        return query
              
    
    #returns my stakes, bond amounts, and pending rewards (rewards are 0?)
    #the bond amount is number of LP tokens staked (does not include unstaked)
    def maccount_stake_info(self, url='https://mantle.terra.dev/'):
        query =  Template("""query {
            WasmContractsContractAddressStore(
            ContractAddress: "terra17f7zu97865jmknk7p2glqvxzhduk78772ezac5"
            QueryMsg: $msg
          ) {
            Height
            Result
            __typename
          }
        }
        """)

        message = '''"{\\"reward_info\\":{\\"staker\\":\\"{0}\\"}}"'''.replace(
                '{0}',self.account)     
        query = query.substitute(msg=message)
              
        return query
    
    #token pair address comes from mirror assets call
    # gets pool statistics, total amount lp tokens, Token Amount, UST Amount
    def mirror_pool_info(self, token_pair, url='https://mantle.terra.dev/'):
        query =  Template("""query {
            WasmContractsContractAddressStore(
            ContractAddress: $tp
            QueryMsg: $msg
          ) {
            Height
            Result
            __typename
          }
        }
        """)
        
        message = '''"{\"pool\":{}}"'''
        query = query.substitute(msg=message)
        query = query.substitute(tp=token_pair)
              
        return query
    
    #returns staking info for mirror token contracts like total bond amount
    def mirror_stake_info(self, mtoken_address, url='https://mantle.terra.dev/'):
        query =  Template("""query {
            WasmContractsContractAddressStore(
            ContractAddress: "terra17f7zu97865jmknk7p2glqvxzhduk78772ezac5"
            QueryMsg: $msg
          ) {
            Height
            Result
            __typename
          }
        }
        """)

        message = '''"{\"pool_info\":{\"asset_token\":\"{0}\"}}"'''.replace(
                '{0}', mtoken_address)     
        query = query.substitute(msg=message)
              
        return query
    
    #token pair address comes from mirror assets call 
    #returns staking info for mirror token contracts like total bond amount
    def maccount_unstaked_lp(self, lptoken, url='https://mantle.terra.dev/'):
        query =  Template("""query {
            WasmContractsContractAddressStore(
            ContractAddress: $lpt
            QueryMsg: $msg
          ) {
            Height
            Result
            __typename
          }
        }
        """)

        message = '''"{\"balance\":{\"address\":\"{0}\"}}"'''.replace(
                '{0}', self.account)     
        query = query.substitute(msg=message)
        query = query.substitute(lpt=lptoken)
              
        return query
    
    def mint_configs(self, mtoken_address, url='https://mantle.terra.dev/'):
        query =  Template("""query {
            WasmContractsContractAddressStore(
            ContractAddress: "terra1wfz7h3aqf4cjmjcvc6s8lxdhh7k30nkczyf0mj"
            QueryMsg: $msg
          ) {
            Height
            Result
            __typename
          }
        }
        """)

        message = '''"{\"asset_config\":{\"asset_token\":\"{0}\"}}"'''.replace(
                '{0}', mtoken_address)     
        query = query.substitute(msg=message)
              
        return query
    

        
    
    
    def terra_pairs():
         
         req = requests.get('https://assets.terra.money/cw20/pairs.json')
         pairs = list(req.json()['mainnet'].keys())
         pairs = pairs +['terra1c0afrdc5253tkp5wt7rxhuj42xwyf2lcre0s7c',
        'terra1c5swgtnuunpf75klq5uztynurazuwqf0mmmcyy',
        'terra1zvn8z6y8u2ndwvsjhtpsjsghk6pa6ugwzxp6vx',
        'terra14zhkur7l7ut7tx6kvj28fp5q982lrqns59mnp3'
        ]
         
         return pairs
     
    def terra_pairs_names():
        
        req = requests.get('https://assets.terra.money/cw20/pairs.json')
        
        # for i in pairs.index:
        #     print(i)
        
        '''current pairs'''
        names = {
        'terra1zw0kfxrxgrs5l087mjm79hcmj3y8z6tljuhpmc':'LUNA-KRT',
        'terra1tndcaqxkpc5ce9qee5ggqf430mr2z3pefe5wj6':'LUNA-UST',
        'terra1gm5p3ner9x9xpwugn9sp6gvhd0lwrtkyrecdn3':'ANC-UST',
        'terra1jxazgm67et0ce260kvrpfv50acuushpjsz2y0p':'LUNA-bLUNA',
        'terra1amv303y8kzxuegvurh0gug2xe9wkgj65enq2ux':'MIR-UST',
        'terra1774f8rwx76k7ruy0gqnzq25wh7lmd72eg6eqp5':'mAAPL-UST',
        'terra1gq7lq389w4dxqtkxj03wp0fvz0cemj0ek5wwmm':'mABNB-UST',
        'terra1uenpalqlmfaf4efgtqsvzpa3gh898d9h2a232g':'mAMC-UST',
        'terra18cxcwv0theanknfztzww8ft9pzfgkmf2xrqy23':'mAMD-UST',
        'terra1vkvmvnmex90wanque26mjvay2mdtf0rz57fm6d':'mAMZN-UST',
        'terra1afdz4l9vsqddwmjqxmel99atu4rwscpfjm4yfp':'mBABA-UST',
        'terra1prfcyujt9nsn5kfj5n925sfd737r2n8tk5lmpv':'mBTC-UST',
        'terra1h7t2yq00rxs8a78nyrnhlvp0ewu8vnfnx5efsl':'mCOIN-UST',
        'terra17rvtq0mjagh37kcmm4lmpz95ukxwhcrrltgnvc':'mDOT-UST',
        'terra14fyt2g3umeatsr4j4g2rs8ca0jceu3k0mcs7ry':'mETH-UST',
        'terra1yl2atgxw422qxahm02p364wtgu7gmeya237pcs':'mFB-UST',
        'terra1ze5f2lm5clq2cdd9y2ve3lglfrq6ap8cqncld8':'mGLXY-UST',
        'terra17eakdtane6d2y7y6v0s79drq7gnhzqan48kxw7':'mGME-UST',
        'terra1u56eamzkwzpm696hae4kl92jm6xxztar9uhkea':'mGOOGL-UST',
        'terra108ukjf6ekezuc52t9keernlqxtmzpj4wf7rx0h':'mGS-UST',
        'terra15kkctr4eug9txq7v6ks6026yd4zjkrm3mc0nkp':'mIAU-UST',
        'terra10ypv4vq67ns54t5ur3krkx37th7j58paev0qhd':'mMSFT-UST',
        'terra1yppvuda72pvmxd727knemvzsuergtslj486rdq':'mNFLX-UST',
        'terra1dkc8075nv34k2fu6xn6wcgrqlewup2qtkr4ymu':'mQQQ-UST',
        'terra1f6d9mhrsl5t6yxqnr4rgfusjlt3gfwxdveeyuy':'mSLV-UST',
        'terra14hklnm2ssaexjwkcfhyyyzvpmhpwx6x6lpy39s':'mSPY-UST',
        'terra1u3pknaazmmudfwxsclcfg3zy74s3zd3anc5m52':'mSQ-UST',
        'terra1pdxyk2gkykaraynmrgjfq2uu7r9pf5v8x7k4xk':'mTSLA-UST',
        'terra1ea9js3y4l7vy0h46k4e5r5ykkk08zc3fx7v4t8':'mTWTR-UST',
        'terra1zey9knmvs2frfrjnf4cfv4prc4ts3mrsefstrj':'mUSO-UST',
        'terra1krny2jc0tpkzeqfmswm7ss8smtddxqm3mxxsjm':'mVIXY-UST',
        'terra178jydtjvj4gw8earkgnqc80c3hrmqj4kw2welz':'MINE-UST',
        'terra1pn20mcwnmeyxf68vpt3cyel3n57qm9mp289jta':'LOTA-UST',
        'terra1tn8ejzw8kpuc87nu42f6qeyen4c7qy35tl8t20':'SPEC-UST',
        'terra19pg6d7rrndg4z4t0jhcd7z9nhl3p5ygqttxjll':'STT-UST',
        'terra1etdkg9p0fkl8zal6ecp98kypd32q8k3ryced9d':'TWD-UST',
        'terra163pkeeuwxzr0yhndf8xd2jprm9hrtk59xf7nqf':'PSI-UST',
        'terra1e59utusv5rspqsu8t37h5w887d9rdykljedxw0':'VKR-UST',
        'terra1c0afrdc5253tkp5wt7rxhuj42xwyf2lcre0s7c':'BETH-UST',
        'terra1c5swgtnuunpf75klq5uztynurazuwqf0mmmcyy':'METH-BETH',
        'terra12mzh5cp6tgc65t2cqku5zvkjj8xjtuv5v9whyd':'MIAW-UST',
        'terra1zz39wfyyqt4tjz7dz6p7s9c8pwmcw2xzde3xl8':'DPH-UST',
        'terra1zvn8z6y8u2ndwvsjhtpsjsghk6pa6ugwzxp6vx':'nLUNA-PSI',
        'terra14zhkur7l7ut7tx6kvj28fp5q982lrqns59mnp3':'nETH-PSI',
        'terra1z6tp0ruxvynsx5r9mmcc2wcezz9ey9pmrw5r8g':'ORION-UST',
        'terra1mv3tksqwfextmnejw8s7ada9qu3pwav098qfxu':'wewstETH-UST',
        'terra1k30c303e059kqhz705a77kgdhr2ndldfdxjvsr':'wsstSOL-UST',
        'terra1tq4mammgkqrxrmcfhwdz59mwvwf4qgy6rdrt46':'SCRT-UST',
        'terra140dcz7t06l4llh38u62wh8rcm0gus9dd93envl':'SCRT-LUNA',
        'terra167gwjhv4mrs0fqj0q5tejyl6cz6qc2cl95z530':'PSI-bPSI',
        'terra1jzqlw8mfau9ewr7lufqkrpgfzk4legz9zx306p':'TLAND-UST',
        'terra1vayuttjw6z4hk5r734z9qatgs8vp6r5a2t043p':'LUNI-UST',
        'terra1mz0p4wzz5tmethu7rca2jjrw077hv2ypj7h06z':'ORNE-UST',
        'terra1zrzy688j8g6446jzd88vzjzqtywh6xavww92hy':'LUNA-LUNAX',
        'terra1zkyrfyq7x9v5vqnnrznn3kvj35az4f6jxftrl2':'KUJI-UST',
        'terra19fjaurx28dq4wgnf9fv3qg0lwldcln3jqafzm6':'PLY-UST',
        'terra1stdzf28wlq7llzfecse366r657rtdh6wtrdfk2':'TSHIBA-UST',
        'terra1hqnk9expq3k4la2ruzdnyapgndntec4fztdyln':'TNS-UST',
        'terra1dqjt2jm908qaayw5hdl36w50sshmgem37suawr':'XRUNE-UST'
        }
        
        
            
        return names
    
    def astroportLPs():
        
        dictionary = {
            
          "astro_multisig": "terra1c7m6j8ya58a2fkkptn8fgudx8sqjqvc8azq0ex",
          "astro_token_address": "terra1xj49zyqrwpv5k928jwfpfy2ha668nwdgkwlrg3",
          "astro_builder_unlock_contract": "terra1fh27l8h4s0tfx9ykqxq5efq4xx88f06x6clwmr",
          "astroport_factory_address": "terra1fnywlw4edny3vw44x04xd67uzkdqluymgreu7g",
          "router_address": "terra16t7dpwwgx9n3lq6l6te3753lsjqwhxwpday9zx",
          "maker_address": "terra12u7hcmpltazmmnq0fvyl225usn3fy6qqlp05w0",
          "vesting_address": "terra1hncazf652xa0gpcwupxfj6k4kl4k4qg64yzjyf",
          "generator_address": "terra1zgrx9jjqrfye8swykfgmd6hpde60j0nszzupp9",
          
          "luna_ust_astroport_pool": "terra1m6ywlgn6wrjuagcmmezzz2a029gtldhey5k552",
          "luna_ust_astroport_lp_token_address": "terra1m24f7k4g66gnh9f7uncp32p722v0kyt3q4l3u5",
          "anc_ust_astroport_pool": "terra1qr2k6yjjd5p2kaewqvg93ag74k6gyjr7re37fs",
          "anc_ust_astroport_lp_token_address": "terra1wmaty65yt7mjw6fjfymkd9zsm6atsq82d9arcd",
          "mir_ust_astroport_pool": "terra143xxfw5xf62d5m32k3t4eu9s82ccw80lcprzl9",
          "mir_ust_astroport_lp_token_address": "terra17trxzqjetl0q6xxep0s2w743dhw2cay0x47puc",
          "orion_ust_astroport_pool": "terra1mxyp5z27xxgmv70xpqjk7jvfq54as9dfzug74m",
          "orion_ust_astroport_lp_token_address": "terra1w80npmymwhdtvcmrg44xmqqdnufu3gyfaytr9z",
          "stt_ust_astroport_pool": "terra1m95udvvdame93kl6j2mk8d03kc982wqgr75jsr",
          "stt_ust_astroport_lp_token_address": "terra14p4srhzd5zng8vghly5artly0s53dmryvg3qc6",
          "vkr_ust_astroport_pool": "terra15s2wgdeqhuc4gfg7sfjyaep5cch38mwtzmwqrx",
          "vkr_ust_astroport_lp_token_address": "terra1lw36qqz72mxajrfgkv24lahudq3ehmkpc305yc",
          "mine_ust_astroport_pool": "terra134m8n2epp0n40qr08qsvvrzycn2zq4zcpmue48",
          "mine_ust_astroport_lp_token_address": "terra16unvjel8vvtanxjpw49ehvga5qjlstn8c826qe",
          "psi_ust_astroport_pool": "terra1v5ct2tuhfqd0tf8z0wwengh4fg77kaczgf6gtx",
          "psi_ust_astroport_lp_token_address": "terra1cspx9menzglmn7xt3tcn8v8lg6gu9r50d7lnve",
          "apollo_ust_astroport_pool": "terra1zpnhtf9h5s7ze2ewlqyer83sr4043qcq64zfc4",
          "apollo_ust_astroport_lp_token_address": "terra1zuktmswe9zjck0xdpw2k79t0crjk86fljv2rm0",
          "bluna_luna_astroport_pool": "terra1j66jatn3k50hjtg2xemnjm8s7y8dws9xqa5y8w",
          "bluna_luna_astroport_lp_token_address": "terra1htw7hm40ch0hacm8qpgd24sus4h0tq3hsseatl",
        
          "avax_luna_astroport_pool": "terra16jaryra6dgfvkd3gqr5tcpy3p2s37stpa9sk7s",
          "avax_luna_astroport_lp_token": "terra1pme6xgsr0f6sdcq5gm2qs8dsc2v0h6gqzs8js5",
          "bnb_luna_astroport_pool": "terra1tehmd65kyleuwuf3a362mhnupkpza29vd86sml",
          "bnb_luna_astroport_lp_token": "terra1wk0lev7qneurzp2dzcauh2ktctwx6v079uvn7w",
          "eth_luan_astroport_pool":  "terra1m32zs8725j9jzvva7zmytzasj392wpss63j2v0",
          "eth_luna_astroport_lp_token": "terra1lmlv43teqcty6xldtg4f40sghnd2f8ehjz0qpk",
          "sol_luna_astroport_pool": "terra16e5tgdxre44gvmjuu3ulsa64kc6eku4972yjp3",
          "sol_luna_astroport_lp_token": "terra1x6jws8lh505gw7dl67a7qq077g9mn3cjj3v22r",
          "matic_luna_astroport_pool": "terra1wr07qcmfqz2vxhcfr6k8xv8eh5es7u9mv2z07x",
          "matic_luna_astroport_lp_token": "terra1n32fdqslpyug72zrcv8gwq37vjj0mxhy9p4g7z",
        
          "astro_ust_astroport_pool": "terra1l7xu2rl3c7qmtx3r5sd2tz25glf6jh8ul7aag7",
          "astro_ust_astroport_lp_token_address": "terra17n5sunn88hpy965mzvt3079fqx3rttnplg779g"
         }
        
        dictionary_names = {
        
        'terra1l7xu2rl3c7qmtx3r5sd2tz25glf6jh8ul7aag7':'ASTRO-UST Astroport',
        'terra1m6ywlgn6wrjuagcmmezzz2a029gtldhey5k552':'LUNA-UST Astroport',
        'terra1qr2k6yjjd5p2kaewqvg93ag74k6gyjr7re37fs':'ANC-UST Astroport',
        'terra143xxfw5xf62d5m32k3t4eu9s82ccw80lcprzl9':'MIR-UST Astroport',
        'terra1mxyp5z27xxgmv70xpqjk7jvfq54as9dfzug74m':'ORION-UST Astroport',
        'terra1m95udvvdame93kl6j2mk8d03kc982wqgr75jsr':'STT-UST Astroport',
        'terra15s2wgdeqhuc4gfg7sfjyaep5cch38mwtzmwqrx':'VKR-UST Astroport',
        'terra134m8n2epp0n40qr08qsvvrzycn2zq4zcpmue48':'MINE-UST Astroport',
        'terra1v5ct2tuhfqd0tf8z0wwengh4fg77kaczgf6gtx':'PSI-UST Astroport',
        'terra1zpnhtf9h5s7ze2ewlqyer83sr4043qcq64zfc4':'APOLLO-UST Astroport',
        'terra1j66jatn3k50hjtg2xemnjm8s7y8dws9xqa5y8w':'LUNA-bLUNA Astroport'
        
        }
        
        return dictionary, dictionary_names
        
     
    def terra_current_block():
        
        req = requests.get('https://lcd.terra.dev/wasm/contracts/terra1tn'+\
                     'dcaqxkpc5ce9qee5ggqf430mr2z3pefe5wj6/store?query_m'+\
                     'sg={%22pool%22:{}}')
        return int(req.json()['height'])
    
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
    
    def coinhall_terra_info():
        
        req = requests.get(
            'https://api.coinhall.org/api/v1/charts/terra/pairs')
        data = req.json()
        
        return data
    
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
          'X-CMC_PRO_API_KEY': 'e9558ea8-4ca6-4dc2-8f79-06303f95bff2',
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        try:
          response = session.get(url, params=parameters)
          ids = json.loads(response.text)
          print(ids)
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
          'X-CMC_PRO_API_KEY': 'e9558ea8-4ca6-4dc2-8f79-06303f95bff2',
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        try:
          response = session.get(url, params=parameters)
          data = json.loads(response.text)
          print(data)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
          print(e)
          
          
        datadf = pd.DataFrame.from_dict(data['data']).set_index('id')
        idsdf = pd.DataFrame.from_dict(ids['data']).set_index('id')
        final2 = datadf.join(idsdf, rsuffix='_id')
        
       
        return final2
    
    def astro_emissions():
        
        data = {
            'LUNA-bLUNA':25000000,
            'ASTRO-UST':15000000,
            'LUNA-UST':13000000,
            'ANC-UST':10444444,
            'stlUNA-LUNA':9400000,
            'MINE-UST':3655556,
            'MIR-UST':3394444,
            'STT-UST':3394444,
            'ORION-UST':2611111,
            'PSI-UST':2350000,
            'APOLLO-UST':1827778,
            'nLUNA-PSI':1827778,
            'VKR-UST':1827778,
            'nETH-PSI':1566667,
            'stETH-UST':1566667,
            'stSOL-UST':1566667,
            'XDEFI-UST':1566667
            }
        
        return data
        
    
    def get_kujira_summary():
        
        req = requests.get('https://api.kujira.app/api/terra1vn5s4s7gpp4yu0mtad8recncyh2h2c6l4qesd6/borrowers/summary')
        data = req.json()
        data = pd.DataFrame.from_dict(data['summary'])
        
        return data
    
    def get_kujia_liquidations(limit=100):
         req = requests.get('https://api.kujira.app/api/terra1vn5s4s7gpp4yu0mtad8recncyh2h2c6l4qesd6/liquidations?limit={0}'.format(limit))
         return pd.DataFrame.from_dict(req.json()['liquidations'])

    
    def get_osmo_prices(x):
        
        # pool info
        req = requests.get('https://api-osmosis.imperator.co/pools/v1/all')
        data= req.json()

                
        df2s = []
        for i in [ '1', '560', '561', '562', '497', '498', '601', '604', '611', '584']:
            
           r = requests.get('https://api-osmosis.imperator.co/pools/v1/liquidity/{0}/chart'.format(i))
           liq = pd.DataFrame.from_dict(r.json()).set_index('time').rename(columns={'value':'liquidity'})
           r = requests.get('https://api-osmosis.imperator.co/pools/v1/volume/{0}/chart'.format(i))
           vol = pd.DataFrame.from_dict(r.json()).set_index('time').rename(columns={'value':'volume'})
           r = requests.get('https://api-osmosis.imperator.co/tokens/v1/historical/{0}/chart?range=50y'.format(i))
           price = pd.DataFrame.from_dict(r.json()).set_index('time').rename(columns={'value':'volume'})
           
           df2 = liq.join(vol)
           df2['symbol'] = data[i][0]['symbol'] + '-' + data[i][1]['symbol']
           df2['volume'] = df2['volume'].fillna(0)
           df2['7d_Volume'] = df2['volume'].rolling(7).sum()
           df2['fees'] = float(data[i][0]['fees'][:-1])/100
           df2['APR'] = ((df2['7d_Volume']*df2['fees'])/df2['liquidity'])*52
           df2['id'] = i
           df2[''] = ''
           
           df2.to_csv(r'C:\Users\rmathews\Downloads\{0}_{1}_{2}.csv'.format(data[i][0]['symbol'],
                                                                            data[i][1]['symbol'],
                                                                            i))
           df2s.append(df2)
           
        test = pd.concat(df2s).to_csv(r'C:\Users\rmathews\Downloads\all_osmo.csv')
        
    prices = []
    for i in ['OSMO', 'LUNA', 'SCRT', 'CMDX']:
            r = requests.get('https://api-osmosis.imperator.co/tokens/v1/historical/{0}/chart?range=50y'.format(i))
            price = pd.DataFrame.from_dict(r.json())
            price['symbol'] = i
            price['time'] = price['time'].apply(lambda x: dt.datetime.utcfromtimestamp(x))
            prices.append(price)
            
    pd.concat(prices).to_csv(r'C:\Users\rmathews\Downloads\all_osmo_prices.csv')
            
          
        
        
        
    
    
         
         
         
         
    








