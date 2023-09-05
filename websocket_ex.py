import os
import rel
import time
import json
import numpy as np
import pandas as pd
import websocket

from websocket import WebSocketApp
from src.paths import BASE_DATA_PATH, OPTIONS_DATA_PATH 

# dataframe to store streamed data.
df_mp, df_idx, df_mkt = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def parse_opt_mp_stream(msg):
    df = pd.DataFrame(msg['data'])
    df.columns = ['eventName', 'time', 'contract', 'markPrice']
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    return df.pivot(index='time', columns='contract', values='markPrice')

def parse_opt_index_stream(msg):
    df = pd.DataFrame([msg['data']])
    df.columns = ['eventName', 'time', 'underlying', 'price']
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    return df.pivot(index='time', columns='underlying', values='price')

def parse_opt_mkt_stream(msg):
    df = pd.DataFrame([msg['data']])
    df = df[['E', 's', 'o', 'h', 'l', 'c', 'V', 'd', 'g', 'v', 'vo', 'bo', 'ao']]
    df.columns = ['time', 'contract', 'o', 'h', 'l', 'c', 'v', 'delta', 'gamma', 'vega', 'IV', 'IV_buy', 'IV_ask']
    return df

def on_message(ws, message):
    global df_mp, df_idx, df_mkt
    msg = json.loads(message)
    
    if msg['stream'].split('@')[-1] == 'markPrice': # opt contract mark price
        df = parse_opt_mp_stream(msg)
        df_mp = pd.concat([df_mp, df], axis=0)
        
    elif msg['stream'].split('@')[-1] == 'index': # opt contract index price
        df = parse_opt_index_stream(msg)
        df_idx = pd.concat([df_idx, df], axis=0)
        
    elif msg['stream'].split('@')[-1] == 'ticker': # indiv. opt contract
        df = parse_opt_mkt_stream(msg)
        df_mkt = pd.concat([df_mkt, df], axis=0)
        
    
        
def on_open(ws):
    print('Opened Connection')

def on_error(ws, error):
    print(error)
    
def on_close(ws, close_status_code, msg):
    global df_mp, df_idx, df_mkt
    print('websocket closed. Preparing data...')
    
    # define file paths
    mp_path  = os.path.join(OPTIONS_DATA_PATH, f'{underlying}_markprice.csv')
    idx_path = os.path.join(OPTIONS_DATA_PATH, f'{underlying}_indexprice.csv')
    mkt_path = os.path.join(OPTIONS_DATA_PATH, f'{underlying}_market.csv')
    
    # market data parsing
    df_mkt['time'] = pd.to_datetime(df_mkt['time'])
    
    # store data
    if os.path.exists(mp_path):
        df_mp_legacy = pd.read_csv(mp_path, index_col=0)
        df_mp_legacy = pd.concat([df_mp_legacy, df_mp], axis=0)
        df_mp_legacy.to_csv(mp_path)
    else:
        df_mp.to_csv(mp_path)
    
    if os.path.exists(idx_path):
        df_idx_legacy = pd.read_csv(idx_path, index_col=0)
        df_idx_legacy = pd.concat([df_idx_legacy, df_idx], axis=0)
        df_idx_legacy.to_csv(idx_path)
    else:
        df_idx.to_csv(idx_path)
    
    if os.path.exists(mkt_path):
        df_mkt_legacy = pd.read_csv(mkt_path, index_col=0)
        df_mkt_legacy = pd.concat([df_mkt_legacy, df_mkt], axis=0)
        df_mkt_legacy.to_csv(mkt_path)
    else:
        df_mkt.to_csv(mkt_path)
    print('market data store complete.')
    
    
if __name__ == '__main__':
    # websocket.enableTrace(True)
    
    # List of Binance API endpoint for options WebSocket streams
    underlying = 'xrp'
    symbol = underlying + 'usdt'
    stream_url = f'wss://nbstream.binance.com/eoptions/stream?streams={symbol.upper()}@index/{underlying.upper()}@markPrice'

    # get contract info given underlying
    contract_info = pd.read_csv(os.path.join(BASE_DATA_PATH, 'contracts.csv'))
    contract_info = contract_info[contract_info['underlying'] == symbol.upper()] 
    
    # construct a path for all available option contract
    stream_url += '/' + (contract_info['symbol'] + '@ticker/').sum()
    
    # initialize WebsocketApp and run forever.
    wsapp = WebSocketApp(
                stream_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
    
    wsapp.run_forever()
