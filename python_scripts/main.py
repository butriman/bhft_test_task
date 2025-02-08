from typing import Literal
from exchange import Exchange, Bybit, Binance, Gateio, Kraken, Okx
import pandas as pd
import datetime
from raw_etl import RawETLoader, DmETLoader
 
import re
import argparse


exchange_dict = {
    'Bybit': Bybit,
    'Binance': Binance,
    'Gateio': Gateio,
    'Kraken': Kraken,
    'Okx': Okx
}
 
 
def dt_regex_type(arg_value, pat=re.compile(r"\b\d{4}-\d{2}-\d{2}\b")):
    if not pat.match(arg_value):
        raise argparse.ArgumentTypeError("Invalid value. Try mask YYYY-MM-DD")
    return arg_value


def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', nargs='?', default='incremental', choices=['initial', 'incremental', 'custom'])
    parser.add_argument('-d', '--start_dt', nargs='?', default='2025-01-01', type=dt_regex_type)
    parser.add_argument('-e', '--exchange', nargs='*', default=None, choices=['Bybit', 'Binance', 'Gateio', 'Kraken', 'Okx'], type=str)
    return parser


def load(exchange: Exchange, start_dt: datetime.datetime, raw_etl: RawETLoader, dm_etl: DmETLoader):
    # raw
    kline_list = exchange.load_kline(mode='custom', start_dt=start_dt)  
    raw_etl.info_insert(exchange.name, exchange.info_resp, exchange.info_ts)
    raw_etl.kline_insert(exchange.name, kline_list, exchange.kline_ts)
    # load from raw
    pd_info = raw_etl.info_read(exchange.name, 'incremental', start_dt=start_dt)
    pd_kline = raw_etl.kline_read(exchange.name, 'incremental', start_dt=start_dt)
    # load to dm
    tbl_name = 'dim_coin'
    tbl_cols = dm_etl.get_tbl_cols(tbl_name)
    dm_etl.tbl_load(tbl_name=tbl_name, df_tbl=pd_info[tbl_cols])

    tbl_name = 'tfct_coin'
    tbl_cols = dm_etl.get_tbl_cols(tbl_name)
    dm_etl.tbl_load(tbl_name=tbl_name, df_tbl=pd_kline[tbl_cols])

    tbl_name = 'tfct_exchange_rate'
    tbl_cols = dm_etl.get_tbl_cols(tbl_name)
    pd_rate: pd.DataFrame = pd_kline.merge(pd_info[['exchange', 'symbol', 'base_coin', 'quote_coin', 'insert_ts']], 'left', on=['exchange', 'symbol'])
    non_usdt_coin_list = pd_rate[pd_rate['quote_coin'] != 'USDT']['quote_coin'].drop_duplicates(ignore_index=True).to_list()
    pd_rate = pd_rate[(pd_rate['base_coin'].isin(non_usdt_coin_list) & (pd_rate['quote_coin'] == 'USDT')) | (pd_rate['quote_coin'].isin(non_usdt_coin_list) & (pd_rate['base_coin'] == 'USDT'))]
    pd_rate['insert_ts'] = pd_rate[['insert_ts_x', 'insert_ts_y']].max(axis=1)
    pd_rate = pd_rate[['exchange', 'symbol', 'oper_dt', 'base_coin', 'quote_coin', 'price_avg', 'insert_ts']]
    pd_rate.loc[pd_rate['base_coin'] != 'USDT', 'coin'] = pd_rate['base_coin']
    pd_rate.loc[pd_rate['quote_coin'] != 'USDT', 'coin'] = pd_rate['quote_coin']
    pd_rate.loc[pd_rate['quote_coin'] == 'USDT', 'usdt_amt'] = pd_rate['price_avg']
    pd_rate.loc[pd_rate['quote_coin'] != 'USDT', 'usdt_amt'] = 1.0 / pd_rate['price_avg']
    pd_rate['rn'] = pd_rate.groupby([col for col in tbl_cols if col not in ['insert_ts', 'usdt_amt']])['insert_ts'].rank(method='first', ascending=False)
    pd_rate = pd_rate[tbl_cols][pd_rate['rn'] == 1].reset_index(drop=True)
    dm_etl.tbl_load(tbl_name=tbl_name, df_tbl=pd_rate[tbl_cols])
 
    
def pipeline_launch(
        mode: Literal['initial', 'incremental', 'custom'] = 'incremental', 
        start_dt: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1),
        exchange_input_list: list | None = None
    ):
    raw_etl, dm_etl = RawETLoader(), DmETLoader()
    exchange_list: list[Exchange] = [exchange() for key,exchange in exchange_dict.items() if key in exchange_input_list] if exchange_input_list else [exchange() for key,exchange in exchange_dict.items()]
    
    for exchange in exchange_list:
        if mode == 'incremental':
            start_dt = datetime.datetime.combine(dm_etl.get_abs_values(exchange.name)['max_dt'], datetime.datetime.min.time()) - datetime.timedelta(days=2)
        elif mode == 'initial':
            start_dt = datetime.datetime(2025, 1, 1)
        elif mode == 'custom':
            start_dt = datetime.datetime(2025, 1, 1) if start_dt <= datetime.datetime(2025, 1, 1) else start_dt
        try:
            load(exchange, start_dt, raw_etl, dm_etl)
        except Exception as msg:
            print(f'Exception: {msg} occured while loading {exchange.name} data...')


if __name__ == "__main__":
    parser = createParser()
    namespace = parser.parse_args()
 
    print(namespace, namespace.mode, namespace.start_dt, namespace.exchange, sep='\n')


    pipeline_launch(mode=namespace.mode, start_dt=datetime.datetime.strptime(namespace.start_dt, '%Y-%m-%d'), exchange_input_list=namespace.exchange)
