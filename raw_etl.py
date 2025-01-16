from typing import Literal
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert, Insert
from sqlalchemy.dialects._typing import _OnConflictWhereT
import datetime
import calendar
import pandas as pd
import numpy as np
from pandas.io.sql import SQLTable



class RawETLoader:
    db_engine: sa.Engine
    db_schema: str = 'raw'

    def __init__(self) -> None:
        self.db_engine = sa.create_engine(
            "postgresql+psycopg2://postgres:postgres@localhost:5432/bhft",  # не выношу креды отдельно, но по-хорошему надо
            connect_args={'options': '-csearch_path={}'.format(self.db_schema)}
        )
        self.metadata: sa.MetaData = sa.MetaData(schema=self.db_schema)
        self.metadata.reflect(bind=self.db_engine)
        print('RawETLoader initialized!')
        return None


    def info_insert(self, exchange_type: Literal['BYBIT', 'BINANCE', 'GATEIO', 'KRAKEN', 'OKX'], data: dict, insert_ts: int = 0):
        info_raw_tbl = sa.Table('exchange_api_instrument_info', self.metadata)
        
        with self.db_engine.connect() as conn:
            conn.execute(
                    info_raw_tbl.insert(), {'exchange': exchange_type, 'insert_ts': insert_ts if insert_ts else calendar.timegm(datetime.datetime.now(tz=datetime.timezone.utc).timetuple()) * 1000, 'data': data}
                )
            conn.commit()
        return None
    

    def kline_insert(self, exchange_type: Literal['BYBIT', 'BINANCE', 'GATEIO', 'KRAKEN', 'OKX'], data: list, insert_ts: int = 0):
        kline_raw_tbl = sa.Table('exchange_api_kline', self.metadata)

        with self.db_engine.connect() as conn:
            conn.execute(
                kline_raw_tbl.insert(), [{'exchange': exchange_type, 'symbol': row[0], 'time_frame': 'D', 'insert_ts': insert_ts if insert_ts else calendar.timegm(datetime.datetime.now(tz=datetime.timezone.utc).timetuple()) * 1000, 'data': row[-1]} for row in data if row]
            )
            conn.commit()
        return None
    

    def info_read(self, exchange_type: Literal['BYBIT', 'BINANCE', 'GATEIO', 'KRAKEN', 'OKX'], mode: Literal['incremental', 'initial'] = 'incremental', start_dt: datetime.datetime | None = None) -> pd.DataFrame:
        def extract_keys(row) -> pd.DataFrame | None:
            if row['data']:
                if row['exchange'] == 'BYBIT':
                    json_items = row['data'].get('result', {}).get('list', {})
                elif row['exchange'] == 'BINANCE':
                    json_items = row['data'].get('symbols', {})
                elif row['exchange'] == 'GATEIO':
                    json_items = row['data']
                elif row['exchange'] == 'KRAKEN':
                    json_result = row['data'].get('result', {})
                    json_items = [{'symbol': symbol, 'base': val['base'], 'quote': val['quote'], 'status': val['status']} for symbol,val in json_result.items()]
                elif row['exchange'] == 'OKX':
                    json_items = row['data'].get('data', {})
                df_items = pd.json_normalize(json_items)
                df_items['exchange'], df_items['insert_ts'] = row['exchange'], row['insert_ts']
                if row['exchange'] == 'BYBIT':
                    return df_items[['exchange', 'insert_ts', 'symbol', 'baseCoin', 'quoteCoin', 'status']]
                elif row['exchange'] == 'BINANCE':
                    return df_items[['exchange', 'insert_ts', 'symbol', 'baseAsset', 'quoteAsset', 'status']]
                elif row['exchange'] == 'GATEIO':
                    df_items['id'] = df_items['id'].str.replace('_', '')
                    return df_items[['exchange', 'insert_ts', 'id', 'base', 'quote', 'trade_status']]
                elif row['exchange'] == 'KRAKEN':
                    return df_items[['exchange', 'insert_ts', 'symbol', 'base', 'quote', 'status']] 
                elif row['exchange'] == 'OKX':
                    df_items['instId'] = df_items['instId'].str.replace('-', '')
                    return df_items[['exchange', 'insert_ts', 'instId', 'baseCcy', 'quoteCcy', 'state']]
            
        if mode == 'initial':
            with self.db_engine.connect() as conn:
                df_info = pd.read_sql_query(f"select * from raw.exchange_api_instrument_info where exchange = '{exchange_type}'", conn)
        elif mode == 'incremental':
            with self.db_engine.connect() as conn: 
                dt_condition = calendar.timegm(start_dt.date().timetuple()) * 1000 if start_dt else calendar.timegm((datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)).date().timetuple()) * 1000
                df_info = pd.read_sql_query(f"select * from raw.exchange_api_instrument_info where exchange = '{exchange_type}' and insert_ts >= {dt_condition}", conn)

        if not df_info.empty:
            df_info_flat: pd.DataFrame = pd.concat(df_info.apply(extract_keys, axis=1).to_list(), ignore_index=True) # type: ignore
            df_info_flat.columns = ['exchange', 'insert_ts', 'symbol', 'base_coin', 'quote_coin', 'trading_status']
            df_info_flat['rn'] = df_info_flat.groupby(['exchange', 'symbol', 'base_coin', 'quote_coin', 'trading_status'])['insert_ts'].rank(method='first', ascending=False)
            df_info_flat = df_info_flat[['exchange', 'symbol', 'base_coin', 'quote_coin', 'trading_status', 'insert_ts']][df_info_flat['rn'] == 1].reset_index(drop=True)
            return df_info_flat
        else:
            print('Warning: no data found in db table!')
            return pd.DataFrame()
    

    def kline_read(self, exchange_type: Literal['BYBIT', 'BINANCE', 'GATEIO', 'KRAKEN', 'OKX'], mode: Literal['incremental', 'initial'] = 'incremental', start_dt: datetime.datetime | None = None) -> pd.DataFrame:
        def extract_keys(row) -> pd.DataFrame | None:
            if row['data']:
                if row['exchange'] == 'BYBIT':
                    json_items = row['data'].get('result', {})
                    if json_items:
                        df_items = pd.json_normalize(json_items)
                        df_items['exchange'], df_items['symbol'], df_items['insert_ts'] = row['exchange'], row['symbol'], row['insert_ts']
                        if not df_items[df_items['list'].astype(bool)].empty:
                            return df_items.explode('list').reset_index(drop=True)
                elif row['exchange'] == 'KRAKEN':
                    json_result = row['data'].get('result', {})
                    if json_result:
                        json_items = [{'symbol': symbol, 'list': val} for symbol,val in json_result.items() if symbol != 'last']
                        df_items = pd.json_normalize(json_items)
                        df_items['exchange'], df_items['symbol'], df_items['insert_ts'] = row['exchange'], row['symbol'], row['insert_ts']
                        if not df_items[df_items['list'].astype(bool)].empty:
                            return df_items.explode('list').reset_index(drop=True)
                elif row['exchange'] == 'OKX':
                    json_items = row['data']
                    df_items = pd.json_normalize(json_items)
                    df_items['exchange'], df_items['symbol'], df_items['insert_ts'] = row['exchange'], row['symbol'], row['insert_ts']
                    if not df_items[df_items['data'].astype(bool)].empty:
                        return df_items.explode('data').reset_index(drop=True)

        if mode == 'initial':
            with self.db_engine.connect() as conn:
                df_kline = pd.read_sql_query(f"select * from raw.exchange_api_kline where exchange = '{exchange_type}'", conn)
        elif mode == 'incremental':
            with self.db_engine.connect() as conn: 
                dt_condition = calendar.timegm(start_dt.date().timetuple()) * 1000 if start_dt else calendar.timegm((datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)).date().timetuple()) * 1000
                df_kline = pd.read_sql_query(f"select * from raw.exchange_api_kline where exchange = '{exchange_type}' and insert_ts >= {dt_condition}", conn)

        if not df_kline.empty:
            df_kline.columns = ['exchange', 'symbol', 'time_frame', 'insert_ts', 'data']
            if exchange_type == 'BYBIT':
                df_kline_flat: pd.DataFrame = pd.concat(df_kline.apply(extract_keys, axis=1).to_list(), ignore_index=True) # type: ignore
                df_kline_flat[['oper_dt', 'open_price', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'turnover']] = pd.DataFrame(df_kline_flat.list.values.tolist(), index=df_kline_flat.index)
                df_kline_flat['oper_dt'] = pd.to_datetime(df_kline_flat['oper_dt'].astype('int64'), unit='ms')
            elif exchange_type == 'BINANCE':
                df_kline_flat: pd.DataFrame = df_kline.explode('data')
                df_kline_flat = df_kline_flat[~df_kline_flat['data'].isnull()]
                df_kline_flat[['oper_dt', 'open_price', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'endTime', 'turnover', 'tradesNum', 'takerBVol', 'takerQVol', 'ignored']] = pd.DataFrame(df_kline_flat.data.values.tolist(), index=df_kline_flat.index)
                df_kline_flat['oper_dt'] = pd.to_datetime(df_kline_flat['oper_dt'].astype('int64'), unit='ms')
            elif exchange_type == 'GATEIO':
                df_kline_flat: pd.DataFrame = df_kline.explode('data')
                df_kline_flat = df_kline_flat[~df_kline_flat['data'].isnull()]
                df_kline_flat[['oper_dt', 'turnover', 'closePrice', 'highPrice', 'lowPrice', 'open_price', 'volume', 'winClosed']] = pd.DataFrame(df_kline_flat.data.values.tolist(), index=df_kline_flat.index)
                df_kline_flat['oper_dt'] = pd.to_datetime(df_kline_flat['oper_dt'].astype('int64'), unit='s')
            elif exchange_type == 'OKX':
                df_kline_flat: pd.DataFrame = pd.concat(df_kline.apply(extract_keys, axis=1).to_list(), ignore_index=True) # type: ignore
                df_kline_flat[['oper_dt', 'open_price', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'turnover', 'volCcyQuote', 'confirm']] = pd.DataFrame(df_kline_flat.data.values.tolist(), index=df_kline_flat.index)
                df_kline_flat['oper_dt'] = pd.to_datetime(df_kline_flat['oper_dt'].astype('int64'), unit='ms')
            
            if exchange_type != 'KRAKEN':
                df_kline_flat = df_kline_flat[['exchange', 'symbol', 'oper_dt', 'volume', 'turnover', 'insert_ts', 'open_price']]\
                    .astype({'volume': 'float64', 'turnover': 'float64'})\
                    .drop_duplicates()\
                    .reset_index(drop=True)
                df_kline_flat['price_avg'] = df_kline_flat['turnover'] / df_kline_flat['volume']
                df_kline_flat.loc[df_kline_flat['price_avg'].isnull(), 'price_avg'] = df_kline_flat['open_price'].astype('float64')
            else:
                df_kline_flat = pd.concat(df_kline.apply(extract_keys, axis=1).to_list(), ignore_index=True) # type: ignore
                df_kline_flat[['oper_dt', 'open_price', 'highPrice', 'lowPrice', 'closePrice', 'vwap', 'volume', 'tradesNum']] = pd.DataFrame(df_kline_flat.list.values.tolist(), index=df_kline_flat.index)
                df_kline_flat = df_kline_flat[['exchange', 'symbol', 'oper_dt', 'volume', 'insert_ts', 'vwap', 'open_price']].rename(columns={'vwap': 'price_avg', 'volume': 'turnover'}).astype({'turnover': 'float64', 'price_avg': 'float64'}).drop_duplicates().reset_index(drop=True)
                df_kline_flat.loc[np.isclose(df_kline_flat['turnover'], 0.0), 'price_avg'] = df_kline_flat['open_price'].astype('float64')
                df_kline_flat.loc[~np.isclose(df_kline_flat['turnover'], 0.0), 'turnover'] = df_kline_flat['turnover'] * df_kline_flat['price_avg']
                df_kline_flat['oper_dt'] = pd.to_datetime(df_kline_flat['oper_dt'].astype('int64'), unit='s')

            df_kline_flat['rn'] = df_kline_flat.groupby(['exchange', 'symbol', 'oper_dt'])['insert_ts'].rank(method='first', ascending=False)
            df_kline_flat = df_kline_flat[['exchange', 'symbol', 'oper_dt', 'price_avg', 'turnover', 'insert_ts']][df_kline_flat['rn'] == 1].reset_index(drop=True)
            return df_kline_flat.rename(columns={'turnover': 'vol_amt'})
        else:
            print('Warning: no data found in db table!')
            return pd.DataFrame()
        

class DmETLoader:
    db_engine: sa.Engine
    db_schema: str = 'spot'
    tbl_abs_values: dict

    def __init__(self) -> None:
        self.db_engine = sa.create_engine(
            "postgresql+psycopg2://postgres:postgres@localhost:5432/bhft",  # не выношу креды отдельно, но по-хорошему надо
            connect_args={'options': '-csearch_path={}'.format(self.db_schema)}
        )
        self.metadata: sa.MetaData = sa.MetaData(schema=self.db_schema)
        self.metadata.reflect(bind=self.db_engine)

        # здесь получим max_value
        stmt = """
        with _tfct_coin as (select exchange, min(oper_dt) as min_dt, max(oper_dt) as max_dt from spot.tfct_coin group by exchange),
        _tfct_rate as (select exchange, min(oper_dt) as min_dt, max(oper_dt) as max_dt from spot.tfct_exchange_rate group by exchange),
        _union as (select * from _tfct_coin union select * from _tfct_coin)
        select exchange, min(min_dt) as min_dt, max(max_dt) as max_dt from _union group by exchange
        """
        with self.db_engine.connect() as conn:
            df_max_value = pd.read_sql_query(stmt, conn)
        self.tbl_abs_values = df_max_value.set_index('exchange').transpose().to_dict()
        print('DmETLoader initialized!')


    def get_tbl_cols(self, tbl_name: str) -> list:
        tbl = sa.Table(tbl_name, self.metadata)
        return [col.name for col in tbl.columns]
    

    def get_abs_values(self, exchange_type: Literal['BYBIT', 'BINANCE', 'GATEIO', 'KRAKEN', 'OKX']) -> dict:
        return self.tbl_abs_values.get(exchange_type, {})
    
    
    def __build_where_clause(self, tbl: SQLTable, insert_stmt: Insert) -> _OnConflictWhereT:
        if tbl.name == 'dim_coin':
            return ((tbl.table.c.insert_ts < insert_stmt.excluded.insert_ts) & \
                    ((tbl.table.c.base_coin != insert_stmt.excluded.base_coin) | \
                    (tbl.table.c.quote_coin != insert_stmt.excluded.quote_coin) | \
                    (tbl.table.c.trading_status != insert_stmt.excluded.trading_status)))
        elif tbl.name == 'tfct_coin':
            return ((tbl.table.c.insert_ts < insert_stmt.excluded.insert_ts) & \
                    (tbl.table.c.vol_amt != insert_stmt.excluded.vol_amt))
        elif tbl.name == 'tfct_exchange_rate':
            return ((tbl.table.c.insert_ts < insert_stmt.excluded.insert_ts) & \
                    (tbl.table.c.usdt_amt != insert_stmt.excluded.usdt_amt))
        

    def tbl_load(self, tbl_name: str, df_tbl: pd.DataFrame) -> None:
        def upsert_on_conflict(table, conn, keys, data_iter):
            insp = sa.inspect(conn)
            #keys_modified = self.__build_key_list(table.name, keys)
            #data = [dict(zip(keys_modified, row)) for row in data_iter]
            data = [dict(zip(keys, row)) for row in data_iter]
            insert_statement = insert(table.table).values(data)
            upsert_statement = insert(table.table).values(data).on_conflict_do_update(
                index_elements=insp.get_pk_constraint(table_name=table.name)["constrained_columns"],
                set_={c.key: c for c in insert_statement.excluded}, 
                #self.__build_col_set(table.name, insert_statement),
                where=self.__build_where_clause(table, insert_statement)
            )
            result = conn.execute(upsert_statement)
            return result.rowcount
        
        with self.db_engine.connect() as conn:
            rows_affected = df_tbl.to_sql(tbl_name, conn, if_exists='append', index=False, method=upsert_on_conflict)
        print(f'Info: upsert {rows_affected} строк(и) в таблицу {self.db_schema}.{tbl_name}')