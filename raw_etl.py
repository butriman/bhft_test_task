from abc import ABC, abstractmethod
from typing import Literal, TYPE_CHECKING
import sqlalchemy as sa
import datetime
import calendar
import pandas as pd

if TYPE_CHECKING:
    from exchange import Bybit


class RawETLoader:
    start_dt: datetime.datetime
    end_dt: datetime.datetime
    #mode: Literal['incremental', 'initial']  #self.mode = mode
    db_engine: sa.Engine
    db_schema: str = 'raw'


    def __init__(self) -> None:
        self.db_engine = sa.create_engine(
            "postgresql+psycopg2://postgres:postgres@localhost:5432/bhft",  # не выношу креды отдельно, но по-хорошему надо
            connect_args={'options': '-csearch_path={}'.format(self.db_schema)}
        )
        self.raw_metadata: sa.MetaData = sa.MetaData(schema='raw')
        self.raw_metadata.reflect(bind=self.db_engine)
        #self.dm_metadata: sa.MetaData = sa.MetaData(schema='dm')
        #self.dm_metadata.reflect(bind=self.db_engine)
        print('RawETLoader initialized!')
        return None


    def info_insert(self, data: dict):
        info_raw_tbl = sa.Table('exchange_api_instrument_info', self.raw_metadata)
        
        with self.db_engine.connect() as conn:
            conn.execute(
                info_raw_tbl.insert(), {'exchange': 'BYBIT', 'insert_ts': data.get('time', 0), 'data': data}
            )
            conn.commit()
        return None
    

    def kline_insert(self, data: list):
        kline_raw_tbl = sa.Table('exchange_api_kline', self.raw_metadata)
        """
        with self.db_engine.connect() as conn:
            for row in data:
                if row:
                    conn.execute(
                        kline_raw_tbl.insert(), {'exchange': 'BYBIT', 'symbol': row.get('result', {}).get('symbol', None), 'time_frame': 'D', 'insert_ts': row.get('time', 0), 'data': row}
                    )
            conn.commit()
        """
        with self.db_engine.connect() as conn:
            conn.execute(
                kline_raw_tbl.insert(), [{'exchange': 'BYBIT', 'symbol': row.get('result', {}).get('symbol', None), 'time_frame': 'D', 'insert_ts': row.get('time', 0), 'data': row} for row in data if row]
            )
            conn.commit()
        return None
    

    def info_read(self, mode: Literal['incremental', 'initial'] = 'incremental', start_dt: datetime.datetime | None = None) -> pd.DataFrame:
        def extract_keys(row) -> pd.DataFrame | None:
            if row['data']:
                json_items = row['data'].get('result', {}).get('list', {})
                df_items = pd.json_normalize(json_items)
                df_items['exchange'], df_items['insert_ts'] = row['exchange'], row['insert_ts']
                return df_items[['exchange', 'insert_ts', 'symbol', 'baseCoin', 'quoteCoin', 'status']]
            
        if mode == 'initial':
            with self.db_engine.connect() as conn:
                df_info = pd.read_sql_table('exchange_api_instrument_info', conn)
        elif mode == 'incremental':
            with self.db_engine.connect() as conn: 
                dt_condition = start_dt if start_dt else calendar.timegm((datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)).date().timetuple()) * 1000
                df_info = pd.read_sql_query(f'select * from raw.exchange_api_instrument_info where insert_ts >= {dt_condition}', conn)

        if not df_info.empty:
            df_info_flat: pd.DataFrame = pd.concat(df_info.apply(extract_keys, axis=1).to_list(), ignore_index=True) # type: ignore
            df_info_flat['rn'] = df_info_flat.groupby(['exchange', 'symbol', 'baseCoin', 'quoteCoin', 'status'])['insert_ts'].rank(method='first', ascending=False)
            df_info_flat = df_info_flat[['exchange', 'symbol', 'baseCoin', 'quoteCoin', 'status', 'insert_ts']][df_info_flat['rn'] == 1].reset_index(drop=True)
            df_info_flat.columns = ['exchange', 'symbol', 'base_coin', 'quote_coin', 'trading_status', 'insert_ts']
            return df_info_flat
        else:
            print('Warning: no data found in db table!')
            return pd.DataFrame()
    

    def kline_read(self, mode: Literal['incremental', 'initial'] = 'incremental', start_dt: datetime.datetime | None = None) -> pd.DataFrame:
        def extract_keys(row) -> pd.DataFrame | None:
            if row['data']:
                json_items = row['data'].get('result', {})
                df_items = pd.json_normalize(json_items)
                df_items['exchange'], df_items['symbol'], df_items['insert_ts'] = row['exchange'], row['symbol'], row['insert_ts']
                if not df_items[df_items['list'].astype(bool)].empty:
                    return df_items.explode('list').reset_index(drop=True)

        if mode == 'initial':
            with self.db_engine.connect() as conn:
                df_kline = pd.read_sql_table('exchange_api_kline', conn)
        elif mode == 'incremental':
            with self.db_engine.connect() as conn: 
                dt_condition = start_dt if start_dt else calendar.timegm((datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)).date().timetuple()) * 1000
                df_kline = pd.read_sql_query(f'select * from raw.exchange_api_kline where insert_ts >= {dt_condition}', conn)

        if not df_kline.empty:
            df_kline_flat: pd.DataFrame = pd.concat(df_kline.apply(extract_keys, axis=1).to_list(), ignore_index=True) # type: ignore
            df_kline_flat[['startTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'turnover']] = pd.DataFrame(df_kline_flat.list.values.tolist(), index=df_kline_flat.index)
            df_kline_flat = df_kline_flat[['exchange', 'symbol', 'startTime', 'openPrice', 'volume', 'insert_ts']]\
                .rename(columns={'startTime': 'report_dt', 'openPrice': 'open_price', 'volume': 'vol_amt'})\
                .astype({'report_dt': 'Int64', 'open_price': 'Float64', 'vol_amt': 'Float64'})\
                .drop_duplicates()
            df_kline_flat['report_dt'] = pd.to_datetime(df_kline_flat['report_dt'], unit='ms')

            df_kline_flat['rn'] = df_kline_flat.groupby(['exchange', 'symbol', 'report_dt', 'open_price', 'vol_amt'])['insert_ts'].rank(method='first', ascending=False)
            df_kline_flat = df_kline_flat[['exchange', 'symbol', 'report_dt', 'open_price', 'vol_amt', 'insert_ts']][df_kline_flat['rn'] == 1].reset_index(drop=True)
            return df_kline_flat
        else:
            print('Warning: no data found in db table!')
            return pd.DataFrame()