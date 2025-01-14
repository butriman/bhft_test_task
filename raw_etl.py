from abc import ABC, abstractmethod
from typing import Literal, TYPE_CHECKING
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert, Insert
from sqlalchemy.dialects._typing import _OnConflictWhereT, _OnConflictSetT
import datetime
import calendar
import pandas as pd
from pandas.io.sql import SQLTable

if TYPE_CHECKING:
    from exchange import Bybit


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


    def info_insert(self, data: dict):
        info_raw_tbl = sa.Table('exchange_api_instrument_info', self.metadata)
        
        with self.db_engine.connect() as conn:
            conn.execute(
                info_raw_tbl.insert(), {'exchange': 'BYBIT', 'insert_ts': data.get('time', 0), 'data': data}
            )
            conn.commit()
        return None
    

    def kline_insert(self, data: list):
        kline_raw_tbl = sa.Table('exchange_api_kline', self.metadata)
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
                .rename(columns={'startTime': 'oper_dt', 'openPrice': 'open_price', 'volume': 'vol_amt'})\
                .astype({'oper_dt': 'Int64', 'open_price': 'Float64', 'vol_amt': 'Float64'})\
                .drop_duplicates()
            df_kline_flat['oper_dt'] = pd.to_datetime(df_kline_flat['oper_dt'], unit='ms')

            df_kline_flat['rn'] = df_kline_flat.groupby(['exchange', 'symbol', 'oper_dt', 'open_price', 'vol_amt'])['insert_ts'].rank(method='first', ascending=False)
            df_kline_flat = df_kline_flat[['exchange', 'symbol', 'oper_dt', 'open_price', 'vol_amt', 'insert_ts']][df_kline_flat['rn'] == 1].reset_index(drop=True)
            return df_kline_flat
        else:
            print('Warning: no data found in db table!')
            return pd.DataFrame()
        

class DmETLoader:
    db_engine: sa.Engine
    db_schema: str = 'spot'


    def __init__(self) -> None:
        self.db_engine = sa.create_engine(
            "postgresql+psycopg2://postgres:postgres@localhost:5432/bhft",  # не выношу креды отдельно, но по-хорошему надо
            connect_args={'options': '-csearch_path={}'.format(self.db_schema)}
        )
        self.metadata: sa.MetaData = sa.MetaData(schema=self.db_schema)
        self.metadata.reflect(bind=self.db_engine)
        print('DmETLoader initialized!')


    def get_tbl_cols(self, tbl_name: str) -> list:
        tbl = sa.Table(tbl_name, self.metadata)
        return [col.name for col in tbl.columns]

    """
    def __build_col_set(self, tbl_name: Literal['dim_coin', 'tfct_coin', 'tfct_exchange_rate'], insert_stmt: Insert) -> _OnConflictSetT:
        if tbl_name == 'dim_coin':
            return {c.key: c for c in insert_stmt.excluded}
        elif tbl_name == 'tfct_coin':
            return {c.key: c for c in insert_stmt.excluded if c.key not in ['open_price']}
        elif tbl_name == 'tfct_exchange_rate':
            return {c.key: c for c in insert_stmt.excluded if c.key not in ['vol_amt']}

    
    def __build_key_list(self, tbl_name: Literal['dim_coin', 'tfct_coin'], keys: list) -> list:
        if tbl_name == 'dim_coin':
            return keys
        elif tbl_name == 'tfct_coin':
            return [key for key in keys if key not in ['open_price']]
    """
    
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


    """
    def dim_coin_load(self, df_tbl: pd.DataFrame) -> None:
        def upsert_on_conflict(table, conn, keys, data_iter):
            insp = sa.inspect(conn)
            data = [dict(zip(keys, row)) for row in data_iter]
            insert_statement = insert(table.table).values(data)
            upsert_statement = insert(table.table).values(data).on_conflict_do_update(
                index_elements=insp.get_pk_constraint(table_name=table.name)["constrained_columns"],
                set_={c.key: c for c in insert_statement.excluded},
                where=((table.table.c.insert_ts < insert_statement.excluded.insert_ts) & \
                    ((table.table.c.base_coin != insert_statement.excluded.base_coin) | \
                        (table.table.c.quote_coin != insert_statement.excluded.quote_coin) | \
                        (table.table.c.trading_status != insert_statement.excluded.trading_status)))
            )
            result = conn.execute(upsert_statement)
            return result.rowcount

        with self.db_engine.connect() as conn:
            rows_affected = df_tbl.to_sql('dim_coin', conn, if_exists='append', index=False, method=upsert_on_conflict)
        print(f'Info: upsert {rows_affected} строк(и) в таблицу {self.db_schema}.dim_coin')

    
    def tfct_coin_load(self, df_tbl: pd.DataFrame) -> None:
        def upsert_on_conflict(table, conn, keys, data_iter):
            insp = sa.inspect(conn)
            data = [dict(zip(keys, row)) for row in data_iter]
            insert_statement = insert(table.table).values(data)
            upsert_statement = insert(table.table).values(data).on_conflict_do_update(
                index_elements=insp.get_pk_constraint(table_name=table.name)["constrained_columns"],
                set_={c.key: c for c in insert_statement.excluded},
                where=((table.table.c.insert_ts < insert_statement.excluded.insert_ts) & \
                    (table.table.c.vol_amt != insert_statement.excluded.vol_amt))
            )
            result = conn.execute(upsert_statement)
            return result.rowcount

        with self.db_engine.connect() as conn:
            rows_affected = df_tbl.to_sql('tfct_coin', conn, if_exists='append', index=False, method=upsert_on_conflict)
        
        print(f'Info: upsert {rows_affected} строк(и) в таблицу {self.db_schema}.dim_coin')
    """