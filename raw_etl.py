from abc import ABC, abstractmethod
from typing import Literal, TYPE_CHECKING
import sqlalchemy as sa
import datetime
import pandas as pd

if TYPE_CHECKING:
    from exchange import Bybit


class ETLoader:
    start_dt: datetime.datetime
    end_dt: datetime.datetime
    #mode: Literal['incremental', 'initial']  #self.mode = mode
    db_engine: sa.Engine

    def __init__(self) -> None:
        self.db_engine = sa.create_engine(
            "postgresql+psycopg2://postgres:postgres@localhost:5432/bhft"  # не выношу креды отдельно, но по-хорошему надо
        )
        self.raw_metadata: sa.MetaData = sa.MetaData(schema='raw')
        self.raw_metadata.reflect(bind=self.db_engine)
        #self.dm_metadata: sa.MetaData = sa.MetaData(schema='dm')
        #self.dm_metadata.reflect(bind=self.db_engine)
        print('ETLoader initialized!')
        return None

    def raw_load_info(self, data: list):
        info_raw_tbl = sa.Table('exchange_api_instrument_info', self.raw_metadata)
        
        with self.db_engine.connect() as conn:
            for row in data:
                conn.execute(
                    info_raw_tbl.insert(), {'exchange': 'BYBIT', 'insert_ts': row['time'], 'data': row}
                )
            conn.commit()
        return None
    
    def raw_load_kline(self, data: list):
        kline_raw_tbl = sa.Table('exchange_api_kline', self.raw_metadata)

        with self.db_engine.connect() as conn:
            for row in data:
                conn.execute(
                    kline_raw_tbl.insert(), {'exchange': 'BYBIT', 'symbol': row['result']['symbol'], 'time_frame': 'D', 'insert_ts': row['time'], 'data': row}
                )
            conn.commit()
        return None