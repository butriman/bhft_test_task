from abc import ABC, abstractmethod
from typing import Literal
from json import JSONDecodeError
import requests
import datetime
import calendar


class Exchange(ABC):
    @property
    @abstractmethod
    def url(self) -> str:
        # api link
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def endpoint_dict(self) -> dict:
        # API endpoints
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def spot_coins(self) -> list:
        # list of coins
        raise NotImplementedError()

    @abstractmethod
    def _kline(self) -> dict:
        # get kline of spot pairs
        raise NotImplementedError
    

class Bybit(Exchange):
    url: str = 'https://api.bybit.com'
    endpoint_dict: dict = {
        'info': '/v5/market/instruments-info',  # https://bybit-exchange.github.io/docs/v5/market/instrument
        'kline': '/v5/market/kline'  # https://bybit-exchange.github.io/docs/v5/market/kline
    }
    category: str = 'spot'
    spot_coins: list = []
    info_resp: dict = {}

    
    def __init__(self) -> None:
        """
        Initialization of a class instance by obtaining exchange's list of available spot pairs
        """
        url: str = self.url + self.endpoint_dict['info']
        try:
            resp = requests.get(url=url, params={'category': self.category})
            if resp.ok:
                self.info_resp = resp.json()
                self.spot_coins = [[v for k,v in coin.items() if k in ['symbol', 'baseCoin', 'quoteCoin', 'status']] for coin in resp.json()['result']['list']]
            else:
                print('Bybit coins domain is unreacheable')
            print('Bybit initialized')
        except JSONDecodeError as json_err:
            print(f'Exception: JSONDecodeError {json_err = }')
        except Exception as msg:
            print(f'Exception: init {msg}')


    def _kline(self, 
               symbol: str,
               limit: int | None = None,
               start_dt: datetime.datetime | None = None,
               end_dt: datetime.datetime | None = None,
    ) -> dict:
        """
        Incremental (last day): 
        _kline(
            base_coin_list[0], 
            start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1), 
            limit=1)
        """
        url: str = self.url + self.endpoint_dict['kline']
        params: dict = {
            'category': self.category,
            'interval': 'D',
            'symbol': symbol
        }
        if not any([limit, start_dt, end_dt]):
            tmp_start_dt = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
            tmp_end_dt = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=1)
            params['start'] = calendar.timegm(tmp_start_dt.date().timetuple()) * 1000
            params['end'] = calendar.timegm(tmp_end_dt.date().timetuple()) * 1000
        elif limit and not any([start_dt, end_dt]):
            params['limit'] = limit
        elif limit and start_dt and not end_dt:
            params['start'] = calendar.timegm(start_dt.date().timetuple()) * 1000
            params['limit'] = limit
        elif limit and not start_dt and end_dt:
            params['end'] = calendar.timegm(end_dt.date().timetuple()) * 1000
            params['limit'] = limit

        if start_dt:
            params['start'] = calendar.timegm(start_dt.date().timetuple()) * 1000
        if end_dt:
            params['end'] = calendar.timegm(end_dt.date().timetuple()) * 1000
        try:
            resp = requests.get(url=url, params=params) 
            if resp.ok:
                if any([resp.json()['retCode'] != 0, 
                        resp.json()['retMsg'] not in ['OK', 'success', 'SUCCESS', ''], 
                        resp.json()['retExtInfo']]):
                    print(f'Warning: sus response parameters ({resp.json()["retCode"] = }, {resp.json()["retMsg"] = }, {resp.json()["retExtInfo"] = }) {params = }')
                return resp.json()
            else:
                print(f'Error: Bybit kline endpoint {resp.status_code = }')
                return {}
        except JSONDecodeError as json_err:
            print(f'Exception: JSONDecodeError {json_err = }, {params = }')
            return {}
        except Exception as msg:
            print(f'Exception: get_kline {msg}, {params = }')
            return {}


    def load_kline(self, 
                   mode: Literal['inc', 'init', 'custom'] = 'inc',
                   limit: int | None = None,
                   start_dt: datetime.datetime | None = None,
                   end_dt: datetime.datetime | None = None,
        ) -> list:
        """
        Function manages the process of kline data collection:
            1. inc - incremental (last day)
            2. init - initial (last 1000 days)
            3. custom - requires start_dt || end_dt || limit to be provided
        """
        if mode == 'inc':
            return [
                self._kline(symbol=coin[0],
                            start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1), 
                            limit=1)
                for coin in self.spot_coins
            ]
        elif mode == 'init':
            return [
                self._kline(symbol=coin[0],
                            limit=1000)
                for coin in self.spot_coins
            ]
        elif mode == 'custom':
            return [
                self._kline(symbol=coin[0],
                            limit=limit,
                            start_dt=start_dt,
                            end_dt=end_dt)
                for coin in self.spot_coins
            ]


    def get_tickers(self, category: str = 'spot'):
        print('Empty for now')


class Binance(Exchange):
    url: str = 'api.binance.com'
    method: str = '/info'

    def __init__(self):
        print('Binance initialized')

    def _auth(self):
        print('Binance auth passed')

    def _kline(self):
        print('Binance kline used')


class Gateio(Exchange):
    url: str = 'api.gateio.com'
    method: str = '/info'

    def __init__(self):
        print('Gateio initialized')

    def _auth(self):
        print('Gateio auth passed')

    def _kline(self):
        print('Gateio kline used')


class Kraken(Exchange):
    url: str = 'api.kraken.com'
    method: str = '/info'

    def __init__(self):
        print('Kraken initialized')

    def _auth(self):
        print('Kraken auth passed')

    def _kline(self):
        print('Kraken kline used')


class Okex(Exchange):
    url: str = 'api.okex.com'
    method: str = '/info'

    def __init__(self):
        print('Okex initialized')

    def _auth(self):
        print('Okex auth passed')

    def _kline(self):
        print('Okex kline used')