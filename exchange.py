from abc import ABC, abstractmethod
from typing import Literal
from json import JSONDecodeError
import requests
import datetime
import calendar


class Exchange(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        # name of exchange
        raise NotImplementedError()

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
    
    @property
    @abstractmethod
    def info_ts(self) -> dict:
        # info resp's timestamp
        raise NotImplementedError
    
    @property
    @abstractmethod
    def kline_ts(self) -> dict:
        # kline resp's timestamp
        raise NotImplementedError
    
    @property
    @abstractmethod
    def info_resp(self) -> dict:
        # get kline of spot pairs
        raise NotImplementedError

    @abstractmethod
    def _kline(self) -> dict | list:
        # get kline of spot pairs
        raise NotImplementedError
    

class Bybit(Exchange):
    name: str = 'BYBIT'
    url: str = 'https://api.bybit.com'
    endpoint_dict: dict = {
        'info': '/v5/market/instruments-info',  # https://bybit-exchange.github.io/docs/v5/market/instrument
        'kline': '/v5/market/kline'  # https://bybit-exchange.github.io/docs/v5/market/kline
    }
    category: str = 'spot'
    spot_coins: list = []
    info_resp: dict = {}
    info_ts: int = 0
    kline_ts: int = 0

    
    def __init__(self) -> None:
        """
        Initialization of a class instance by obtaining exchange's list of available spot pairs
        """
        url: str = self.url + self.endpoint_dict['info']
        try:
            resp = requests.get(url=url, params={'category': self.category})
            if resp.ok:
                self.info_resp = resp.json()
                self.spot_coins = [[coin['symbol'], coin['baseCoin'], coin['quoteCoin'], coin['status']] for coin in resp.json()['result']['list']]
                self.info_ts = self.info_resp.get('time', 0)
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
                self.kline_ts = calendar.timegm(datetime.datetime.strptime(resp.headers.get('Date', 'Thu, 01 Jan 1970 00:00:00 GMT'), '%a, %d %b %Y %H:%M:%S %Z').timetuple()) * 1000
                return resp.json()
            else:
                print(f'Error: Bybit kline endpoint {resp.status_code = }')
                return {}
        except JSONDecodeError as json_err:
            print(f'Exception: JSONDecodeError {json_err = }, {params = }')
            return {}
        except Exception as msg:
            print(f'Exception: Bybit get_kline {msg}, {params = }')
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
                (
                    coin[0],
                    self._kline(symbol=coin[0],
                                start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1), 
                                limit=1)
                )
                for coin in self.spot_coins
            ]
        elif mode == 'init':
            return [
                (
                    coin[0],
                    self._kline(symbol=coin[0],
                                limit=1000)
                )
                for coin in self.spot_coins
            ]
        elif mode == 'custom':
            return [
                (
                    coin[0],
                    self._kline(symbol=coin[0],
                                limit=limit,
                                start_dt=start_dt,
                                end_dt=end_dt)
                )
                for coin in self.spot_coins
            ]


class Binance(Exchange):
    name: str = 'BINANCE'
    url: str = 'https://data-api.binance.vision'
    endpoint_dict: dict = {
        'kline': '/api/v3/klines',  # https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data
        'info': '/api/v3/exchangeInfo'  # https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints
    }
    category: str = 'spot'
    spot_coins: list = []
    info_resp: dict = {}
    info_ts: int = 0
    kline_ts: int = 0

    def __init__(self):
        """
        Initialization of a class instance by obtaining exchange's list of available spot pairs
        """
        url: str = self.url + self.endpoint_dict['info']
        try:
            params = {
                'permissions': self.category.upper(), 
                'showPermissionSets': 'false',
                'symbolStatus': 'TRADING'
            }
            resp = requests.get(url=url, params=params)
            if resp.ok:
                self.info_resp = resp.json()
                self.spot_coins = [[coin['symbol'], coin['baseAsset'], coin['quoteAsset'], coin['status']] for coin in resp.json()['symbols']]
                self.info_ts = self.info_resp.get('serverTime', 0)
            else:
                print('Binance coins domain is unreacheable')
            print('Binance initialized')
        except JSONDecodeError as json_err:
            print(f'Exception: JSONDecodeError {json_err = }')
        except Exception as msg:
            print(f'Exception: init {msg}')


    def _kline(self, 
               symbol: str,
               limit: int | None = None,
               start_dt: datetime.datetime | None = None,
               end_dt: datetime.datetime | None = None,
    ) -> list:
        """
        Incremental (last day): 
        _kline(
            base_coin_list[0], 
            start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1), 
            limit=1)
        """
        url: str = self.url + self.endpoint_dict['kline']
        params: dict = {
            'interval': '1d',
            'symbol': symbol
        }
        if not any([limit, start_dt, end_dt]):
            tmp_start_dt = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
            tmp_end_dt = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=1)
            params['startTime'] = calendar.timegm(tmp_start_dt.date().timetuple()) * 1000
            params['endTime'] = calendar.timegm(tmp_end_dt.date().timetuple()) * 1000
        elif limit and not any([start_dt, end_dt]):
            params['limit'] = limit
        elif limit and start_dt and not end_dt:
            params['startTime'] = calendar.timegm(start_dt.date().timetuple()) * 1000
            params['limit'] = limit
        elif limit and not start_dt and end_dt:
            params['endTime'] = calendar.timegm(end_dt.date().timetuple()) * 1000
            params['limit'] = limit

        if start_dt:
            params['startTime'] = calendar.timegm(start_dt.date().timetuple()) * 1000
        if end_dt:
            params['endTime'] = calendar.timegm(end_dt.date().timetuple()) * 1000
        try:
            resp = requests.get(url=url, params=params) 
            if resp.ok:
                self.kline_ts = calendar.timegm(datetime.datetime.strptime(resp.headers.get('Date', 'Thu, 01 Jan 1970 00:00:00 GMT'), '%a, %d %b %Y %H:%M:%S %Z').timetuple()) * 1000
                return resp.json()
            else:
                print(f'Error: Binance kline endpoint {resp.status_code = }')
                return []
        except JSONDecodeError as json_err:
            print(f'Exception: JSONDecodeError {json_err = }, {params = }')
            return []
        except Exception as msg:
            print(f'Exception: Binance get_kline {msg}, {params = }')
            return []
        

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
                (
                    coin[0],
                    self._kline(symbol=coin[0],
                                start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1), 
                                limit=1)
                )
                for coin in self.spot_coins
            ]
        elif mode == 'init':
            return [
                (
                    coin[0],
                    self._kline(symbol=coin[0],
                                limit=1000)
                )
                for coin in self.spot_coins
            ]
        elif mode == 'custom':
            return [
                (
                    coin[0],
                    self._kline(symbol=coin[0],
                            limit=limit,
                            start_dt=start_dt,
                            end_dt=end_dt)
                )
                for coin in self.spot_coins
            ]


class Gateio(Exchange):
    name: str = 'GATEIO'
    url: str = 'https://api.gateio.ws/api/v4'
    endpoint_dict: dict = {
        'kline': '/spot/candlesticks',  # https://www.gate.io/docs/developers/apiv4/#market-candlesticks
        'info': '/spot/currency_pairs'
    }
    category: str = 'spot'
    spot_coins: list = []
    info_resp: dict = {}
    info_ts: int = 0
    kline_ts: int = 0

    def __init__(self):
        """
        Initialization of a class instance by obtaining exchange's list of available spot pairs
        """
        url: str = self.url + self.endpoint_dict['info']
        try:
            resp = requests.get(url=url)
            if resp.ok:
                self.info_resp = resp.json()
                self.spot_coins = [[coin['id'], coin['base'], coin['quote'], coin['trade_status']] for coin in resp.json()]
                self.info_ts = int(int(resp.headers.get('X-Out-Time', 0)) / 1000)
            else:
                print('Gateio coins domain is unreacheable')
            print('Gateio initialized')
        except JSONDecodeError as json_err:
            print(f'Exception: Gateio JSONDecodeError {json_err = }')
        except Exception as msg:
            print(f'Exception: Gateio init {msg}')


    def _kline(self, 
               symbol: str,
               limit: int | None = None,
               start_dt: datetime.datetime | None = None,
               end_dt: datetime.datetime | None = None,
    ) -> list:
        """
        Incremental (last day): 
        _kline(
            base_coin_list[0], 
            start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1), 
            limit=1)
        """
        url: str = self.url + self.endpoint_dict['kline']
        params: dict = {
            'interval': '1d',
            'currency_pair': symbol
        }
        if not any([limit, start_dt, end_dt]):
            tmp_start_dt = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
            tmp_end_dt = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=1)
            params['from'] = calendar.timegm(tmp_start_dt.date().timetuple()) 
            params['to'] = calendar.timegm(tmp_end_dt.date().timetuple()) 
        elif limit and not any([start_dt, end_dt]):
            params['limit'] = limit
        elif limit and start_dt and not end_dt:
            params['from'] = calendar.timegm(start_dt.date().timetuple()) 
            params['limit'] = limit
        elif limit and not start_dt and end_dt:
            params['to'] = calendar.timegm(end_dt.date().timetuple()) 
            params['limit'] = limit

        if start_dt:
            params['from'] = calendar.timegm(start_dt.date().timetuple()) 
        if end_dt:
            params['to'] = calendar.timegm(end_dt.date().timetuple()) 
        try:
            resp = requests.get(url=url, params=params) 
            if resp.ok:
                self.kline_ts = int(int(resp.headers.get('X-Out-Time', 0)) / 1000)
                return resp.json()
            else:
                print(f'Error: Gateio kline endpoint {resp.status_code = }')
                return []
        except JSONDecodeError as json_err:
            print(f'Exception: Gateio JSONDecodeError {json_err = }, {params = }')
            return []
        except Exception as msg:
            print(f'Exception: Gateio get_kline {msg}, {params = }')
            return []
        

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
                (
                    coin[0].replace('_', ''),
                    self._kline(symbol=coin[0],
                                start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1), 
                                limit=1)
                )
                for coin in self.spot_coins
            ]
        elif mode == 'init':
            return [
                (
                    coin[0].replace('_', ''),
                    self._kline(symbol=coin[0],
                                limit=1000)
                )
                for coin in self.spot_coins
            ]
        elif mode == 'custom':
            return [
                (
                    coin[0].replace('_', ''),
                    self._kline(symbol=coin[0],
                            limit=limit,
                            start_dt=start_dt,
                            end_dt=end_dt)
                )
                for coin in self.spot_coins
            ]


class Kraken(Exchange):
    name: str = 'KRAKEN'
    url: str = 'https://api.kraken.com/0/public'
    endpoint_dict: dict = {
        'kline': '/OHLC',  # https://docs.kraken.com/api/docs/rest-api/get-ohlc-data
        'info': '/AssetPairs'
    }
    category: str = 'spot'
    spot_coins: list = []
    info_resp: dict = {}
    info_ts: int = 0
    kline_ts: int = 0

    def __init__(self):
        """
        Initialization of a class instance by obtaining exchange's list of available spot pairs
        """
        url: str = self.url + self.endpoint_dict['info']
        try:
            resp = requests.get(url=url)
            if resp.ok:
                self.info_resp = resp.json()
                self.spot_coins = [[coin_k, coin_val['base'], coin_val['quote'], coin_val['status']] for coin_k, coin_val in resp.json()['result'].items()]
                self.info_ts = calendar.timegm(datetime.datetime.strptime(resp.headers.get('Date', 'Thu, 01 Jan 1970 00:00:00 GMT'), '%a, %d %b %Y %H:%M:%S %Z').timetuple()) * 1000
            else:
                print('Kraken coins domain is unreacheable')
            print('Kraken initialized')
        except JSONDecodeError as json_err:
            print(f'Exception: Kraken JSONDecodeError {json_err = }')
        except Exception as msg:
            print(f'Exception: Kraken init {msg}')


    def _kline(self, 
               symbol: str,
               limit: int | None = None,
               start_dt: datetime.datetime | None = None,
               end_dt: datetime.datetime | None = None,
    ) -> list:
        """
        Incremental (last day): 
        _kline(
            base_coin_list[0], 
            start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
        )
        """
        url: str = self.url + self.endpoint_dict['kline']
        params: dict = {
            'interval': '1440',
            'pair': symbol
        }

        if start_dt:
            params['since'] = calendar.timegm(start_dt.date().timetuple()) 
        try:
            resp = requests.get(url=url, params=params) 
            if resp.ok:
                self.kline_ts = calendar.timegm(datetime.datetime.strptime(resp.headers.get('Date', 'Thu, 01 Jan 1970 00:00:00 GMT'), '%a, %d %b %Y %H:%M:%S %Z').timetuple()) * 1000
                return resp.json()
            else:
                print(f'Error: Kraken kline endpoint {resp.status_code = }')
                return []
        except JSONDecodeError as json_err:
            print(f'Exception: JSONDecodeError {json_err = }, {params = }')
            return []
        except Exception as msg:
            print(f'Exception: Kraken get_kline {msg}, {params = }')
            return []
        

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
                (
                    coin[0],
                    self._kline(symbol=coin[0],
                                start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1))
                )
                for coin in self.spot_coins
            ]
        elif mode == 'init':
            return [
                (
                    coin[0],
                    self._kline(symbol=coin[0])
                )
                for coin in self.spot_coins
            ]
        elif mode == 'custom':
            return [
                (
                    coin[0],
                    self._kline(symbol=coin[0],
                                start_dt=start_dt)
                )
                for coin in self.spot_coins
            ]


class Okx(Exchange):
    name: str = 'OKX'
    url: str = 'https://www.okx.com/api/v5/'
    endpoint_dict: dict = {
        'kline': '/market/candles',  # https://www.okx.com/docs-v5/en/#public-data
        'info': '/public/instruments'
    }
    category: str = 'spot'
    spot_coins: list = []
    info_resp: dict = {}
    info_ts: int = 0
    kline_ts: int = 0

    def __init__(self):
        """
        Initialization of a class instance by obtaining exchange's list of available spot pairs
        """
        url: str = self.url + self.endpoint_dict['info']
        try:
            params = {
                'instType': 'SPOT'
            }
            resp = requests.get(url=url, params=params)
            if resp.ok:
                self.info_resp = resp.json()
                self.spot_coins = [[coin['instId'], coin['baseCcy'], coin['quoteCcy'], coin['state']] for coin in resp.json()['data']]
                self.info_ts = calendar.timegm(datetime.datetime.strptime(resp.headers.get('Date', 'Thu, 01 Jan 1970 00:00:00 GMT'), '%a, %d %b %Y %H:%M:%S %Z').timetuple()) * 1000
            else:
                print('Okx coins domain is unreacheable')
            print('Okx initialized')
        except JSONDecodeError as json_err:
            print(f'Exception: Okx JSONDecodeError {json_err = }')
        except Exception as msg:
            print(f'Exception: Okx init {msg}')


    def _kline(self, 
               symbol: str,
               limit: int | None = None,
               start_dt: datetime.datetime | None = None,
               end_dt: datetime.datetime | None = None,
    ) -> list:
        """
        Incremental (last day): 
        _kline(
            base_coin_list[0], 
            start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1), 
            limit=1)
        """
        url: str = self.url + self.endpoint_dict['kline']
        params: dict = {
            'bar': '1Dutc',
            'instId': symbol
        }
        if not any([limit, start_dt, end_dt]):
            tmp_start_dt = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
            tmp_end_dt = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=1)
            params['before'] = calendar.timegm(tmp_start_dt.date().timetuple()) * 1000
            params['after'] = calendar.timegm(tmp_end_dt.date().timetuple()) * 1000
        elif limit and not any([start_dt, end_dt]):
            params['limit'] = limit
        elif limit and start_dt and not end_dt:
            params['before'] = calendar.timegm(start_dt.date().timetuple()) * 1000
            params['limit'] = limit
        elif limit and not start_dt and end_dt:
            params['after'] = calendar.timegm(end_dt.date().timetuple()) * 1000
            params['limit'] = limit

        if start_dt:
            params['before'] = calendar.timegm(start_dt.date().timetuple()) * 1000
        if end_dt:
            params['after'] = calendar.timegm(end_dt.date().timetuple()) * 1000
        try:
            resp = requests.get(url=url, params=params) 
            if resp.ok:
                self.kline_ts = calendar.timegm(datetime.datetime.strptime(resp.headers.get('Date', 'Thu, 01 Jan 1970 00:00:00 GMT'), '%a, %d %b %Y %H:%M:%S %Z').timetuple()) * 1000
                return resp.json()
            else:
                print(f'Error: Okx kline endpoint {resp.status_code = }')
                return []
        except JSONDecodeError as json_err:
            print(f'Exception: Okx JSONDecodeError {json_err = }, {params = }')
            return []
        except Exception as msg:
            print(f'Exception: Okx get_kline {msg}, {params = }')
            return []
        

    def load_kline(self, 
                   mode: Literal['inc', 'init', 'custom'] = 'inc',
                   limit: int | None = None,
                   start_dt: datetime.datetime | None = None,
                   end_dt: datetime.datetime | None = None,
        ) -> list:
        """
        Function manages the process of kline data collection:
            1. inc - incremental (last day)
            2. init - initial (last 300 days)
            3. custom - requires start_dt || end_dt || limit to be provided
        """
        if mode == 'inc':
            return [
                (
                    coin[0].replace('-', ''),
                    self._kline(symbol=coin[0],
                                start_dt=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=2), 
                                limit=1)
                )
                for coin in self.spot_coins
            ]
        elif mode == 'init':
            return [
                (
                    coin[0].replace('-', ''),
                    self._kline(symbol=coin[0],
                                limit=300)
                )
                for coin in self.spot_coins
            ]
        elif mode == 'custom':
            return [
                (
                    coin[0].replace('-', ''),
                    self._kline(symbol=coin[0],
                            limit=limit,
                            start_dt=start_dt,
                            end_dt=end_dt)
                )
                for coin in self.spot_coins
            ]