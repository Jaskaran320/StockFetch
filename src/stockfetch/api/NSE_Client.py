# Client side wrapper for NSE API
from __future__ import annotations

import httpx
import pandas as pd
from stockfetch.core.data_api_client import DataAPIClient


class NSE_Client(DataAPIClient):
    def __init__(self):
        super().__init__()
        # self.today = datetime.today()
        print('➡️ NSE India client initialized')
        self.base_api_url: str = 'https://www.nseindia.com/api'
        self.home_url: str = 'https://www.nseindia.com/'
        self.headers: dict = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9,en-IN;q=0.8,en-GB;q=0.7',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
        }
        self.client = httpx.Client(
            headers=self.headers,
            follow_redirects=True,
            timeout=30.0,
        )
        self._init_session()

    def _init_session(self):
        try:
            response = self.client.get(self.home_url)
            if response.status_code == 200:
                print('✅ Session initialized successfully')
            else:
                print(
                    f"❌ Session initialization failed: {response.status_code}",
                )
        except Exception as e:
            print(f"❌ Session initialization error: {e}")

    def __del__(self):
        self.client.close()
        print('➡️ NSE India client deinitialized')

    def _sanitize_uri(self, uri):
        santized_uri = uri.replace(' ', '%20')
        return santized_uri

    def _build_request(self, endpoint: str, params: dict):
        request_url = f"{self.base_api_url}/{endpoint}"

        if params:
            request_url = f"{request_url}?"
            for key, value in params.items():
                request_url = f"{request_url}{key}={value}&"

        request_url = self._sanitize_uri(request_url)
        return request_url

    def get_market_holidays(self):
        endpoint = 'holiday-master'
        params = {'type': 'trading'}
        endpoint_url = self._build_request(endpoint, params)
        try:
            response = self.client.get(endpoint_url)
            if response.status_code == 200:
                print('✅ Response Fetched')
                return response.json()
            else:
                print(f"❌ Status code: {response.status_code}")
        except Exception as e:
            print(f"Error Occured:{e}")

    def get_historical_ohlc_data(
        self,
        symbol,
        start_date,
        end_date,
        data_folder_path=None,
    ):
        endpoint = 'historical/cm/equity'

        params = {
            'symbol': symbol,
            'series': '[%22EQ%22]',
            'from': start_date,
            'to': end_date,
        }
        endpoint_url = self._build_request(endpoint, params)
        print(endpoint_url)
        try:
            response = self.client.get(endpoint_url)
            if response.status_code == 200:
                print('✅ Response Fetched')
                response_dataframe = pd.DataFrame(response.json()['data'])
                if data_folder_path:
                    self._dump_data(
                        response_dataframe,
                        'test_dump',
                        data_folder_path,
                        compress=False,
                    )
                return response_dataframe
            else:
                print(f"❌ Status code: {response.status_code}")
        except Exception as e:
            print(f"Error Occured:{e}")
