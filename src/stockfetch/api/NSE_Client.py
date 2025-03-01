# Client side wrapper for NSE API
from stockfetch.core.data_api_client import DataAPIClient
import httpx
import sys


class NSE_Client(DataAPIClient):
    def __init__(self):
        super().__init__()
        base_url = "https://www.nseindia.com/api"
        headers = {
            "User-Agent": "StockFetch/0.1",
            "Accept": "application/json",
            "accept-language": "en-US,en;q=0.9,en-IN;q=0.8,en-GB;q=0.7",
            "cache-control": "no-cache",
        }

    def _sanitize_uri(self, uri):
        santized_uri = uri.replace(" ", "%20")
        return santized_uri

    def _build_request(self, endpoint: str, params: dict):
        request_url = f"{self.base_url}/{endpoint}"

        if params:
            request_url = f"{request_url}?"
            for key, value in params.items():
                request_url = f"{request_url}{key}={value}&"

        request_url = self._sanitize_uri(request_url)

    def _parse_response(self, response):
        return super()._parse_response(response)
    
    def check_module_health(self):
        print("Health check for NSE_Client")
    


