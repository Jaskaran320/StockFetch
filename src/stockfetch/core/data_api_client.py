# Factory class for different Stock Market APIs
from abc import ABC, abstractmethod
import pandas


class DataAPIClient(ABC):
    def __init__(self):
        print("Stock API Client initialized ✅")
        
        
        
    # API Provider management
    def _set_API_key(self,API_key:str):
        """_summary_
        Set API key (if any) for API.
        Args:
            API_key (str): _description_
        """

    # API Request handling
    @abstractmethod
    def _build_request(self,url:str ,params:dict) -> str: 
        """_summary_
        Abstract method to build request for fetching stock data.
        Args:
            url (str): url endpoint for stock API
            params (dict): parameters for stock API

        Returns:
            str: endpoint url with parameters
        """
        pass

        pass
    
    @abstractmethod
    def get_market_holidays(self) -> dict: 
        """_summary_
        Abstract method to get market holidays.
        Returns:
            dict: Market holidays
        """
        pass

    @abstractmethod
    def get_historical_ohlc_data(self, symbol:str, start_date:str, end_date:str) -> pandas.DataFrame:
        """_summary_
        Abstract method to get equity OHLC data.
        Args:
            symbol (str): Stock symbol
            start_date (str): Start date for data
            end_date (str): End date for data

        Returns:
            pandas.DataFrame: OHLC data
        """
        pass
    #Todo: Add generic caching methods.

    
        
        
    
