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

    @abstractmethod
    def _parse_response(self,response:str) -> pandas.DataFrame:
        """_summary_
        Abstract method to parse response from stock API.
        Args:
            response (str): response from stock API

        Returns:
            pandas.DataFrame: DataFrame of stock data
        """
        pass


    
    #Todo: Add generic caching methods.

    
        
        
    
