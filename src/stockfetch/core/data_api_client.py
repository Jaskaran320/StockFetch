# Factory class for different Stock Market APIs
from __future__ import annotations

import datetime
import json
import os
import pickle
from abc import ABC
from abc import abstractmethod

import pandas


class DataAPIClient(ABC):
    def __init__(self):
        print('✅ Stock API Client initialized')

    # API Provider management
    def _set_API_key(self, API_key: str):
        """_summary_
        Set API key (if any) for API.
        Args:
            API_key (str): _description_
        """

    # API Request handling
    @abstractmethod
    def _build_request(self, url: str, params: dict) -> str:
        """_summary_
        Abstract method to build request for fetching stock data.
        Args:
            url (str): url endpoint for stock API
            params (dict): parameters for stock API

        Returns:
            str: endpoint url with parameters
        """

    @abstractmethod
    def get_market_holidays(self) -> dict:
        """_summary_
        Abstract method to get market holidays.
        Returns:
            dict: Market holidays
        """
        pass

    @abstractmethod
    def get_historical_ohlc_data(
        self, symbol: str, start_date: str, end_date: str, data_folder_path: str,
    ) -> pandas.DataFrame:
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

    # API Response Handling

    def _dump_data(
        self,
        data: dict | pandas.DataFrame | list | str,
        file_name: str,
        data_folder_path: str,
        compress: bool = False,
    ) -> None:
        """_summary_
        Dump data to file.
        Args:
            data (dict | pandas.DataFrame | list): Data to dump
            file_name (str): File name
            data_folder_path (str): Path to data folder
            compress (bool, optional): Compress data. Defaults to False.
        """

        if os.path.exists(data_folder_path):
            print('📁 Data folder exists')
        else:
            print('📁 New Folder created')
            os.mkdir(data_folder_path)

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
        data_file_name = f"{file_name}_{timestamp}"
        extension = None

        if compress:
            extension = '.pkl'
            pickle.dump(
                data,
                open(
                    os.path.join(
                        data_folder_path,
                        data_file_name + extension,
                    ), 'wb',
                ),
            )
        elif isinstance(data, pandas.DataFrame):
            extension = '.csv'
            data.to_csv(
                os.path.join(
                    data_folder_path,
                    data_file_name + extension,
                ),
            )
        elif isinstance(data, dict):
            extension = '.json'
            json.dump(
                data,
                open(
                    os.path.join(
                        data_folder_path,
                        data_file_name + extension,
                    ), 'w',
                ),
            )
        else:
            extension = '.txt'
            with open(
                os.path.join(
                    data_folder_path,
                    data_file_name + extension,
                ), 'w+',
            ) as f:
                f.write(data)
