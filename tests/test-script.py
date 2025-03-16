from __future__ import annotations

import os

import stockfetch.api.NSE_Client as NSE_Client
if __name__ == '__main__':
    nse_client_instance = NSE_Client.NSE_Client()
    symbol = 'DOMS'
    start_date = '01-02-2024'
    end_date = '01-05-2024'
    data_directory_path = os.path.join(os.getcwd(), 'duops')
    nse_client_instance.set_data_directory_path(data_directory_path)
    df = nse_client_instance.get_historical_ohlc_data(
        symbol, start_date, end_date, data_directory_path=os.path.join(
            os.getcwd(), 'dumps',
        ),
    )
