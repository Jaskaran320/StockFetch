from __future__ import annotations

import stockfetch.api.NSE_Client as NSE_Client

if __name__ == '__main__':
    print('Testing script')
    nse_client_instance = NSE_Client.NSE_Client()
    symbol = 'TATACONSUM'
    start_date = '01-02-2024'
    end_date = '01-05-2024'
    df = nse_client_instance.get_historical_ohlc_data(
        symbol, start_date, end_date, data_folder_path='C:\\Users\\ahuja\\Desktop\\Projects\\StockFetch\\StockFetch\\tests\\dumps',
    )
    print(df.head())
