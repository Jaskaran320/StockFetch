import stockfetch.core.data_api_client as data_api_client
import stockfetch.api.NSE_Client as NSE_Client

if __name__ == '__main__':
    print("Testing script")
    nse_client_instance = NSE_Client.NSE_Client()
    symbol = "TATACONSUM"
    start_date = "01-02-2000"
    end_date = "01-01-2001"
    df = nse_client_instance.get_historical_ohlc_data(symbol,start_date,end_date)
    print(df)