import stockfetch.core.data_api_client as data_api_client
import stockfetch.api.NSE_Client as NSE_Client

if __name__ == '__main__':
    print("Testing script")
    nse_client_instance = NSE_Client.NSE_Client()
    nse_client_instance.check_module_health()
    print("Test complete")