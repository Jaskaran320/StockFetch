import asyncio
import contextlib
from pprint import pprint

# make this absolute import
from .enums import (
    InstrumentType,
    PreopenKey,
    SortType,
    BandType,
    BandView,
    LargeDealType,
    MarketSegment,
    ResultPeriod,
)
# make this absolute import
from .serve import NSEIndia


async def test_nseindia_functions():
    async with NSEIndia() as nse_instance:
        try:
            print("Testing get_index_quote:")
            index_quote = await nse_instance.get_index_quote("NIFTY 50")
            pprint(index_quote)
        except Exception as error:
            print(f"Error in get_index_quote: {error}")

        try:
            print("\nTesting get_advances_declines:")
            advances_declines = await nse_instance.get_advances_declines("pandas")
            pprint(advances_declines)
        except Exception as error:
            print(f"Error in get_advances_declines: {error}")

        try:
            print("\nTesting get_top_losers:")
            top_losers = await nse_instance.get_top_losers()
            pprint(top_losers)
        except Exception as error:
            print(f"Error in get_top_losers: {error}")

        try:
            print("\nTesting get_top_gainers:")
            top_gainers = await nse_instance.get_top_gainers()
            pprint(top_gainers)
        except Exception as error:
            print(f"Error in get_top_gainers: {error}")

        try:
            print("\nTesting get_india_vix:")
            india_vix = await nse_instance.get_india_vix()
            print(india_vix)
        except Exception as error:
            print(f"Error in get_india_vix: {error}")

        try:
            print("\nTesting get_index_info:")
            index_info = await nse_instance.get_index_info("NIFTY 50")
            pprint(index_info)
        except Exception as error:
            print(f"Error in get_index_info: {error}")

        try:
            print("\nTesting calculate_black_scholes:")
            bs_result = await nse_instance.calculate_black_scholes(
                100, 90, 1, sigma=0.2, r=0.1, q=0.0, td=365
            )
            pprint(bs_result)
        except Exception as error:
            print(f"Error in calculate_black_scholes: {error}")

        try:
            print("\nTesting get_equity_history:")
            equity_history = await nse_instance.get_equity_history(
                "INFY", "01-01-2024", "31-01-2024"
            )
            pprint(equity_history)
        except Exception as error:
            print(f"Error in get_equity_history: {error}")

        try:
            print("\nTesting get_derivative_history:")
            derivative_history = await nse_instance.get_derivative_history(
                "INFY",
                "01-01-2024",
                "31-01-2024",
                InstrumentType.OPTION_STOCK,
                "31-01-2024",
            )
            pprint(derivative_history)
        except Exception as error:
            print(f"Error in get_derivative_history: {error}")

        try:
            print("\nTesting get_expiry_history:")
            expiry_history = await nse_instance.get_expiry_history(
                "INFY", "01-01-2024", "31-01-2024", "options"
            )
            pprint(expiry_history)
        except Exception as error:
            print(f"Error in get_expiry_history: {error}")

        try:
            print("\nTesting get_index_history:")
            index_history = await nse_instance.get_index_history(
                "NIFTY 50", "01-01-2024", "31-01-2024"
            )
            pprint(index_history)
        except Exception as error:
            print(f"Error in get_index_history: {error}")

        try:
            print("\nTesting get_index_pe_pb_div:")
            index_pe_pb_div = await nse_instance.get_index_pe_pb_div(
                "NIFTY 50", "01-01-2024", "31-01-2024"
            )
            pprint(index_pe_pb_div)
        except Exception as error:
            print(f"Error in get_index_pe_pb_div: {error}")

        try:
            print("\nTesting get_index_total_returns:")
            index_total_returns = await nse_instance.get_index_total_returns(
                "NIFTY 50", "01-01-2024", "31-01-2024"
            )
            pprint(index_total_returns)
        except Exception as error:
            print(f"Error in get_index_total_returns: {error}")

        try:
            print("\nTesting get_bhavcopy:")
            bhavcopy_data = nse_instance.get_bhavcopy("07-02-2025")
            pprint(bhavcopy_data.head())
        except Exception as error:
            print(f"Error in get_bhavcopy: {error}")

        try:
            print("\nTesting get_bulk_deals_data:")
            bulk_deals = nse_instance.get_bulk_deals_data()
            pprint(bulk_deals.head())
        except Exception as error:
            print(f"Error in get_bulk_deals_data: {error}")

        try:
            print("\nTesting get_block_deals_data:")
            block_deals = nse_instance.get_block_deals_data()
            pprint(block_deals.head())
        except Exception as error:
            print(f"Error in get_block_deals_data: {error}")

        try:
            print("\nTesting calculate_beta:")
            beta_value = await nse_instance.calculate_beta(
                "INFY", days=365, symbol2="NIFTY 50"
            )
            print(beta_value)
        except Exception as error:
            print(f"Error in calculate_beta: {error}")

        try:
            print("\nTesting get_preopen_data:")
            preopen_data = await nse_instance.get_preopen_data(PreopenKey.NIFTY, "pandas")
            pprint(preopen_data.head())
        except Exception as error:
            print(f"Error in get_preopen_data: {error}")

        try:
            print("\nTesting get_preopen_movers:")
            preopen_movers = await nse_instance.get_preopen_movers(PreopenKey.FNO, 1.5)
            pprint(preopen_movers)
        except Exception as error:
            print(f"Error in get_preopen_movers: {error}")

        try:
            print("\nTesting get_most_active:")
            most_active = await nse_instance.get_most_active("securities", SortType.VALUE)
            pprint(most_active.head())
        except Exception as error:
            print(f"Error in get_most_active: {error}")

        try:
            print("\nTesting get_price_band_hitters:")
            price_band = await nse_instance.get_price_band_hitters(
                BandType.BOTH, BandView.ALL
            )
            pprint(price_band.head())
        except Exception as error:
            print(f"Error in get_price_band_hitters: {error}")

        try:
            print("\nTesting get_large_deals:")
            large_deals = await nse_instance.get_large_deals(LargeDealType.BULK)
            pprint(large_deals.head())
        except Exception as error:
            print(f"Error in get_large_deals: {error}")

        try:
            print("\nTesting get_large_deals_historical:")
            large_deals_hist = await nse_instance.get_large_deals_historical(
                "01-01-2024", "31-01-2024", LargeDealType.BULK
            )
            pprint(large_deals_hist)
        except Exception as error:
            print(f"Error in get_large_deals_historical: {error}")

        try:
            print("\nTesting get_fao_participant_oi:")
            fao_data = nse_instance.get_fao_participant_oi("28-01-2025")
            pprint(fao_data.head())
        except Exception as error:
            print(f"Error in get_fao_participant_oi: {error}")

        try:
            print("\nTesting is_market_open_today:")
            market_status = await nse_instance.is_market_open_today(MarketSegment.FO)
            print(market_status)
        except Exception as error:
            print(f"Error in is_market_open_today: {error}")

        try:
            print("\nTesting get_security_wise_archive:")
            security_archive = await nse_instance.get_security_wise_archive(
                "01-01-2024", "31-01-2024", "INFY", "EQ"
            )
            pprint(security_archive.head())
        except Exception as error:
            print(f"Error in get_security_wise_archive: {error}")

        try:
            print("\nTesting get_option_chain:")
            option_chain = await nse_instance.get_option_chain("INFY")
            pprint(option_chain)
        except Exception as error:
            print(f"Error in get_option_chain: {error}")

        try:
            print("\nTesting build_option_chain:")
            built_option_chain = await nse_instance.build_option_chain(
                "INFY", expiry="latest", oi_mode="full"
            )
            pprint(built_option_chain)
        except Exception as error:
            print(f"Error in build_option_chain: {error}")

        # try:
        #     print("\nTesting get_quote:")
        #     quote_info = await nse_instance.get_quote("INFY")
        #     pprint(quote_info)
        # except Exception as error:
        #     print(f"Error in get_quote: {error}")

        try:
            print("\nTesting get_expiry_details:")
            expiry_details = await nse_instance.get_expiry_details(
                "INFY", meta="Futures", i=0
            )
            pprint(expiry_details)
        except Exception as error:
            print(f"Error in get_expiry_details: {error}")

        try:
            print("\nTesting get_pcr:")
            pcr_value = await nse_instance.get_pcr("INFY", expiry_index=0)
            print(pcr_value)
        except Exception as error:
            print(f"Error in get_pcr: {error}")

        try:
            print("\nTesting get_quote_ltp:")
            quote_ltp_value = await nse_instance.get_quote_ltp(
                "INFY", expiry_date="latest", option_type="FUT", strike_price=0.0
            )
            print(quote_ltp_value)
        except Exception as error:
            print(f"Error in get_quote_ltp: {error}")

        try:
            print("\nTesting get_quote_metadata:")
            quote_meta_info = await nse_instance.get_quote_metadata(
                "INFY", expiry_date="latest", option_type="FUT", strike_price=0.0
            )
            pprint(quote_meta_info)
        except Exception as error:
            print(f"Error in get_quote_metadata: {error}")

        try:
            print("\nTesting get_option_chain_ltp:")
            option_chain_payload = await nse_instance.get_option_chain("INFY")
            option_chain_ltp = nse_instance.get_option_chain_ltp(
                option_chain_payload,
                strike_price=1900,
                option_type="CE",
                expiry_index=0,
                intent="buy",
            )
            print(option_chain_ltp)
        except Exception as error:
            print(f"Error in get_option_chain_ltp: {error}")

        # try:
        #     print("\nTesting get_equity_info:")
        #     equity_info = await nse_instance.get_equity_info("INFY")
        #     pprint(equity_info)
        # except Exception as error:
        #     print(f"Error in get_equity_info: {error}")

        # try:
        #     print("\nTesting get_derivative_info:")
        #     derivative_info = await nse_instance.get_derivative_info("INFY")
        #     pprint(derivative_info)
        # except Exception as error:
        #     print(f"Error in get_derivative_info: {error}")

        # try:
        #     print("\nTesting get_holidays:")
        #     holidays = await nse_instance.get_holidays(HolidayType.TRADING)
        #     pprint(holidays)
        # except Exception as error:
        #     print(f"Error in get_holidays: {error}")

        try:
            print("\nTesting get_corporate_results:")
            corporate_results = await nse_instance.get_corporate_results(
                "equities", ResultPeriod.QUARTERLY
            )
            pprint(corporate_results)
        except Exception as error:
            print(f"Error in get_corporate_results: {error}")

        try:
            print("\nTesting get_events:")
            events_calendar = await nse_instance.get_events()
            pprint(events_calendar.head())
        except Exception as error:
            print(f"Error in get_events: {error}")

        try:
            print("\nTesting get_past_results:")
            past_results = await nse_instance.get_past_results("INFY")
            pprint(past_results)
        except Exception as error:
            print(f"Error in get_past_results: {error}")

        try:
            print("\nTesting get_simple_moving_average_absolute:")
            sma_abs = await nse_instance.get_simple_moving_average_absolute("INFY", "01-01-2024", "31-01-2024")
            print(sma_abs)
        except Exception as error:
            print(f"Error in get_simple_moving_average_absolute: {error}")

        try:
            print("\nTesting get_simple_moving_average:")
            sma = await nse_instance.get_simple_moving_average("INFY", days=50)
            print(sma)
        except Exception as error:
            print(f"Error in get_simple_moving_average: {error}")

        try:
            print("\nTesting get_exponential_moving_average:")
            ema = await nse_instance.get_exponential_moving_average("INFY", days=50)
            print(ema)
        except Exception as error:
            print(f"Error in get_exponential_moving_average: {error}")

        try:
            print("\nTesting get_double_exponential_moving_average:")
            dema = await nse_instance.get_double_exponential_moving_average("INFY", days=50)
            print(dema)
        except Exception as error:
            print(f"Error in get_double_exponential_moving_average: {error}")

        try:
            print("\nTesting get_triple_exponential_moving_average:")
            tema = await nse_instance.get_triple_exponential_moving_average("INFY", days=50)
            print(tema)
        except Exception as error:
            print(f"Error in get_triple_exponential_moving_average: {error}")

        try:
            print("\nTesting get_relative_strength_index:")
            rsi = await nse_instance.get_relative_strength_index("INFY", days=14)
            print(rsi)
        except Exception as error:
            print(f"Error in get_relative_strength_index: {error}")

        try:
            print("\nTesting get_moving_average_convergence_divergence:")
            macd = await nse_instance.get_moving_average_convergence_divergence("INFY", get_signal=True)
            print(macd)
        except Exception as error:
            print(f"Error in get_moving_average_convergence_divergence: {error}")

        try:
            print("\nTesting get_stochastic_oscillator:")
            stochastic = await nse_instance.get_stochastic_oscillator("INFY")
            print(stochastic)
        except Exception as error:
            print(f"Error in get_stochastic_oscillator: {error}")

        try:
            print("\nTesting get_bollinger_bands:")
            bollinger = await nse_instance.get_bollinger_bands("INFY")
            print(bollinger)
        except Exception as error:
            print(f"Error in get_bollinger_bands: {error}")

        try:
            print("\nTesting get_average_directional_index:")
            adx = await nse_instance.get_average_directional_index("INFY")
            print(adx)
        except Exception as error:
            print(f"Error in get_average_directional_index: {error}")

        try:
            print("\nTesting get_commodity_channel_index:")
            cci = await nse_instance.get_commodity_channel_index("INFY")
            print(cci)
        except Exception as error:
            print(f"Error in get_commodity_channel_index: {error}")

        try:
            print("\nTesting get_ichimoku_cloud:")
            ichimoku = await nse_instance.get_ichimoku_cloud("INFY")
            print(ichimoku)
        except Exception as error:
            print(f"Error in get_ichimoku_cloud: {error}")

        try:
            print("\nTesting get_fibonacci_retracement:")
            fibonacci = await nse_instance.get_fibonacci_retracement("INFY")
            print(fibonacci)
        except Exception as error:
            print(f"Error in get_fibonacci_retracement: {error}")

        try:
            print("\nTesting get_support_and_resistance_levels:")
            support_resistance = await nse_instance.get_support_and_resistance_levels("INFY", days=50)
            print(support_resistance)
        except Exception as error:
            print(f"Error in get_support_and_resistance_levels: {error}")


if __name__ == "__main__":
    OUTPUT_FILE = "nseindia_output.txt"

    async def main():
        with open(OUTPUT_FILE, "w") as file:
            with contextlib.redirect_stdout(file):
                await test_nseindia_functions()

    asyncio.run(main())
    # asyncio.run(test_nseindia_functions())
