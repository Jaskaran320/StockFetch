import asyncio
from functools import cache
import os
import numpy as np
import requests
import aiohttp
import pandas as pd
import json
import datetime
import logging
import urllib.parse
from typing import Union, List, Dict
from scipy.stats import norm
import math

# make this absolute import
from .enums import (
    Mode,
    Index,
    HolidayType,
    ResultPeriod,
    OptionType,
    InstrumentType,
    SortType,
    PreopenKey,
    BandType,
    BandView,
    LargeDealType,
    MarketSegment,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s"
)


class NSEFetcher:
    """
    Handles fetching data from NSE, abstracting away the underlying method.
    """

    def __init__(self, mode: Mode = Mode.LOCAL):
        self.mode = mode
        self.HEADERS = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,en-IN;q=0.8,en-GB;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
        }
        self.CURL_HEADERS = (
            ' -H "authority: beta.nseindia.com" -H "cache-control: max-age=0" '
            '-H "dnt: 1" -H "upgrade-insecure-requests: 1" '
            '-H "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36" '
            '-H "sec-fetch-user: ?1" -H "accept: text/html,application/xhtml+xml,application/xml;q=0.9,'
            'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9" '
            '-H "sec-fetch-site: none" -H "sec-fetch-mode: navigate" '
            '-H "accept-encoding: gzip, deflate, br" -H "accept-language: en-US,en;q=0.9,hi;q=0.8" --compressed'
        )
        self.NIFTY_INDICES_HEADERS = {
            "Connection": "keep-alive",
            "sec-ch-ua": '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "DNT": "1",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua-mobile": "?0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36",
            "Content-Type": "application/json; charset=UTF-8",
            "Origin": "https://niftyindices.com",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://niftyindices.com/reports/historical-data",
        }
        self.session = None

    async def _init_session(self):
        """Initialize the aiohttp session"""
        if self.session is None:
            self.session = aiohttp.ClientSession(headers=self.HEADERS)
        return self

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    async def _fetch(self, payload: str) -> dict:
        try:
            if self.mode == Mode.VPN:
                return await self._fetch_vpn(payload)
            else:
                return await self._fetch_local(payload)
        except (aiohttp.ClientError, ValueError, json.JSONDecodeError) as error:
            logging.error(f"Error fetching data: {error}")
            raise

    async def _fetch_local(self, payload: str) -> dict:
        try:
            async with self.session.get(payload) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, ValueError, json.JSONDecodeError):
            logging.info("Retrying with a new session...")
            await self.close()
            self.session = aiohttp.ClientSession(headers=self.HEADERS)
            async with self.session.get("http://nseindia.com") as _:
                pass
            async with self.session.get(payload) as response:
                response.raise_for_status()
                return await response.json()

    async def _fetch_vpn(self, payload: str) -> dict:
        encoded_url = (
            urllib.parse.quote(payload, safe=":/?&=")
            if ("%26" not in payload and "%20" not in payload)
            else payload
        )
        payload_command = f'curl -b cookies.txt "{encoded_url}"{self.CURL_HEADERS}'
        try:
            output = os.popen(payload_command).read()
            return json.loads(output)
        except ValueError:
            logging.info("Retrying VPN fetch after cookie refresh...")
            os.popen(f'curl -c cookies.txt "https://www.nseindia.com"{self.CURL_HEADERS}').read()
            output = os.popen(payload_command).read()
            return json.loads(output)
        
    async def _fetch_pdf(self, url: str) -> bytes:
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.read()
        except aiohttp.ClientError as error:
            logging.error(f"Error fetching PDF: {error}")
            raise

    async def fetch_niftyindices(self, url: str, data: dict) -> pd.DataFrame:
        try:
            async with self.session.post(url, headers=self.NIFTY_INDICES_HEADERS, json=data) as response:
                response.raise_for_status()
                json_response = await response.json()
                return pd.DataFrame.from_records(json.loads(json_response["d"]))
        except (aiohttp.ClientError, ValueError, json.JSONDecodeError) as error:
            logging.error(f"Error fetching data from NiftyIndices: {error}")
            raise


class NSEIndia:
    """
    Main class for interacting with the NSE website.
    """

    def __init__(self, mode: Mode = Mode.LOCAL):
        self.fetcher = NSEFetcher(mode)
        self._fno_symbols = None
        self._eq_symbols = None

    async def __aenter__(self):
        """Async context manager enter"""
        await self.fetcher._init_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.fetcher.close()

    def _running_status(self) -> bool:
        """Checks if the market is currently running."""
        now = datetime.datetime.now()
        start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        return start < now < end

    def _filter_expiry_dates(self, expiry_dates_str: List[str]) -> List[str]:
        """
        Filters and sorts expiry dates to include only those on or after today.
        """
        expiry_dates = [
            datetime.datetime.strptime(date, "%d-%b-%Y").date()
            for date in expiry_dates_str
        ]
        expiry_dates = [
            date.strftime("%d-%b-%Y")
            for date in expiry_dates
            if date >= datetime.datetime.now().date()
        ]
        return expiry_dates

    def _purify_symbol(self, symbol: str) -> str:
        return symbol.replace("&", "%26")

    async def get_fno_symbols(self) -> List[str]:
        """Gets the list of FNO symbols."""
        if self._fno_symbols is None:
            logging.info("Fetching FNO symbols...")
            fno_data = await self.fetcher._fetch(
                "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
            )
            data = fno_data["data"]
            self._fno_symbols = ["NIFTY", "NIFTYIT", "BANKNIFTY"] + [
                item["symbol"] for item in data
            ]
        return self._fno_symbols

    def get_equity_symbols(self) -> List[str]:
        """Gets the list of equity symbols."""
        if self._eq_symbols is None:
            logging.info("Fetching Equity symbols...")            
            eq_list_pd = pd.read_csv(
                "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
            )
            self._eq_symbols = eq_list_pd["SYMBOL"].tolist()
        return self._eq_symbols

    async def is_valid_symbol(self, symbol: str, fno: bool = False) -> bool:
        """
        Checks if provided symbol is valid or not.

        :param symbol:
        :param fno: True if symbol is of Future and Options segment. Default: False
        :return:
        """
        if fno:
            return symbol.upper() in await self.get_fno_symbols()
        else:
            return symbol.upper() in self.get_equity_symbols()

    async def get_option_chain(self, symbol: str) -> dict:
        """Gets the option chain for a given symbol."""
        symbol = self._purify_symbol(symbol)
        if symbol in [ind.value for ind in Index]:
            url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
        else:
            url = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
        return await self.fetcher._fetch(url)

    async def build_option_chain(
        self, symbol: str, expiry: str = "latest", oi_mode: str = "full"
    ) -> tuple[pd.DataFrame, float, str]:
        """
        Builds an option chain DataFrame.

        :param symbol:
        :param expiry: "latest", "next" or specific date in "dd-mmm-yyyy" format
        :param oi_mode: "full" or "compact"
        :return: (option_chain_df, underlying_value, timestamp)
        """

        if not await self.is_valid_symbol(symbol):
            raise ValueError(f"Invalid Symbol {symbol} provided")

        payload = await self.get_option_chain(symbol)

        if expiry == "latest":
            expiry = payload["records"]["expiryDates"][0]
        elif expiry == "next":
            expiry_dates = self._filter_expiry_dates(payload["records"]["expiryDates"])
            expiry = (
                expiry_dates[1] if len(expiry_dates) > 1 else expiry_dates[0]
            )

        if oi_mode == "compact":
            columns = [
                "CALLS_OI",
                "CALLS_Chng in OI",
                "CALLS_Volume",
                "CALLS_IV",
                "CALLS_LTP",
                "CALLS_Net Chng",
                "Strike Price",
                "PUTS_OI",
                "PUTS_Chng in OI",
                "PUTS_Volume",
                "PUTS_IV",
                "PUTS_LTP",
                "PUTS_Net Chng",
            ]
        elif oi_mode == "full":
            columns = [
                "CALLS_Chart",
                "CALLS_OI",
                "CALLS_Chng in OI",
                "CALLS_Volume",
                "CALLS_IV",
                "CALLS_LTP",
                "CALLS_Net Chng",
                "CALLS_Bid Qty",
                "CALLS_Bid Price",
                "CALLS_Ask Price",
                "CALLS_Ask Qty",
                "Strike Price",
                "PUTS_Bid Qty",
                "PUTS_Bid Price",
                "PUTS_Ask Price",
                "PUTS_Ask Qty",
                "PUTS_Net Chng",
                "PUTS_LTP",
                "PUTS_IV",
                "PUTS_Volume",
                "PUTS_Chng in OI",
                "PUTS_OI",
                "PUTS_Chart",
            ]
        else:
            raise ValueError("Invalid oi_mode.  Must be 'full' or 'compact'.")

        oi_data = pd.DataFrame(columns=columns)
        oi_row: Dict[str, Union[int, float]] = {
            col: 0 for col in columns if col != "Strike Price"
        }

        for item in payload["records"]["data"]:
            if item["expiryDate"] == expiry:
                oi_row["Strike Price"] = item["strikePrice"]

                for option_type in ["CE", "PE"]:
                    try:
                        oi_row[
                            (
                                f"{option_type}CALLS_OI"
                                if option_type == "CE"
                                else f"{option_type}PUTS_OI"
                            )
                        ] = item[option_type]["openInterest"]
                        oi_row[
                            (
                                f"{option_type}CALLS_Chng in OI"
                                if option_type == "CE"
                                else f"{option_type}PUTS_Chng in OI"
                            )
                        ] = item[option_type]["changeinOpenInterest"]
                        oi_row[
                            (
                                f"{option_type}CALLS_Volume"
                                if option_type == "CE"
                                else f"{option_type}PUTS_Volume"
                            )
                        ] = item[option_type]["totalTradedVolume"]
                        oi_row[
                            (
                                f"{option_type}CALLS_IV"
                                if option_type == "CE"
                                else f"{option_type}PUTS_IV"
                            )
                        ] = item[option_type]["impliedVolatility"]
                        oi_row[
                            (
                                f"{option_type}CALLS_LTP"
                                if option_type == "CE"
                                else f"{option_type}PUTS_LTP"
                            )
                        ] = item[option_type]["lastPrice"]
                        oi_row[
                            (
                                f"{option_type}CALLS_Net Chng"
                                if option_type == "CE"
                                else f"{option_type}PUTS_Net Chng"
                            )
                        ] = item[option_type]["change"]

                        if oi_mode == "full":
                            oi_row[
                                (
                                    f"{option_type}CALLS_Bid Qty"
                                    if option_type == "CE"
                                    else f"{option_type}PUTS_Bid Qty"
                                )
                            ] = item[option_type]["bidQty"]
                            oi_row[
                                (
                                    f"{option_type}CALLS_Bid Price"
                                    if option_type == "CE"
                                    else f"{option_type}PUTS_Bid Price"
                                )
                            ] = item[option_type]["bidprice"]
                            oi_row[
                                (
                                    f"{option_type}CALLS_Ask Price"
                                    if option_type == "CE"
                                    else f"{option_type}PUTS_Ask Price"
                                )
                            ] = item[option_type]["askPrice"]
                            oi_row[
                                (
                                    f"{option_type}CALLS_Ask Qty"
                                    if option_type == "CE"
                                    else f"{option_type}PUTS_Ask Qty"
                                )
                            ] = item[option_type]["askQty"]
                    except KeyError:
                        pass
                if oi_mode == "full":
                    oi_row["CALLS_Chart"], oi_row["PUTS_Chart"] = 0, 0

                oi_data = pd.concat(
                    [oi_data, pd.DataFrame([oi_row])], ignore_index=True
                )

        return (
            oi_data,
            float(payload["records"]["underlyingValue"]),
            payload["records"]["timestamp"],
        )

    async def get_quote(self, symbol: str, section: str = "") -> dict:
        """
        Gets quote information.

        :param symbol:
        :param section:  Optional section (e.g., for specific board lots)
        :return:
        """
        symbol = self._purify_symbol(symbol)
        if section == "":
            if symbol.upper() in await self.get_fno_symbols():
                url = f"https://www.nseindia.com/api/quote-derivative?symbol={symbol}"
            else:
                url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        else:
            url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}§ion={section}"
        return await self.fetcher._fetch(url)

    async def get_expiry_details(
        self, symbol: str, meta: str = "Futures", i: int = 0
    ) -> tuple[datetime.date, int]:
        """
        Gets expiry details (date and days to expiry) for a given symbol and instrument.

        :param symbol:
        :param meta: "Futures" or "Options"
        :param i: index of expiry date. 0 for nearest, 1 for next, etc.
        :return: (expiry_date, days_to_expiry)
        """
        payload = await self.get_quote(symbol)

        if meta == "Futures":
            selected_key = next(
                (
                    key
                    for key in payload["expiryDatesByInstrument"]
                    if "futures" in key.lower()
                ),
                None,
            )
        elif meta == "Options":
            selected_key = next(
                (
                    key
                    for key in payload["expiryDatesByInstrument"]
                    if "options" in key.lower()
                ),
                None,
            )
        else:
            raise ValueError("Invalid instrument. Must be 'Futures' or 'Options'")

        expiry_dates = self._filter_expiry_dates(
            payload["expiryDatesByInstrument"][selected_key]
        )

        if not expiry_dates:
            raise ValueError(f"No valid expiry dates found for {symbol} {meta}")

        if i >= len(expiry_dates):
            raise IndexError("Provided expiry index is out of range")

        current_expiry = datetime.datetime.strptime(expiry_dates[i], "%d-%b-%Y").date()
        dte = (current_expiry - datetime.datetime.now().date()).days
        return current_expiry, dte

    async def get_pcr(self, symbol, expiry_index: int = 0) -> float:
        """
        Calculates the Put-Call Ratio (PCR) for a given symbol and expiry.

        :param symbol: The stock or index symbol.
        :param expiry_index: The index of the expiry date to use (0 for the nearest, 1 for the next, etc.).
        :return: The PCR value.
        """

        if not await self.is_valid_symbol(symbol):
            raise ValueError(f"Invalid Symbol {symbol} provided")

        payload = await self.get_option_chain(symbol)
        expiry_date = payload["records"]["expiryDates"][expiry_index]

        ce_oi = 0
        pe_oi = 0
        for item in payload["records"]["data"]:
            if item["expiryDate"] == expiry_date:
                try:
                    ce_oi += item["CE"]["openInterest"]
                    pe_oi += item["PE"]["openInterest"]
                except KeyError:
                    pass

        if ce_oi == 0:
            return float(
                "inf"
            )  # Return infinity if CE OI is zero to avoid division by zero

        return pe_oi / ce_oi

    async def get_quote_ltp(
        self,
        symbol: str,
        expiry_date: str = "latest",
        option_type: str = "-",
        strike_price: float = 0.0,
    ) -> float:
        """
        Gets the Last Traded Price (LTP) for a given symbol, expiry, and option type.

        :param symbol:
        :param expiry_date: "latest", "next", or specific date "dd-mmm-yyyy".
        :param option_type: "-", "Fut", "PE", "CE"
        :param strike_price: Required for option_type PE or CE
        :return:
        """
        payload = await self.get_quote(symbol)

        if option_type.upper() in ("PE", "CE", "FUT"):
            instrument_type = (
                "Options" if option_type.upper() in ("PE", "CE") else "Futures"
            )
        else:
            instrument_type = None

        if expiry_date in ("latest", "next"):

            if instrument_type:
                selected_key = next(
                    (
                        key
                        for key in payload["expiryDatesByInstrument"]
                        if instrument_type.lower() in key.lower()
                    ),
                    None,
                )
                if not selected_key:
                    raise ValueError(
                        f"No {instrument_type} expiry dates found for {symbol}"
                    )
                expiry_dates = self._filter_expiry_dates(
                    payload["expiryDatesByInstrument"][selected_key]
                )
                expiry_date_index = 0 if expiry_date == "latest" else 1
                try:
                    expiry_date = expiry_dates[expiry_date_index]
                except IndexError:
                    raise ValueError(
                        f"No {expiry_date} expiry found for {symbol} {instrument_type}"
                    )
            else:
                expiry_dates = self._filter_expiry_dates(payload["expiryDates"])
                expiry_date_index = 0 if expiry_date == "latest" else 1
                try:
                    expiry_date = expiry_dates[expiry_date_index]
                except IndexError:
                    raise ValueError(f"No {expiry_date} expiry found for {symbol}")
        elif not instrument_type:
            expiry_dates = self._filter_expiry_dates(payload["expiryDates"])
            if expiry_date not in expiry_dates:
                raise ValueError(
                    "Invalid expiry date. It must be in dd-mmm-yyyy format"
                )

        if instrument_type:
            try:
                datetime.datetime.strptime(expiry_date, "%d-%b-%Y")
            except ValueError:
                raise ValueError("Invalid expiry date format.  Should be dd-mmm-yyyy.")

        if instrument_type:
            _option_type = (
                "Put"
                if option_type.upper() == "PE"
                else "Call" if option_type.upper() == "CE" else "Futures"
            )

        if option_type.upper() in ("PE", "CE", "FUT"):
            for stock_data in payload["stocks"]:
                meta = stock_data["metadata"]
                if instrument_type in meta["instrumentType"]:
                    if _option_type == "Futures" and meta["expiryDate"] == expiry_date:
                        return meta["lastPrice"]
                    elif (
                        _option_type in ("Put", "Call")
                        and meta["expiryDate"] == expiry_date
                        and meta["optionType"] == _option_type
                        and meta["strikePrice"] == strike_price
                    ):
                        return meta["lastPrice"]
            raise ValueError(
                f"No data found for {symbol} with expiry {expiry_date}, option type {_option_type}, and strike price {strike_price}."
            )
        else:
            return payload["underlyingValue"]

    async def get_quote_metadata(
        self,
        symbol: str,
        expiry_date: str,
        option_type: OptionType,
        strike_price: float,
    ) -> dict:
        """
        Gets quote metadata.

        :param symbol: The stock symbol.
        :param expiry_date: "latest", "next", or specific date "dd-mmm-yyyy".
        :param option_type: OptionType enum value
        :param strike_price: Required for option_type
        :return: The metadata dictionary.
        """

        payload = await self.get_quote(symbol)

        instrument_type = (
            "Options" if option_type.value.upper() in ("PE", "CE") else "Futures"
        )

        # Special case for indices and futures: use RELIANCE expiry dates as a proxy. Bit of a hack.
        if symbol in [ind.value for ind in Index] and option_type.upper() == "FUT":
            expiry_dates = await self.get_expiry_list("RELIANCE")
            if expiry_date == "latest":
                expiry_date = expiry_dates[0]
            elif expiry_date == "next":
                expiry_date = expiry_dates[1]

        if expiry_date in ("latest", "next"):
            selected_key = next(
                (
                    key
                    for key in payload["expiryDatesByInstrument"]
                    if instrument_type.lower() in key.lower()
                ),
                None,
            )
            if not selected_key:
                raise ValueError(
                    f"No {instrument_type} expiry dates found for {symbol}"
                )
            expiry_dates = self._filter_expiry_dates(
                payload["expiryDatesByInstrument"][selected_key]
            )
            expiry_date_index = 0 if expiry_date == "latest" else 1
            try:
                expiry_date = expiry_dates[expiry_date_index]
            except IndexError:
                raise ValueError(
                    f"No {expiry_date} expiry found for {symbol} {instrument_type}"
                )

        else:
            expiry_dates = self._filter_expiry_dates(payload["expiryDates"])
            if expiry_date not in expiry_dates:
                raise ValueError(
                    "Invalid expiry date. It must be in dd-mmm-yyyy format"
                )

        try:
            datetime.datetime.strptime(expiry_date, "%d-%b-%Y")
        except ValueError:
            raise ValueError("Invalid expiry date format. Should be dd-mmm-yyyy.")

        _option_type = (
            "Put"
            if option_type.value.upper() == "PE"
            else "Call" if option_type.value.upper() == "CE" else "Futures"
        )

        for stock_data in payload["stocks"]:
            meta = stock_data["metadata"]
            if instrument_type in meta["instrumentType"]:
                if _option_type == "Futures" and meta["expiryDate"] == expiry_date:
                    return meta
                elif (
                    _option_type in ("Put", "Call")
                    and meta["expiryDate"] == expiry_date
                    and meta["optionType"] == _option_type
                    and meta["strikePrice"] == strike_price
                ):
                    return meta

        raise ValueError(
            f"No data found for {symbol} with expiry {expiry_date}, option type {_option_type}, and strike price {strike_price}."
        )

    def get_option_chain_ltp(
        self,
        payload: dict,
        strike_price: float,
        option_type: str,
        expiry_index: int = 0,
        intent: str = "",
    ) -> float:
        """
        Gets the LTP from a pre-fetched option chain.

        :param payload: Option chain data (from get_option_chain)
        :param strike_price: The strike price.
        :param option_type: "CE" or "PE"
        :param expiry_index: 0 for nearest, 1 for next, etc.
        :param intent: "", "sell", or "buy"
        :return: The LTP value.
        """
        if not option_type.upper() in ("PE", "CE"):
            raise ValueError("Invalid option_type value. It should be PE or CE")

        expiry_dates = self._filter_expiry_dates(payload["records"]["expiryDates"])
        expiry_date = expiry_dates[expiry_index]

        for item in payload["records"]["data"]:
            if (
                item["strikePrice"] == strike_price
                and item["expiryDate"] == expiry_date
            ):
                if intent == "":
                    return item[option_type.upper()]["lastPrice"]
                elif intent == "sell":
                    return item[option_type.upper()]["bidprice"]
                elif intent == "buy":
                    return item[option_type.upper()]["askPrice"]
                else:
                    raise ValueError("Invalid intent.  Must be '', 'sell', or 'buy'.")
        raise ValueError(
            f"Option chain data not found for strike {strike_price}, expiry {expiry_date}, option type {option_type}"
        )

    async def get_equity_info(self, symbol: str) -> dict:
        """
        Fetches equity information, automatically handling potential F&O symbols.

        :param symbol: The stock symbol.
        :return: The equity information dictionary.
        """
        symbol = self._purify_symbol(symbol)
        try:
            payload = await self.fetcher._fetch(
                f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            )
            # Check for error, and if present, try the derivative endpoint.
            if "error" in payload and payload["error"] == {}:
                logging.warning("Equity endpoint failed, trying F&O endpoint.")
                payload = await self.fetcher._fetch(
                    f"https://www.nseindia.com/api/quote-derivative?symbol={symbol}"
                )
        except KeyError:
            logging.error("Error fetching data. Check symbol and API status.")
            raise
        return payload

    async def get_derivative_info(self, symbol: str) -> dict:
        """
        Fetches derivative information, automatically handling potential equity symbols.

        :param symbol: The derivative symbol.
        :return: The derivative information dictionary.
        """

        if not await self.is_valid_symbol(symbol):
            raise ValueError(f"Invalid Symbol {symbol} provided")

        symbol = self._purify_symbol(symbol)
        try:
            payload = await self.fetcher._fetch(
                f"https://www.nseindia.com/api/quote-derivative?symbol={symbol}"
            )
            # Check for error, and if present, try the equity endpoint.
            if "error" in payload and payload["error"] == {}:
                logging.warning("Derivative endpoint failed, trying equity endpoint.")
                payload = await self.fetcher._fetch(
                    f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
                )
        except KeyError:
            logging.error("Error fetching data. Check symbol and API status.")
            raise
        return payload

    async def get_holidays(self, holiday_type: HolidayType = HolidayType.TRADING) -> dict:
        """Gets the trading or clearing holidays."""
        return await self.fetcher._fetch(
            f"https://www.nseindia.com/api/holiday-master?type={holiday_type.value}"
        )

    async def get_corporate_results(
        self, index: str = "equities", period: ResultPeriod = ResultPeriod.QUARTERLY
    ) -> pd.DataFrame:
        """
        Gets corporate financial results.

        :param index: "equities", "debt", or "sme"
        :param period:
        :return:
        """
        if index not in ["equities", "debt", "sme"]:
            raise ValueError("Invalid index. Must be 'equities', 'debt', or 'sme'.")
        if period.value not in [rp.value for rp in ResultPeriod]:
            raise ValueError(
                "Invalid period. Must be 'Quarterly', 'Annual', 'Half-Yearly' or 'Others'."
            )

        url = f"https://www.nseindia.com/api/corporates-financial-results?index={index}&period={period.value}"
        return pd.json_normalize(await self.fetcher._fetch(url))

    async def get_events(self) -> pd.DataFrame:
        """Gets the corporate events calendar."""
        return pd.json_normalize(
            await self.fetcher._fetch("https://www.nseindia.com/api/event-calendar")
        )

    async def get_past_results(self, symbol: str) -> dict:
        """Gets past corporate results for a symbol."""
        symbol = self._purify_symbol(symbol)
        return await self.fetcher._fetch(
            f"https://www.nseindia.com/api/results-comparision?symbol={symbol}"
        )

    async def get_expiry_list(
        self, symbol: str, type: str = "list"
    ) -> Union[List[str], pd.DataFrame]:
        """
        Gets the list of expiry dates for a symbol.

        :param symbol:
        :param type: "list" or "pandas"
        :return:
        """
        logging.info(f"Getting Expiry List of: {symbol}")

        if type not in ["list", "pandas"]:
            raise ValueError("Invalid type.  Must be 'list' or 'pandas'.")

        if type == "list":
            payload = await self.get_quote(symbol)
            expiry_dates = sorted(
                list(set(payload["expiryDates"])),
                key=lambda date: datetime.datetime.strptime(date, "%d-%b-%Y"),
            )
            return expiry_dates
        else:
            payload = await self.get_option_chain(symbol)
            return pd.DataFrame({"Date": payload["records"]["expiryDates"]})

    async def get_custom_fno_data(self, symbol: str, attribute: str = "lastPrice") -> any:
        """
        Gets custom data from the F&O securities list.

        :param symbol:
        :param attribute:
        :return:
        """
        if not await self.is_valid_symbol(symbol):
            raise ValueError(f"Invalid Symbol {symbol} provided")

        positions = await self.fetcher._fetch(
            "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
        )
        for item in positions["data"]:
            if item["symbol"] == symbol.upper():
                return item[attribute]
        raise ValueError(f"Symbol {symbol} not found in F&O list.")

    async def get_block_deals(self) -> dict:
        """Gets the block deals data."""
        return await self.fetcher._fetch("https://nseindia.com/api/block-deal")

    async def get_market_status(self) -> dict:
        """Gets the market status."""
        return await self.fetcher._fetch("https://nseindia.com/api/marketStatus")

    async def get_circulars(self, mode: str = "latest") -> dict:
        """Gets the latest or all circulars."""
        if mode == "latest":
            url = "https://nseindia.com/api/latest-circular"
        else:
            url = "https://www.nseindia.com/api/circulars"
        return await self.fetcher._fetch(url)

    async def get_fii_dii_data(self, mode: str = "pandas") -> Union[pd.DataFrame, dict]:
        """Gets FII/DII trading activity data."""
        try:
            data = await self.fetcher._fetch("https://www.nseindia.com/api/fiidiiTradeReact")
            return pd.DataFrame(data) if mode == "pandas" else data
        except:
            logging.info("Pandas is not working for some reason.")
            return await self.fetcher._fetch("https://www.nseindia.com/api/fiidiiTradeReact")

    async def get_nsetools_quote(self, symbol: str) -> dict:
        """
        Mimics the nsetools get_quote function (for compatibility).
        It gives details of FNO listed Securities

        :param symbol:
        :return:
        """

        if not await self.is_valid_symbol(symbol):
            raise ValueError(f"Invalid Symbol {symbol} provided")

        payload = await self.fetcher._fetch(
            "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
        )
        for item in payload["data"]:
            if item["symbol"] == symbol.upper():
                return item
        raise ValueError(f"Symbol {symbol} not found")

    @cache
    async def get_index_list(self) -> List[str]:
        """Gets the list of indices."""
        payload = await self.fetcher._fetch(
            "https://www.nseindia.com/api/allIndices"
        )
        return [item["index"] for item in payload["data"]]

    async def get_index_quote(self, index: str) -> dict:
        """
        Gets the quote for a specific index.

        :param index:
        :return:
        """
        payload = await self.fetcher._fetch(
            "https://www.nseindia.com/api/allIndices"
        )
        for item in payload["data"]:
            if item["index"] == index.upper():
                return item
        raise ValueError(f"Index {index} not found.")

    async def get_advances_declines(self, mode: str = "pandas") -> Union[pd.DataFrame, dict]:
        """Gets advances and declines data."""
        try:
            data = await self.fetcher._fetch(
                "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
            )
            return pd.DataFrame(data["data"]) if mode == "pandas" else data
        except:
            logging.info("Pandas is not working for some reason.")
            return await self.fetcher._fetch(
                "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
            )

    async def get_top_losers(self) -> pd.DataFrame:
        """Gets the top 5 losers."""
        positions = await self.fetcher._fetch(
            "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
        )
        df = pd.DataFrame(positions["data"])
        return df.sort_values(by="pChange").head(5)

    async def get_top_gainers(self) -> pd.DataFrame:
        """Gets the top 5 gainers."""
        positions = await self.fetcher._fetch(
            "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
        )
        df = pd.DataFrame(positions["data"])
        return df.sort_values(by="pChange", ascending=False).head(5)

    # def get_fno_lot_sizes(
    #     self, symbol: str = "all", mode: str = "list"
    # ) -> Union[Dict[str, int], pd.DataFrame, int, None]:
    #     """
    #     Gets F&O lot sizes.

    #     :param symbol: "all" or a specific symbol.
    #     :param mode: "list" or "pandas".
    #     :return:
    #     """
    #     url = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"
    #     breakpoint()
    #     if mode == "list":
    #         # response_text = requests.get(url).text
    #         response = self.fetcher._fetch_pdf(url)
    #         with open("fo_mktlots.pdf", "wb") as f:
    #             f.write(response)
    #         res_dict = {}
    #         for line in response_text.split("\n"):
    #             if line and "," in line and "symbol" not in line.lower():
    #                 code, name = [x.strip() for x in line.split(",")[1:3]]
    #                 res_dict[code] = int(name)
    #         if symbol == "all":
    #             return res_dict
    #         elif symbol.upper() in res_dict:
    #             return res_dict[symbol.upper()]
    #         else:
    #             return None  # Return None if the symbol is not found.

    #     elif mode == "pandas":
    #         payload = pd.read_csv(url)
    #         if symbol == "all":
    #             return payload
    #         else:
    #             filtered_payload = payload[payload.iloc[:, 1] == symbol.upper()]
    #             return (
    #                 filtered_payload if not filtered_payload.empty else None
    #             )  # Return None if not found

    #     else:
    #         raise ValueError("Invalid mode.  Must be 'list' or 'pandas'.")

    async def get_india_vix(self) -> float:
        """Returns the current value of India VIX."""
        payload = await self.fetcher._fetch("https://www.nseindia.com/api/allIndices")
        for index_data in payload["data"]:
            if index_data["index"] == "INDIA VIX":
                return index_data["last"]
        raise ValueError("INDIA VIX not found in the response.")

    async def get_index_info(self, index: str) -> dict:
        """Returns information about a given index."""
        payload = await self.fetcher._fetch("https://www.nseindia.com/api/allIndices")
        for index_data in payload["data"]:
            if index_data["index"] == index:
                return index_data
        raise ValueError(f"Index '{index}' not found in the response.")

    async def calculate_black_scholes(
        self,
        S0: float,
        X: float,
        t: float,
        sigma: float = None,
        r: float = 0.10,
        q: float = 0.0,
        td: int = 365,
    ) -> tuple:
        """
        Calculates option prices and Greeks using the Black-Scholes model.

        :param S0: Current price of the underlying asset.
        :param X: Strike price of the option.
        :param t: Time to expiration in days.
        :param sigma: Volatility of the underlying asset. If None, uses India VIX.
        :param r: Risk-free interest rate (annualized). Default is 10% pa.
        :param q: Continuous dividend yield (annualized).
        :param td: Number of trading days in a year.
        :return: (call_theta, put_theta, call_premium, put_premium, call_delta, put_delta, gamma, vega, call_rho, put_rho)
        """

        if sigma is None:
            sigma = await self.get_india_vix()

        S0, X, sigma, r, q, t = (
            float(S0),
            float(X),
            float(sigma / 100),
            float(r),
            float(q / 100),
            float(t / td),
        )

        d1 = (math.log(S0 / X) + (r - q + 0.5 * sigma**2) * t) / (sigma * math.sqrt(t))
        Nd1 = (math.exp((-(d1**2)) / 2)) / math.sqrt(2 * math.pi)
        d2 = d1 - sigma * math.sqrt(t)
        Nd2 = norm.cdf(d2)

        call_theta = (
            -(
                (S0 * sigma * math.exp(-q * t))
                / (2 * math.sqrt(t))
                * (1 / (math.sqrt(2 * math.pi)))
                * math.exp(-(d1 * d1) / 2)
            )
            - (r * X * math.exp(-r * t) * norm.cdf(d2))
            + (q * math.exp(-q * t) * S0 * norm.cdf(d1))
        ) / td
        put_theta = (
            -(
                (S0 * sigma * math.exp(-q * t))
                / (2 * math.sqrt(t))
                * (1 / (math.sqrt(2 * math.pi)))
                * math.exp(-(d1 * d1) / 2)
            )
            + (r * X * math.exp(-r * t) * norm.cdf(-d2))
            - (q * math.exp(-q * t) * S0 * norm.cdf(-d1))
        ) / td
        call_premium = math.exp(-q * t) * S0 * norm.cdf(d1) - X * math.exp(
            -r * t
        ) * norm.cdf(d1 - sigma * math.sqrt(t))
        put_premium = X * math.exp(-r * t) * norm.cdf(-d2) - math.exp(
            -q * t
        ) * S0 * norm.cdf(-d1)
        call_delta = math.exp(-q * t) * norm.cdf(d1)
        put_delta = math.exp(-q * t) * (norm.cdf(d1) - 1)
        gamma = (
            (math.exp(-r * t) / (S0 * sigma * math.sqrt(t)))
            * (1 / (math.sqrt(2 * math.pi)))
            * math.exp(-(d1 * d1) / 2)
        )
        vega = ((1 / 100) * S0 * math.exp(-r * t) * math.sqrt(t)) * (
            1 / (math.sqrt(2 * math.pi)) * math.exp(-(d1 * d1) / 2)
        )
        call_rho = (1 / 100) * X * t * math.exp(-r * t) * norm.cdf(d2)
        put_rho = (-1 / 100) * X * t * math.exp(-r * t) * norm.cdf(-d2)

        return (
            call_theta,
            put_theta,
            call_premium,
            put_premium,
            call_delta,
            put_delta,
            gamma,
            vega,
            call_rho,
            put_rho,
        )

    async def _fetch_historical_data(self, url: str, chunk_size: int = 40) -> pd.DataFrame:
        """
        Helper function to fetch historical data with pagination.
        Handles fetching large datasets by breaking them into smaller chunks.
        """
        start_date_str, end_date_str = self._extract_start_end_dates(url)
        start_date = datetime.datetime.strptime(start_date_str, "%d-%m-%Y")
        end_date = datetime.datetime.strptime(end_date_str, "%d-%m-%Y")

        all_data = pd.DataFrame()
        current_start = start_date

        while current_start <= end_date:
            current_end = min(
                current_start + datetime.timedelta(days=chunk_size - 1), end_date
            )
            current_start_str = current_start.strftime("%d-%m-%Y")
            current_end_str = current_end.strftime("%d-%m-%Y")

            chunk_url = url.replace(start_date_str, current_start_str).replace(
                end_date_str, current_end_str
            )
            logging.info(f"Fetching data from {current_start_str} to {current_end_str}")

            try:
                chunk_data = await self.fetcher._fetch(chunk_url)
                if "data" not in chunk_data:
                    raise ValueError(
                        f"API returned no data or unexpected format. Response: {chunk_data}"
                    )

                df_chunk = pd.DataFrame.from_records(chunk_data["data"])
                all_data = pd.concat([all_data, df_chunk], ignore_index=True)
            except Exception as e:
                logging.error(f"Failed to fetch historical data chunk: {e}")

            current_start = current_end + datetime.timedelta(days=1)

        return all_data.iloc[::-1].reset_index(drop=True)  # Reverse and reset index

    def _extract_start_end_dates(self, url: str) -> tuple[str, str]:
        """Extracts start and end dates from the URL."""
        from_date_param = urllib.parse.unquote(url).split("from=")[1].split("&")[0]
        to_date_param = urllib.parse.unquote(url).split("to=")[1].split("&")[0]
        return from_date_param, to_date_param
    
    def _format_date(self, date: str, format: str = "%d-%b-%Y") -> str:
        """Formats the date to the specified format."""
        return datetime.datetime.strptime(date, "%d-%m-%Y").strftime(format)

    async def _get_past_trading_date(self, days: int) -> str:
        """
        Calculates a past trading date, considering weekends and holidays.
        """
        end_date = datetime.datetime.now()
        past_date = end_date - datetime.timedelta(days=days)

        trading_holidays = await self.get_holidays(HolidayType.TRADING)
        holidays = trading_holidays['FO']
        holiday_dates = [datetime.datetime.strptime(h['tradingDate'], '%d-%b-%Y').date() for h in holidays]

        while days > 0:
            past_date -= datetime.timedelta(days=1)
            if past_date.weekday() >= 5 or past_date.date() in holiday_dates:
                continue
            days -= 1

        return past_date.strftime("%d-%m-%Y")

    async def get_equity_history(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Gets historical equity data.

        :param symbol: The stock symbol.
        :param start_date: "dd-mm-yyyy"
        :param end_date: "dd-mm-yyyy"
        :return:
        """
        if not await self.is_valid_symbol(symbol):
            raise ValueError(f"Invalid Symbol {symbol} provided")

        url = f'https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}&series=["EQ"]&from={start_date}&to={end_date}'
        return await self._fetch_historical_data(url)

    async def get_derivative_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        instrument_type: InstrumentType,
        expiry_date: str,
        strike_price: float = None,
        option_type: OptionType = None,
    ) -> pd.DataFrame:
        """
        Gets historical derivative data.

        :param symbol:
        :param start_date: "dd-mm-yyyy"
        :param end_date: "dd-mm-yyyy"
        :param instrument_type:
        :param expiry_date: "dd-mm-yyyy"
        :param strike_price:
        :param option_type:
        :return:
        """

        if not await self.is_valid_symbol(symbol):
            raise ValueError(f"Invalid Symbol {symbol} provided")

        instrument_str = instrument_type.value
        if "NIFTY" in symbol:
            instrument_str = instrument_str.replace("STK", "IDX")

        if (
            instrument_type
            in (InstrumentType.OPTION_INDEX, InstrumentType.OPTION_STOCK)
            and expiry_date == ""
        ):
            raise ValueError("Expiry date is required for options.")

        strike_price_str = ""
        if strike_price is not None:
            strike_price_str = f"&strikePrice={strike_price:.2f}"

        option_type_str = ""
        if option_type is not None:
            option_type_str = f"&optionType={option_type.value}"

        url = (
            f"https://www.nseindia.com/api/historical/fo/derivatives?&from={start_date}&to={end_date}"
            f"{option_type_str}{strike_price_str}&expiryDate={expiry_date}&instrumentType={instrument_str}&symbol={symbol}"
        )
        return await self._fetch_historical_data(url)

    async def get_expiry_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        instrument_type: str = "options",
    ) -> List[str]:
        """
        Gets historical expiry dates for a given symbol within a date range.

        :param symbol: The stock or index symbol.
        :param start_date: Start date in 'dd-mm-yyyy' format.
        :param end_date: End date in 'dd-mm-yyyy' format.
        :param instrument_type: "options" (default) or "futures"
        :return: A list of expiry dates in 'dd-mmm-yyyy' format.
        """
        if not await self.is_valid_symbol(symbol):
            raise ValueError(f"Invalid Symbol {symbol} provided")

        if instrument_type.lower() not in ("options", "futures"):
            raise ValueError("Instrument must be 'options' or 'futures'")

        url = f"https://www.nseindia.com/api/historical/fo/derivatives/meta?&from={start_date}&to={end_date}&symbol={symbol}"
        payload = await self.fetcher._fetch(url)

        try:
            datetime.datetime.strptime(start_date, "%d-%m-%Y")
            datetime.datetime.strptime(end_date, "%d-%m-%Y")
        except ValueError:
            raise ValueError("Invalid date format.  Should be dd-mm-yyyy.")

        for key, _ in payload["expiryDatesByInstrument"].items():
            if instrument_type.lower() == "options" and "OPT" in key:
                payload_data = payload["expiryDatesByInstrument"][key]
                break
            elif instrument_type.lower() == "futures" and "FUT" in key:
                payload_data = payload["expiryDatesByInstrument"][key]
                break
        else:  # No break, meaning no matching key was found
            raise ValueError(
                f"No {instrument_type} expiry dates found in API response for {symbol}"
            )

        start_date_obj = datetime.datetime.strptime(start_date, "%d-%m-%Y").date()
        end_date_obj = datetime.datetime.strptime(end_date, "%d-%m-%Y").date()

        filtered_dates = []
        added_after_end_date = (
            False  # Flag to track if one date after end_date is added
        )

        for date_str in payload_data:
            date_obj = datetime.datetime.strptime(date_str, "%d-%b-%Y").date()
            if start_date_obj <= date_obj <= end_date_obj:
                filtered_dates.append(date_str)
            elif date_obj > end_date_obj and not added_after_end_date:
                filtered_dates.append(date_str)
                added_after_end_date = True

        return filtered_dates

    async def get_index_history(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Gets historical index data from niftyindices.com."""

        start_date = self._format_date(start_date)
        end_date = self._format_date(end_date)
        print(start_date, end_date)
        data = {
            "cinfo": f"{{'name':'{symbol}','startDate':'{start_date}','endDate':'{end_date}','indexName':'{symbol}'}}"
        }
        return await self.fetcher.fetch_niftyindices(
            "https://niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString",
            data,
        )

    async def get_index_pe_pb_div(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Gets historical PE, PB, and dividend yield data for an index from niftyindices.com."""

        start_date = self._format_date(start_date)
        end_date = self._format_date(end_date)
        data = {
            "cinfo": f"{{'name':'{symbol}','startDate':'{start_date}','endDate':'{end_date}','indexName':'{symbol}'}}"
        }
        return await self.fetcher.fetch_niftyindices(
            "https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString",
            data,
        )

    async def get_index_total_returns(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Gets historical total returns index data from niftyindices.com."""

        start_date = self._format_date(start_date)
        end_date = self._format_date(end_date)
        data = {
            "cinfo": f"{{'name':'{symbol}','startDate':'{start_date}','endDate':'{end_date}','indexName':'{symbol}'}}"
        }
        return await self.fetcher.fetch_niftyindices(
            "https://niftyindices.com/Backpage.aspx/getTotalReturnIndexString", data
        )

    def get_bhavcopy(self, date: str) -> pd.DataFrame:
        """
        Downloads and reads the Bhavcopy CSV file for a given date.

        :param date: Date in 'dd-mm-yyyy' format.
        :return: Pandas DataFrame containing the Bhavcopy data.
        """
        try:
            date_formatted = date.replace("-", "")
            url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_formatted}.csv"
            return pd.read_csv(url)
        except Exception as e:
            logging.error(f"Error downloading or reading Bhavcopy for {date}: {e}")
            raise

    def get_bulk_deals_data(self) -> pd.DataFrame:
        """
        Downloads and reads the bulk deals data

        :return: Pandas DataFrame containing the bulk deals data.
        """
        try:
            url = "https://archives.nseindia.com/content/equities/bulk.csv"
            return pd.read_csv(url)
        except Exception as e:
            logging.error(f"Error downloading or reading bulk deals data: {e}")
            raise

    def get_block_deals_data(self) -> pd.DataFrame:
        """
        Downloads and reads the block deals data.

        :return: Pandas DataFrame containing the block deals data.
        """
        try:
            url = "https://archives.nseindia.com/content/equities/block.csv"
            return pd.read_csv(url)
        except Exception as e:
            logging.error(f"Error downloading or reading block deals data: {e}")
            raise

    async def calculate_beta(
        self, symbol: str, days: int = 365, symbol2: str = "NIFTY 50"
    ) -> float:
        """
        Calculates the beta of a stock or index relative to another symbol (default: NIFTY 50).

        :param symbol:
        :param days:
        :param symbol2:
        :return:
        """

        async def _get_beta_df(symbol: str, days: int) -> pd.DataFrame:
            if "NIFTY" in symbol:
                end_date = datetime.datetime.now().strftime("%d-%m-%Y")
                start_date = (
                    datetime.datetime.now() - datetime.timedelta(days=days)
                ).strftime("%d-%m-%Y")
                df = await self.get_index_history(symbol, start_date, end_date)
                df["daily_change"] = df["CLOSE"].astype(float).pct_change()
                return df[["HistoricalDate", "daily_change"]].iloc[1:]

            else:
                if not await self.is_valid_symbol(symbol, fno=False):
                    raise ValueError(f"Invalid symbol {symbol} provided")
                end_date = datetime.datetime.now().strftime("%d-%m-%Y")
                start_date = (
                    datetime.datetime.now() - datetime.timedelta(days=days)
                ).strftime("%d-%m-%Y")
                df = await self.get_equity_history(symbol, start_date, end_date)
                df["daily_change"] = df["CH_CLOSING_PRICE"].pct_change()
                return df[["CH_TIMESTAMP", "daily_change"]].iloc[1:]

        df1 = await _get_beta_df(symbol, days)
        df2 = await _get_beta_df(symbol2, days)

        # Ensure both DataFrames have the same number of rows
        if len(df1) != len(df2):
            logging.warning(
                f"DataFrames for {symbol} and {symbol2} have different lengths. Using inner merge."
            )
            # Use merge with 'inner' to align the dates
            if "NIFTY" in symbol:
                merged_df = pd.merge(
                    df1,
                    df2,
                    left_on="HistoricalDate",
                    right_on="HistoricalDate",
                    how="inner",
                    suffixes=("_x", "_y"),
                )
            else:
                merged_df = pd.merge(
                    df1,
                    df2,
                    left_on="CH_TIMESTAMP",
                    right_on="HistoricalDate" if "NIFTY" in symbol2 else "CH_TIMESTAMP",
                    how="inner",
                    suffixes=("_x", "_y"),
                )
            x = merged_df["daily_change_x"].tolist()
            y = merged_df["daily_change_y"].tolist()
        else:
            x = df1["daily_change"].tolist()
            y = df2["daily_change"].tolist()

        mean_x = sum(x) / len(x)
        mean_y = sum(y) / len(y)
        covariance = sum((a - mean_x) * (b - mean_y) for (a, b) in zip(x, y)) / len(x)
        variance = sum((i - mean_y) ** 2 for i in y) / len(y)

        if variance == 0:
            return float(
                "inf"
            )  # Return infinity if variance is zero to avoid division by zero.

        beta = covariance / variance
        return round(beta, 3)

    async def get_preopen_data(
        self, key: PreopenKey = PreopenKey.NIFTY, data_type: str = "pandas"
    ) -> Union[pd.DataFrame, dict]:
        """
        Gets pre-open market data.

        :param key: "NIFTY" (default) or "FO".
        :param type: "pandas" (default) or "dict".
        :return: DataFrame or dictionary with pre-open data.
        """
        payload = await self.fetcher._fetch(
            f"https://www.nseindia.com/api/market-data-pre-open?key={key.value}"
        )
        if data_type == "pandas":
            df = pd.DataFrame(payload["data"])
            return pd.json_normalize(df["metadata"])
        elif data_type == "dict":
            return payload
        else:
            raise ValueError("Invalid type. Must be 'pandas' or 'dict'.")

    async def get_preopen_movers(
        self, key: PreopenKey = PreopenKey.FNO, price_filter: float = 1.5
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Gets pre-open market movers (gainers and losers).

        :param key: The pre-open key, either "FO" (default) for F&O or "NIFTY" for Nifty.
        :param filter: The minimum absolute percentage change to consider a stock a mover (default: 1.5).
        :return: A tuple containing two DataFrames: (gainers, losers).
        """
        preopen_data = await self.get_preopen_data(key, data_type="pandas")
        gainers = preopen_data[preopen_data["pChange"] > price_filter]
        losers = preopen_data[preopen_data["pChange"] < -price_filter]
        return gainers, losers

    async def get_most_active(
        self, asset_type: str = "securities", sort_by: SortType = SortType.VALUE
    ) -> pd.DataFrame:
        """
        Gets most active securities or ETFs.

        :param type: "securities", "etf" or "sme".
        :param sort: "volume" or "value".
        :return:
        """
        if asset_type not in ("securities", "etf", "sme"):
            raise ValueError("Type must be 'securities', 'etf' or 'sme'")

        data = await self.fetcher._fetch(
            f"https://www.nseindia.com/api/live-analysis-most-active-{asset_type}?index={sort_by.value}"
        )
        return pd.DataFrame(data["data"])

    async def get_price_band_hitters(
        self, band_type: BandType = BandType.BOTH, view: BandView = BandView.ALL
    ) -> pd.DataFrame:
        """
        Retrieves data on securities hitting price bands.

        :param band_type: 'upper', 'lower', or 'both' (default).
        :param view: 'AllSec' (default), 'SecGtr20', or 'SecLwr20'.
        :return: DataFrame containing the data.
        """
        payload = await self.fetcher._fetch(
            "https://www.nseindia.com/api/live-analysis-price-band-hitter"
        )
        return pd.DataFrame(payload[band_type.value][view.value]["data"])

    async def get_large_deals(
        self, deal_type: LargeDealType = LargeDealType.BULK
    ) -> pd.DataFrame:
        """
        Retrieves data on large deals (bulk, short, or block).

        :param mode: 'bulk_deals' (default), 'short_deals', or 'block_deals'.
        :return: DataFrame containing the data.
        """
        payload = await self.fetcher._fetch(
            "https://www.nseindia.com/api/snapshot-capital-market-largedeal"
        )
        return pd.DataFrame(payload[f"{deal_type.value.upper()}_DATA"])

    async def get_large_deals_historical(
        self,
        from_date: str,
        to_date: str,
        deal_type: LargeDealType = LargeDealType.BULK,
    ) -> pd.DataFrame:
        """
        Retrieves historical large deals data (bulk, short, or block).

        :param from_date: Start date in 'dd-mm-yyyy' format.
        :param to_date: End date in 'dd-mm-yyyy' format.
        :param mode: 'bulk_deals' (default), 'short_deals', or 'block_deals'.
        :return: DataFrame containing the historical data.
        """

        try:
            datetime.datetime.strptime(from_date, "%d-%m-%Y")
            datetime.datetime.strptime(to_date, "%d-%m-%Y")
        except ValueError:
            raise ValueError("Invalid date format.  Should be dd-mm-yyyy.")

        mode_str = (
            deal_type.value.replace("_", "-")
            if deal_type.value != "short_deals"
            else "short-selling"
        )
        url = f"https://www.nseindia.com/api/historical/{mode_str}?from={from_date}&to={to_date}"
        data = await self.fetcher._fetch(url)
        return pd.DataFrame(data["data"])

    def get_fao_participant_oi(self, date: str) -> pd.DataFrame:
        """
        Fetches Participant Wise Open Interest data for a given date from the NSE archives.

        :param date: Date in 'dd-mm-yyyy' format.
        :return: DataFrame containing the participant-wise OI data.  Returns an empty
                DataFrame if data is not found or if there's an error.
        """
        try:
            date_formatted = date.replace("-", "")
            url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date_formatted}.csv"
            return pd.read_csv(url)
        except Exception as e:
            logging.error(
                f"Error fetching or processing FAO participant OI data for {date}: {e}"
            )
            return pd.DataFrame()  # Return an empty DataFrame on error

    async def is_market_open_today(self, segment: MarketSegment = MarketSegment.FO) -> bool:
        """
        Checks if the market is open today for a given segment.

        :param segment: Market segment to check ('FO', 'COM' etc - refer MarketSegment Enum).
        :return: True if the market is open, False otherwise.
        """
        holiday_json = await self.get_holidays()
        holidays = holiday_json[segment.value]
        today_date = datetime.date.today().strftime("%d-%b-%Y")

        if datetime.datetime.today().weekday() in [5, 6]:
            logging.info("Market is closed today because it's a weekend.")
            return False

        for holiday in holidays:
            if holiday["tradingDate"] == today_date:
                logging.info(
                    f"Market is closed today because of {holiday['description']}"
                )
                return False

        logging.info(f"{segment.value} Market is open today. Have a Nice Trade!")
        return True  # Return True if no holiday matches today's date

    async def get_security_wise_archive(
        self, from_date: str, to_date: str, symbol: str, series: str = "ALL"
    ) -> pd.DataFrame:
        """
        Fetches security-wise archive data (price, volume, deliverable) for a given symbol and series.

        :param from_date: Start date in 'dd-mm-yyyy' format.
        :param to_date: End date in 'dd-mm-yyyy' format.
        :param symbol: The stock symbol.
        :param series: The series (e.g., "EQ", "ALL"). Default is "ALL".
        :return: DataFrame containing the archive data.
        """
        try:
            datetime.datetime.strptime(from_date, "%d-%m-%Y")
            datetime.datetime.strptime(to_date, "%d-%m-%Y")
        except ValueError:
            raise ValueError("Invalid date format.  Should be dd-mm-yyyy.")

        base_url = "https://www.nseindia.com/api/historical/securityArchives"
        url = f"{base_url}?from={from_date}&to={to_date}&symbol={symbol.upper()}&dataType=priceVolumeDeliverable&series={series.upper()}"
        data = await self.fetcher._fetch(url)
        return pd.DataFrame(data["data"])

    async def get_simple_moving_average_absolute(self, symbol: str, start_date: str, end_date: str) -> float:
        """
        Calculates the Moving Average (MA) for a given stock symbol between the specified dates.

        :param symbol: The stock symbol.
        :param start_date: Start date in 'dd-mm-yyyy' format.
        :param end_date: End date in 'dd-mm-yyyy' format.
        :return: The Moving Average (MA) value.
        """
        try:
            data = await self.get_equity_history(symbol, start_date, end_date)
            return round(data["CH_CLOSING_PRICE"].rolling(window=len(data)).mean().iloc[-1], 4)
        except Exception as e:
            logging.error(f"Error calculating Moving Average Absolute: {e}")
            return 0.0

    async def get_simple_moving_average(self, symbol: str, days: int = 50) -> float:
        """
        Calculates the Moving Average (MA) for a given stock symbol for the specified number of days,
        considering weekends and holidays.

        :param symbol: The stock symbol.
        :param days: Number of days to consider for the moving average. Default is 50.
        :return: The Moving Average (MA) value.
        """
        try:
            start_date = await self._get_past_trading_date(days)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)
            return round(data["CH_CLOSING_PRICE"].rolling(window=days).mean().iloc[-1], 4)
        except Exception as e:
            logging.error(f"Error calculating Moving Average Relative: {e}")
            return 0.0
        
    async def get_exponential_moving_average(self, symbol: str, days: int = 50) -> float:
        """
        Calculates the Exponential Moving Average (EMA) for a given stock symbol for the specified number of days,
        considering weekends and holidays.

        :param symbol: The stock symbol.
        :param days: Number of days to consider for the moving average. Default is 50.
        :return: The Exponential Moving Average (EMA) value.
        """
        try:
            start_date = await self._get_past_trading_date(days)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            return round(data["CH_CLOSING_PRICE"].ewm(span=days, adjust=False).mean().iloc[-1], 4)
        except Exception as e:
            logging.error(f"Error calculating Exponential Moving Average: {e}")
            return 0.0
        
    async def get_double_exponential_moving_average(self, symbol: str, days: int = 50) -> float:
        """
        Calculates the Double Exponential Moving Average (DEMA) for a given stock symbol for the specified number of days,
        considering weekends and holidays.

        :param symbol: The stock symbol.
        :param days: Number of days to consider for the moving average. Default is 50.
        :return: The Double Exponential Moving Average (DEMA) value.
        """
        try:
            start_date = await self._get_past_trading_date(days)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            ema = data["CH_CLOSING_PRICE"].ewm(span=days, adjust=False).mean()
            dema = 2 * ema - ema.ewm(span=days, adjust=False).mean()
            return round(dema.iloc[-1], 4)
        except Exception as e:
            logging.error(f"Error calculating Double Exponential Moving Average: {e}")
            return 0.0
        
    async def get_triple_exponential_moving_average(self, symbol: str, days: int = 50) -> float:
        """
        Calculates the Triple Exponential Moving Average (TEMA) for a given stock symbol for the specified number of days,
        considering weekends and holidays.

        :param symbol: The stock symbol.
        :param days: Number of days to consider for the moving average. Default is 50.
        :return: The Triple Exponential Moving Average (TEMA) value.
        """
        try:
            start_date = await self._get_past_trading_date(days)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            ema = data["CH_CLOSING_PRICE"].ewm(span=days, adjust=False).mean()
            ema2 = ema.ewm(span=days, adjust=False).mean()
            ema3 = ema2.ewm(span=days, adjust=False).mean()
            tema = 3 * (ema - ema2) + ema3
            return round(tema.iloc[-1], 4)
        except Exception as e:
            logging.error(f"Error calculating Triple Exponential Moving Average: {e}")
            return 0.0
        
    async def get_relative_strength_index(self, symbol: str, days: int = 14, wilder_smoothing: bool = False) -> float:
        """
        Calculates the Relative Strength Index (RSI) for a given stock symbol for the specified number of days,
        considering weekends and holidays.

        :param symbol: The stock symbol.
        :param days: Number of days to consider for the RSI. Default is 14.
        :param wilder_smoothing: Use Wilder's smoothing method. Default is False.
        :return: The Relative Strength Index (RSI) value.
        """
        try:
            start_date = await self._get_past_trading_date(days)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)
            delta = data["CH_CLOSING_PRICE"].diff()
            
            gains = delta.copy()
            losses = delta.copy()
            gains[gains < 0] = 0
            losses[losses > 0] = 0
            losses = abs(losses)
            
            avg_gain = gains.rolling(window=days).mean()
            avg_loss = losses.rolling(window=days).mean()

            if wilder_smoothing:
                for i in range(days, len(gains)):
                    avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (days-1) + gains.iloc[i]) / days
                    avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (days-1) + losses.iloc[i]) / days
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return round(rsi.iloc[-1], 4)
        except Exception as e:
            logging.error(f"Error calculating Relative Strength Index: {e}")
            return 0.0

    async def get_moving_average_convergence_divergence(self, symbol: str, get_signal: bool = False) -> Union[pd.Series, tuple[pd.Series, pd.Series]]:
        """
        Calculates the Moving Average Convergence Divergence (MACD) for a given stock symbol.
        Uses standard periods: 12 for fast EMA, 26 for slow EMA, and 9 for signal line.

        :param symbol: The stock symbol.
        :param get_signal: If True, returns both MACD and signal line Series.
        :return: MACD if get_signal is False, tuple of (MACD, signal) if True.
        """
        try:
            start_date = await self._get_past_trading_date(days=50)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)
            
            exp12 = data["CH_CLOSING_PRICE"].ewm(span=12, adjust=False).mean()
            exp26 = data["CH_CLOSING_PRICE"].ewm(span=26, adjust=False).mean()
            macd_line = exp12 - exp26
            
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            
            if get_signal:
                return macd_line, signal_line
            return macd_line
            
        except Exception as e:
            logging.error(f"Error calculating Moving Average Convergence Divergence: {e}")
            return (0.0, 0.0) if get_signal else 0.0

    async def get_stochastic_oscillator(self, symbol: str) -> float:
        """
        Calculates the Stochastic Oscillator for a given stock symbol.
        Uses standard period of 14 days.

        :param symbol: The stock symbol.
        :return: The Stochastic Oscillator value.
        """
        try:
            start_date = await self._get_past_trading_date(days=14)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            low = data["CH_TRADE_LOW_PRICE"].rolling(window=14).min()
            high = data["CH_TRADE_HIGH_PRICE"].rolling(window=14).max()

            k = 100 * (data["CH_CLOSING_PRICE"] - low) / (high - low)

            return round(k.iloc[-1], 4)
        
        except Exception as e:
            logging.error(f"Error calculating Stochastic Oscillator: {e}")
            return 0.0
        
    async def get_bollinger_bands(self, symbol: str) -> tuple[float, float, float]:
        """
        Calculates the Bollinger Bands for a given stock symbol.
        Uses standard period of 20 days and 2 standard deviations.

        :param symbol: The stock symbol.
        :return: A tuple containing the Bollinger Bands values: (upper, middle, lower).
        """
        try:
            start_date = await self._get_past_trading_date(days=20)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)
            
            middle = data["CH_CLOSING_PRICE"].rolling(window=20).mean()
            std = data["CH_CLOSING_PRICE"].rolling(window=20).std()
            
            upper = middle + 2 * std
            lower = middle - 2 * std
            
            return round(upper.iloc[-1], 4), round(middle.iloc[-1], 4), round(lower.iloc[-1], 4)
        
        except Exception as e:
            logging.error(f"Error calculating Bollinger Bands: {e}")
            return (0.0, 0.0, 0.0)
        
    async def get_average_directional_index(self, symbol: str, days: int = 14) -> float:
        """
        Calculates the Average Directional Index (ADX) for a given stock symbol.
        Uses standard period of 14 days.

        :param symbol: The stock symbol.
        :param days: Number of days to consider for the ADX. Default is 14.
        :return: The Average Directional Index (ADX) value.
        """

        try:
            start_date = await self._get_past_trading_date(days)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            high = data["CH_TRADE_HIGH_PRICE"]
            low = data["CH_TRADE_LOW_PRICE"]
            close = data["CH_CLOSING_PRICE"]

            high_diff = high.diff()
            low_diff = low.diff()

            plus_dm = high_diff.copy()
            minus_dm = low_diff.copy()

            plus_dm[high_diff < 0] = 0
            minus_dm[low_diff > 0] = 0

            tr1 = high - low
            tr2 = abs(high - close.shift(periods=1))
            tr3 = abs(low - close.shift(periods=1))

            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(window=days).mean()

            plus_di = 100 * plus_dm.rolling(window=days).mean() / atr
            minus_di = 100 * minus_dm.rolling(window=days).mean() / atr
            
            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
            
            # TODO: This calculation is incorrect, gotta fix this
            adx = dx.rolling(window=days).mean()
            
            return round(adx.iloc[-1], 4)
        
        except Exception as e:
            logging.error(f"Error calculating Average Directional Index: {e}")
            return 0.0

    async def get_commodity_channel_index(self, symbol: str, days: int = 20) -> pd.Series:
        """
        Calculates the Commodity Channel Index (CCI) for a given stock symbol.
        Uses standard period of 20 days.

        :param symbol: The stock symbol.
        :param days: Number of days to consider for the CCI. Default is 20.
        :return: The Commodity Channel Index (CCI) Series.
        """

        try:
            buffer_days = 2 * days
            start_date = await self._get_past_trading_date(buffer_days)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            typical_price = (data["CH_TRADE_HIGH_PRICE"] + data["CH_TRADE_LOW_PRICE"] + data["CH_CLOSING_PRICE"]) / 3
            moving_average = typical_price.rolling(window=days).mean()
            mean_average_deviation = typical_price.rolling(window=days).apply(lambda x: abs(x - x.mean()).mean())
            cci = (typical_price - moving_average) / (0.015 * mean_average_deviation)

            return cci

        except Exception as e:
            logging.error(f"Error calculating Commodity Channel Index: {e}")
            return 0.0

    async def get_ichimoku_cloud(self, symbol: str) -> float:
        """
        Calculates the Ichimoku Cloud for a given stock symbol.

        :param symbol: The stock symbol.
        :return: A tuple containing the Ichimoku Cloud values: (tenkan_sen, kijun_sen, senkou_span_a, senkou_span_b, chikou_span).
        """

        try:
            start_date = await self._get_past_trading_date(days=52)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            high = data["CH_TRADE_HIGH_PRICE"]
            low = data["CH_TRADE_LOW_PRICE"]
            close = data["CH_CLOSING_PRICE"]

            tenkan_sen = (high.rolling(window=9).max() + low.rolling(window=9).min()) / 2
            kijun_sen = (high.rolling(window=26).max() + low.rolling(window=26).min()) / 2
            senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(periods=26)
            senkou_span_b = ((high.rolling(window=52).max() + low.rolling(window=52).min()) / 2).shift(periods=26)
            chikou_span = close.shift(periods=-26)

            return (
                round(tenkan_sen.iloc[-1], 4),
                round(kijun_sen.iloc[-1], 4),
                round(senkou_span_a.iloc[-1], 4),
                round(senkou_span_b.iloc[-1], 4),
                round(chikou_span.iloc[-1], 4),
            )

        except Exception as e:
            logging.error(f"Error calculating Ichimoku Cloud: {e}")
            return (0.0, 0.0, 0.0, 0.0, 0.0)

    async def get_fibonacci_retracement(self, symbol: str) -> tuple[float, float, float, float, float]:
        """
        Calculates the Fibonacci Retracement levels for a given stock symbol.

        :param symbol: The stock symbol.
        :return: A tuple containing the Fibonacci Retracement levels: (23.6%, 38.2%, 50%, 61.8%, 78.6%).
        """

        try:
            start_date = await self._get_past_trading_date(days=50)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            high = data["CH_TRADE_HIGH_PRICE"]
            low = data["CH_TRADE_LOW_PRICE"]
            close = data["CH_CLOSING_PRICE"]

            high_price = high.iloc[-1]
            low_price = low.iloc[-1]
            close_price = close.iloc[-1]

            diff = high_price - low_price
            levels = [
                close_price + 0.236 * diff,
                close_price + 0.382 * diff,
                close_price + 0.500 * diff,
                close_price + 0.618 * diff,
                close_price + 0.786 * diff,
            ]

            return tuple(round(level, 4) for level in levels)

        except Exception as e:
            logging.error(f"Error calculating Fibonacci Retracement: {e}")
            return (0.0, 0.0, 0.0, 0.0, 0.0)

    async def get_support_and_resistance_levels(self, symbol: str, days: int) -> tuple[float, float]:
        """
        Calculates the Support and Resistance levels for a given stock symbol.

        :param symbol: The stock symbol.
        :param days: Number of days to consider for the levels.
        :return: A tuple containing the Support and Resistance levels: (Support, Resistance).
        """

        try:
            start_date = await self._get_past_trading_date(days)
            end_date = datetime.datetime.now().strftime("%d-%m-%Y")
            data = await self.get_equity_history(symbol, start_date, end_date)

            high = data["CH_TRADE_HIGH_PRICE"]
            low = data["CH_TRADE_LOW_PRICE"]
            close = data["CH_CLOSING_PRICE"]

            pivot = (high.iloc[-1] + low.iloc[-1] + close.iloc[-1]) / 3

            support = (2 * pivot) - high.iloc[-1]
            resistance = (2 * pivot) - low.iloc[-1]

            return (round(support, 4), round(resistance, 4))

        except Exception as e:
            logging.error(f"Error calculating Support and Resistance levels: {e}")
            return (0.0, 0.0)


# async def main():
#     from pprint import pprint

#     async with NSEIndia() as nse:
#         symbols = await nse.get_simple_moving_average("INFY", days=21)
#         pprint(symbols)

# asyncio.run(main())
