from enum import Enum


class Mode(Enum):
    LOCAL = "local"
    VPN = "vpn"

class Index(Enum):  # Improved: Use Enums for fixed choices
    NIFTY = "NIFTY"
    FINNIFTY = "FINNIFTY"
    BANKNIFTY = "BANKNIFTY"

class HolidayType(Enum):
    TRADING = "trading"
    CLEARING = "clearing"

class ResultPeriod(Enum):
    QUARTERLY = "Quarterly"
    ANNUAL = "Annual"
    HALF_YEARLY = "Half-Yearly"
    OTHERS = "Others"
    
class OptionType(Enum):
    CALL = "CE"
    PUT = "PE"
    FUTURES = "Fut"
    
class InstrumentType(Enum):
    OPTION_STOCK = "OPTSTK"
    OPTION_INDEX = "OPTIDX"
    FUTURES_STOCK = "FUTSTK"
    FUTURES_INDEX = "FUTIDX"
    EQUITY = "EQ"
    
class SortType(Enum):
     VOLUME = "volume"
     VALUE = "value"

class PreopenKey(Enum):
    NIFTY = "NIFTY"
    FNO = "FO"
    
class BandType(Enum):
    UPPER = "upper"
    LOWER = "lower"
    BOTH = "both"
    
class BandView(Enum):
    ALL = "AllSec"
    GREATER_THAN_20 = "SecGtr20"
    LESS_THAN_20 = "SecLwr20"
    
class LargeDealType(Enum):
    BULK = "bulk_deals"
    SHORT = "short_deals"
    BLOCK = "block_deals"

class MarketSegment(Enum):
    FO = "FO"  # Futures and Options
    COM = "COM" # Commodity
    CD = "CD"   # Currency Derivatives
    CB = "CB"   # Corporate Bonds
    CMOT = "CMOT"  # ? Not sure about this one
    IRD = "IRD"    # Interest Rate Derivatives
    MF = "MF"     # Mutual Funds
    NDM = "NDM"   # Negotiated Dealing System
    NTRP = "NTRP" # ?
    SLBS = "SLBS"  # Securities Lending and Borrowing Scheme
