import yfinance as yf
from datetime import datetime
import concurrent.futures
from loguru import logger
import pandas as pd
import os

"""
This script takes input a list of Stock Symbols, and writes some of the key data to a output file.
"""
basedir = os.path.dirname(os.path.abspath(__file__))
logname = f"{basedir}/logs/refresh_data.log"

logger.remove()
logger.add(logname, format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",rotation="100 KB")

file_to_write = f"{basedir}/raw_stock_data.csv"

sgbUrlDict = {"SGBAUG29":["SGB Scheme 21-22","https://www.moneycontrol.com/india/stockpricequote/finance-investment/sovereigngoldbonds250aug2029sr-v2021-22/SGB40"],
    "SGBJUL25":["SGB Scheme 17-18","https://www.moneycontrol.com/india/stockpricequote/financeinvestments/sovereigngoldbonds250jul2025srii201718/SGB08"],
    "SGBOCT27":["SGB Scheme 17-18","https://www.moneycontrol.com/india/stockpricequote/finance-investment/sovereigngoldbonds250oct2027sr-v2019-20/SGB19"]}

"""
Function to read all the stock symbols from the input file, and, store them in a list.
"""
symbols = []
try:
    df = pd.read_excel("/Users/ribhatia/Library/CloudStorage/OneDrive-RishiBhatia/Financial/AllStockSheet.xlsx",\
                  sheet_name="CMP_Stocks",usecols="I")
    new = df.dropna()
    symbols = new.MyStockSymbols.to_list()
except:
    logger.error("Unable to open the file to read the stocks.")
    exit(10)

# This was the old function to get the symbols from a separate file, changed now to get the details from the same excel.
# stock_symbol_list = f"{basedir}/MyStocksSymbols.txt"
# try:
#     with open(stock_symbol_list,"r") as myStocks:
#         for stock in myStocks:
#             symbols.append(stock.rstrip())
# except Exception as e:
#     logger.error("Unable to open the file to read the stocks.")
#     exit(10)

try:
    ### Instantiate multiple tickers.
    tickers = yf.Tickers(" ".join(symbols))
except Exception as e:
    logger.exception("Unable to instantiate ticker object from Yahoo Tickers.")
    exit(10)

stock_info_list = []

def get_stock_info(stock):
    """
    This function would get all the data related to a particular stock symbol from Yahoo Api's.
    """
    try:
        stock_qoute = tickers.tickers[f"{stock}"].info
        return stock_qoute

    except Exception :
        logger.debug(f"Error while getting details for the stock - {stock}")
        return False

maxworker = 50
logger.info("Starting parallel API calls to get the stock data.")

with concurrent.futures.ThreadPoolExecutor(max_workers=maxworker) as executor:
    all_quotes = executor.map(get_stock_info,symbols)
stock_info_list = list(all_quotes)

#### What details to write in the file?? These can be changed/ updated.
details = "symbol,open,regularMarketOpen,regularMarketPreviousClose,fiftyTwoWeekLow,fiftyTwoWeekHigh,shortName,longName".split(",")
refresh_time  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

with open(file_to_write,"w") as file:
    file.write("symbol,price,regularMarketOpen,regularMarketPreviousClose,fiftyTwoWeekLow,fiftyTwoWeekHigh,shortName,longName,Refresh Time"+"\n")

###Writing the output.
with open(file_to_write,"a") as file:
    for stock_data in stock_info_list:
        if stock_data:
            for detail in details:
                try:
                    stringToWrite = str(stock_data[f"{detail}"]).replace(",","")
                    file.write(stringToWrite+",")
                except KeyError as KeyExcept :
                    if detail == "longName":
                        stringToWrite = str(stock_data["shortName"]).replace(",","")
                        file.write(stringToWrite+",")
                    else:
                        logger.debug(f"Error for the key - {detail}, symbols - {stock_data['symbol']}")
                        file.write(",")
                    pass
                except TypeError as TypeExp:
                    logger.exception(f"Type Error Occured for stock - {stock}")
                    pass
                except Exception as Exp:
                    logger.exception("Some Other Error Occured.")
                    pass
            file.write(refresh_time)
            file.write("\n")
        else:
            pass

### Add the SGB Prices to the DF ###
with open(file_to_write,"a") as file:
    for symbol, url in sgbUrlDict.items():
        try:
            logger.info(f"Getting the SGB Data for {symbol}")
            data = pd.read_html(url[1])
            if symbol=="SGBAUG29":
                try:
                    previos_close=float(data[2].iloc[1,1])
                    open=float(data[2].iloc[0,1])
                    F52WeekHigh = float(data[2].iloc[4,1])
                    F52WeekLow = float(data[2].iloc[5,1])
                except:
                    pass
            else:
                previos_close=float(data[0].iloc[1,1])
                open=float(data[0].iloc[0,1])
                F52WeekHigh = float(data[1].iloc[4,1])
                F52WeekLow = float(data[1].iloc[5,1])
            
            file.write(f"{symbol},0,{open},{previos_close},{F52WeekLow},{F52WeekHigh},{url[0]},{url[0]},{refresh_time}")
            file.write("\n")
        except:
            logger.exception("Unable to get the SGB Data.")

logger.info("---------Completed Refresh---------\n\n")
