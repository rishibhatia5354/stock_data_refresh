import pandas as pd
import logging, os
from datetime import datetime
import logging.handlers
import pandas_datareader as pdr
import yfinance as yf

yf.pdr_override()

### Set Logger ###
basedir = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s') 

logname = f"{basedir}/logs/yahoo_data_refresh.log"

fileH = logging.handlers.RotatingFileHandler(logname, maxBytes=100000, backupCount=1)
fileH.setFormatter(format)
logger.addHandler(fileH)

### All Variable Declaration ###
symbols = []

sgbUrlDict = {"SGBAUG29":["SGB Scheme 
21-22","https://www.moneycontrol.com/india/stockpricequote/finance-investment/sovereigngoldbonds250aug2029sr-v2021-22/SGB40"],
    "SGBJUL25":["SGB Scheme 
17-18","https://www.moneycontrol.com/india/stockpricequote/financeinvestments/sovereigngoldbonds250jul2025srii201718/SGB08"],
    "SGBOCT27":["SGB Scheme 
17-18","https://www.moneycontrol.com/india/stockpricequote/finance-investment/sovereigngoldbonds250oct2027sr-v2019-20/SGB19"]}


### Read the stock symbols, create a string for URL ###
with open(f"/Users/ribhatia/Library/CloudStorage/OneDrive-Bhatia/Financial/MyStocks.txt","r") as myStocks:
    for stock in myStocks:
        symbols.append(stock.rstrip())

logger.info("Getting the Quotes from YahooQuotesReader")
try:
    df = 
pdr.yahoo.quotes.YahooQuotesReader(symbols).read()[['price','regularMarketOpen','regularMarketPreviousClose','fiftyTwoWeekLow','fiftyTwoWeekHigh','shortName','longName']]
    logger.info("Completed Getting the Quotes from YahooQuotesReader")
except:
    logger.exception("Error Occured.")
    exit(100)

symbolsSeries = df.index.values
df.insert(0,column="symbol",value=symbolsSeries)

### Add the SGB Prices to the DF ###
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
        df.loc[len(df.index)] =  [symbol,0,open,previos_close,F52WeekLow,F52WeekHigh,url[0],url[0]]
    except:
        logger.exception("Unable to get the SGB Data.")

df["Refresh Time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
### Finally, write the dataframe. ###
try:
    df.to_csv(f"{basedir}/raw_stock_data.csv",index=False)
except:
    logger.exception("Error Writing DF to file.")

logger.info("---------Completed Refresh---------")
