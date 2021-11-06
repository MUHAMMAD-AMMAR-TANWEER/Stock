import os
import datetime
import requests
import pandas as pd
from dateutil.parser import parse
import threading
import time





API_URL = "https://www.alphavantage.co/query"


def predictions_of_company(company,type=0,time="5min"):
     apikey = ""  # key
     if type == 0:





         data = {"function": "TIME_SERIES_DAILY_ADJUSTED",
                     "symbol": company,
                     "outputsize": "full",
                     "datatype": "json",
                     "apikey": apikey}


         response = requests.get(API_URL,
                                     data)
         response_json = response.json()


#
#
         data = pd.DataFrame.from_dict(response_json['Time Series (Daily)'], orient='index')
     #data = data.reset_index()
         data = data.iloc[::-1]
     # data = data.drop('index', axis=1)
     # data = data.apply(pd.to_numeric)
#
         return data
     if type == 1:




         data = {"function": "TIME_SERIES_INTRADAY",
                 "symbol": company,
				 "interval": time,
                 "outputsize": "full",
                 "datatype": "json",
                 "apikey": apikey}

         response = requests.get(API_URL,
                                 data)
         response_json = response.json()

         #
         #
         data = pd.DataFrame.from_dict(response_json['Time Series ({})'.format(time)], orient='index')
         # data = data.reset_index()
         data = data.iloc[::-1]
         return data

     # data = data.drop('index', axis=1)
     # data = data.apply(pd.to_numeric)


def transform_data(symbol):
    df = pd.read_csv("./stock_data/{}.csv".format(symbol))

    ticker = df.rename(
        columns={'1. open': 'Open', "2. high": "High", "3. low": "Low", "4. close": "Close", "6. volume": "Volume","Unnamed: 0":"Date"})
    delta = ticker['Close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    rs = ema_up / ema_down

    ticker['RSI'] = 100 - (100 / (1 + rs))
    exp1 = ticker['Close'].ewm(span=12, adjust=False).mean()

    exp2 = ticker['Close'].ewm(span=26, adjust=False).mean()

    ticker['MACD'] = exp1 - exp2
    ticker['Signal line'] = ticker['MACD'].ewm(span=9, adjust=False).mean()

    ticker['14-high'] = ticker['High'].rolling(14).max()
    ticker['3-low'] = ticker['Low'].rolling(3).min()
    ticker['%K'] = (ticker['Close'] - ticker['3-low']) * 100 / (ticker['14-high'] - ticker['3-low'])

    ticker = ticker.drop(["14-high", "3-low"], axis=1)

    ticker = ticker[['Date', 'Open', 'High', 'Low', 'Close',
                     '5. adjusted close', 'Volume', '7. dividend amount',
                     '8. split coefficient', 'RSI', 'MACD', 'Signal line', '%K']]

    ticker = ticker[26:]

    ticker.to_csv("./stock_data/{}.csv".format(symbol))

class nasdaq():
    def __init__(self):
        self.output = './stock_data'
        self.company_list = './companylist.csv'

    # def build_url(self, symbol):
    # 	url = 'https://www.quandl.com/api/v3/datasets/WIKI/{}.csv?api_key={}'.format(symbol, "vwxvozMWYzhkNkw2XRu1")
    # 	return url

    def symbols(self):
        symbols = []
        with open(self.company_list, 'r') as f:
            next(f)
            for line in f:
                symbols.append(line.split(',')[0].strip())
        return symbols

def download(i, symbol):
    print('Downloading {} {}'.format(symbol, i))
    try:
        df = predictions_of_company(symbol,type=0)#here you can pass time

        df.to_csv("./stock_data/{}.csv".format(symbol))


    except Exception as e:
        print('Failed to download {}'.format(symbol))
        print(e)

def download_all():
    if not os.path.exists('./stock_data'):

        os.makedirs('./stock_data')

    nas = nasdaq()
    for i, symbol in enumerate(nas.symbols()):


        download(i,symbol)
        time.sleep(2)
        print(i)
        transform_data(symbol)
        print(i)
        break

if __name__ == '__main__':
    download_all()