#!/usr/bin/python3

import market
import pandas as pd
from configparser import ConfigParser

def main():
    config = ConfigParser()
    config.read('/opt/market/market.ini')
    stocks = config.get('general', 'stocks')

    db = "postgres://postgres@127.0.0.1:5432/stocks"
    tadb = "postgres://postgres@127.0.0.1:5432/ta"
    session = market.session.Session(db)
    tasession = market.session.Session(tadb)

    for stock in stocks.split():
        print(stock)

        if not session.check_table(stock):
            continue

        dbdf = session.fetch_df(stock)
        close = dbdf['close'].resample('D').last()

        df = pd.DataFrame()
        df['ema50'] = close.ewm(span=50, adjust=False).mean()
        df['ema200'] = close.ewm(span=200, adjust=False).mean()

        tasession.push_df(dataframe=df, name=stock)

if __name__ == "__main__":
    main()
