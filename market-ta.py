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

        dbdf = session.fetch_df('SELECT last(datetime) AS datetime, last(close) AS close FROM "{}" GROUP BY date_trunc(\'day\', datetime) ORDER BY 1;'.format(stock))
        close = dbdf['close']

        df = pd.DataFrame()
        df['ema50'] = close.ewm(span=50, adjust=False).mean()
        df['ema200'] = close.ewm(span=200, adjust=False).mean()

        tasession.push_df(dataframe=df, name=stock, replace=True)

if __name__ == "__main__":
    main()
