#!/usr/bin/python3

import market
import pandas as pd
from configparser import ConfigParser
from yfinance import Ticker

def main():
    gran = [('2y', '60m'), ('60d', '15m'), ('30d', '5m'), ('7d', '1m')]

    config = ConfigParser()
    config.read('/opt/market/market.ini')
    stocks = config.get('general', 'stocks')

    db = "postgres://postgres@127.0.0.1:5432/stocks"
    session = market.session.Session(db)

    for stock in stocks.split():
        print(stock)

        if session.check_table(stock):
            continue

        dfs = list()
        ticker = Ticker(stock)

        for period, interval in gran:
            df = ticker.history(period=period, interval=interval)
            dfs.append(df)

        df = pd.concat(dfs)
        df = market.df.deduplicate(df)

        session.push_df(dataframe=df, name=stock)

if __name__ == "__main__":
    main()
