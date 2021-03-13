#!/usr/bin/python3

import market
import pandas as pd
from configparser import ConfigParser
from yfinance import Ticker

def main():
    config = ConfigParser()
    config.read('/opt/market/market.ini')
    stocks = config.get('general', 'stocks')

    db = "postgres://postgres@127.0.0.1:5432/stocks"
    session = market.session.Session(db)

    for stock in stocks.split():
        print(stock)

        if not session.check_table(stock):
            continue

        ticker = Ticker(stock)
        df = ticker.history(period='15m', interval='1m')

        session.push_df(dataframe=df, name=stock)

if __name__ == "__main__":
    main()
