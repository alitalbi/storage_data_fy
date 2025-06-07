import yfinance as yf


def get_data(ticker,interval):
    data_ticker = yf.download(ticker,interval=interval)
    return data_ticker


def push_data_git():
    pass

def check_update():
    pass

