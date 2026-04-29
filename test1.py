import yfinance as yf
df = yf.Ticker("RELIANCE.NS").history(period="5d")
print(df.tail(2))