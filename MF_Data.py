import yfinance as yf

# Replace 'FUND_SYMBOL' with your mutual fund's ticker symbol
fund = yf.Ticker("0P0001DI4I.BO")
nav_data = fund.history(period="max")  # Fetches historical NAV data
print(nav_data)

start_date = "2024-01-01 00:00:00+05:30"
end_date = "2024-12-04 00:00:00+05:30"
one = nav_data['Date']
