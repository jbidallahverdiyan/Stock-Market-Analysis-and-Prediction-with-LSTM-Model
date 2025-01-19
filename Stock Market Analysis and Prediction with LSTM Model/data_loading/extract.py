# Data Extraction with yfinance

import yfinance as yf
import pandas as pd

def extract_data(tickers, start_date="2014-01-01", end_date="2024-01-01"):
    """
    Fetch historical data using yfinance Ticker(...).history().
    Returns a dictionary {ticker: pandas.DataFrame}.

    auto_adjust=False -> 'Close' column is unadjusted.
    actions=False -> Dividends/Splits columns are not included.
    """
    data_dict = {}
    for ticker in tickers:
        try:
            print(f"Fetching data for {ticker}...")
            ticker_data = yf.Ticker(ticker)

            # Request historical data for the specified date range
            data = ticker_data.history(
                start=start_date,
                end=end_date,
                auto_adjust=False,
                actions=False
            )

            # Reset index to turn the Date index into a column
            data.reset_index(inplace=True)

            # Prepare a rename mapping for columns
            rename_cols = {
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }

            # If "Adj Close" is available, rename it; otherwise create and rename
            if "Adj Close" in data.columns:
                rename_cols["Adj Close"] = "adj_close"
            else:
                data["Adj Close"] = None
                rename_cols["Adj Close"] = "adj_close"

            # Rename columns
            data.rename(columns=rename_cols, inplace=True)

            # Convert 'date' to just YYYY-mm-dd (datetime.date)
            data["date"] = pd.to_datetime(data["date"]).dt.date

            data_dict[ticker] = data
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    return data_dict
