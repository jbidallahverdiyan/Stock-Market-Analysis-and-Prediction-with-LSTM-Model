# Main ETL Process (Extract, Transform, Load)

from pyspark.sql import SparkSession
from extract import extract_data
from transformation import transform_data
from load import load_data

def main():
    # Start a Spark session
    spark = SparkSession.builder \
        .appName("Yahoo Finance to PostgreSQL") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # Ticker lists
    stock_tickers = ["AAPL", "NVDA", "MSFT", "AVGO", "META", "AMZN", "TSLA"]
    index_tickers = ["^GSPC", "NQ=F", "RTY=F", "^DJI"]
    macro_tickers = ["GC=F", "CL=F", "^VIX", "DX-Y.NYB", "^IRX", "^TNX"]

    all_tickers = stock_tickers + index_tickers + macro_tickers

    # 1) Extract
    raw_data = extract_data(all_tickers, start_date="1900-01-01", end_date="2025-01-01")

    # 2) Transform
    transformed_data = transform_data(raw_data)

    # 3) Database config
    db_config = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "7362",
        "dbname": "finance"
    }

    # 4) Load
    for ticker, spark_df in transformed_data.items():
        if ticker in stock_tickers:
            table_name = "stock_data"
        elif ticker in index_tickers:
            table_name = "index_data"
        else:
            table_name = "macro_data"

        rows = spark_df.collect()
        if rows:
            load_data(rows, table_name, db_config)
        else:
            print(f"No rows to insert for {ticker}.")

    spark.stop()

if __name__ == "__main__":
    main()
