# Transforming Data with Spark

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, LongType

def transform_data(raw_data):
    """
    Convert raw pandas DataFrames to Spark DataFrames according to predefined schemas.
    Returns a dict {ticker: spark_df}.
    """
    spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

    # Define Spark schemas for stock, index, and macro data
    stock_schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("date", DateType(), True),
        StructField("open", FloatType(), True),
        StructField("high", FloatType(), True),
        StructField("low", FloatType(), True),
        StructField("close", FloatType(), True),
        StructField("adj_close", FloatType(), True),
        StructField("volume", LongType(), True),
    ])

    index_schema = StructType([
        StructField("index_name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("open", FloatType(), True),
        StructField("high", FloatType(), True),
        StructField("low", FloatType(), True),
        StructField("close", FloatType(), True),
        StructField("adj_close", FloatType(), True),
        StructField("volume", LongType(), True),
    ])

    macro_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("date", DateType(), True),
        StructField("open", FloatType(), True),
        StructField("high", FloatType(), True),
        StructField("low", FloatType(), True),
        StructField("close", FloatType(), True),
        StructField("volume", LongType(), True),
    ])

    transformed_data = {}

    for ticker, df in raw_data.items():
        if df.empty:
            print(f"DataFrame for {ticker} is empty. Skipping.")
            continue

        # Identify which schema to use based on ticker category
        if ticker in ["AAPL", "NVDA", "MSFT", "AVGO", "META", "AMZN", "TSLA"]:
            df["ticker"] = ticker
            for col in ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]:
                if col not in df.columns:
                    df[col] = None
            df = df[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]]
            schema = stock_schema

        elif ticker in ["^GSPC", "NQ=F", "RTY=F", "^DJI"]:
            df["index_name"] = ticker
            for col in ["index_name", "date", "open", "high", "low", "close", "adj_close", "volume"]:
                if col not in df.columns:
                    df[col] = None
            df = df[["index_name", "date", "open", "high", "low", "close", "adj_close", "volume"]]
            schema = index_schema

        else:
            df["symbol"] = ticker
            for col in ["symbol", "date", "open", "high", "low", "close", "volume"]:
                if col not in df.columns:
                    df[col] = None
            df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]
            schema = macro_schema

        # Convert pandas DataFrame to Spark DataFrame using the chosen schema
        try:
            spark_df = spark.createDataFrame(df, schema=schema)
            transformed_data[ticker] = spark_df
        except Exception as e:
            print(f"Error transforming data for {ticker}: {e}")

    return transformed_data