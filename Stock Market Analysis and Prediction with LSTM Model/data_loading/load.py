# Loading Data into PostgreSQL

import psycopg2
from psycopg2.extras import execute_values

def load_data(data, table_name, db_config):
    """
    Insert Spark Rows (collected as a list) into the specified PostgreSQL table.
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        if table_name == "stock_data":
            query = f"""
            INSERT INTO {table_name}
            (ticker, date, open, high, low, close, adj_close, volume)
            VALUES %s
            """
            values = [
                (
                    row.ticker,
                    row.date,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.adj_close,
                    row.volume
                )
                for row in data
            ]

        elif table_name == "index_data":
            query = f"""
            INSERT INTO {table_name}
            (index_name, date, open, high, low, close, adj_close, volume)
            VALUES %s
            """
            values = [
                (
                    row.index_name,
                    row.date,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.adj_close,
                    row.volume
                )
                for row in data
            ]

        else:  # macro_data
            query = f"""
            INSERT INTO {table_name}
            (symbol, date, open, high, low, close, volume)
            VALUES %s
            """
            values = [
                (
                    row.symbol,
                    row.date,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.volume
                )
                for row in data
            ]

        execute_values(cursor, query, values)
        conn.commit()
        print(f"Data successfully loaded into {table_name}.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()