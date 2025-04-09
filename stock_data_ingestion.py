import yfinance as yf
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import plotly.graph_objects as go

from dotenv import load_dotenv
import os

load_dotenv()


class InfluxDBHandler:
    def __init__(self):
        self.token = os.getenv("TOKEN")
        self.org = os.getenv("ORG")
        self.bucket = os.getenv("BUCKET")

        self.url = os.getenv("URL")
        self.client = None

    def connect(self):
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            print("Connected to InfluxDB")
            return True
        except Exception as e:
            print(f"Error connecting to InfluxDB: {e}")
            return False

    def test_connection(self):
        try:
            self.client.ping()
            print("InfluxDB connection test successful")
            return True
        except Exception as e:
            print(f"InfluxDB connection test failed: {e}")
            return False

    def ingest_data(self, symbol, period="3mo"):
        try:
            data = yf.download(symbol, period=period)
            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            for index, row in data.iterrows():
                point = (
                    Point("stock_data")
                    .tag("symbol", symbol)
                    .time(index)
                    .field("open", float(row["Open"].iloc[0]))
                    .field("high", float(row["High"].iloc[0]))
                    .field("low", float(row["Low"].iloc[0]))
                    .field("close", float(row["Close"].iloc[0]))
                    .field("volume", float(row["Volume"].iloc[0]))
                )
                if "Adj Close" in row:
                    point = point.field("adj_close", row["Adj Close"])
                write_api.write(bucket=self.bucket, org=self.org, record=point)
            print(f"Data ingested for symbol '{symbol}' into bucket '{self.bucket}'")
            return True
        except Exception as e:
            print(f"Error ingesting data for symbol '{symbol}': {e}")
            return False

    def retrieve_data(self, symbols, start_time, end_time):
        try:
            query_api = self.client.query_api()
            # Correctly format symbols for Flux array: ["AAPL", "MSFT"] -> ["AAPL", "MSFT"]
            symbols_flux_array = "[" + ", ".join([f'"{s}"' for s in symbols]) + "]"

            query = f"""
                from(bucket: "{self.bucket}")
                |> range(start: {start_time}, stop: {end_time})
                |> filter(fn: (r) => r["_measurement"] == "stock_data")
                |> filter(fn: (r) => r["_field"] == "close")
                |> filter(fn: (r) => contains(value: r.symbol, set: {symbols_flux_array}))
            """
            tables = query_api.query(query, org=self.org)  # Ensure org is passed

            data = {}
            # Initialize dictionary structure for each symbol
            for symbol in symbols:
                data[symbol] = {}

            # Populate the dictionary
            for table in tables:
                for record in table.records:
                    symbol = record.values.get("symbol")
                    time = record.get_time()  # Use get_time() for datetime object
                    value = record.get_value()
                    # Ensure symbol is one we requested and data is valid
                    if symbol in data and time and value is not None:
                        data[symbol][time] = value

            # Convert to DataFrame
            df = pd.DataFrame.from_dict(data)

            # Check if DataFrame is empty before proceeding
            if df.empty:
                print("No data retrieved or DataFrame is empty after processing.")
                return pd.DataFrame()

            # Convert index to datetime (UTC) and sort
            df.index = pd.to_datetime(df.index, utc=True)
            df = df.sort_index()

            print("Data retrieved successfully")
            return df
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return None

    def visualize_data(self, df, output_file="stock_visualization.html"):
        try:
            fig = go.Figure()
            for column in df.columns:
                fig.add_trace(
                    go.Scatter(x=df.index, y=df[column], mode="lines", name=column)
                )
            fig.update_layout(
                title="Stock Data Visualization",
                xaxis_title="Date",
                yaxis_title="Closing Price",
            )
            fig.write_html(output_file)
            print(f"Data visualized and saved to '{output_file}'")
            return True
        except Exception as e:
            print(f"Error visualizing data: {e}")
            return False


if __name__ == "__main__":
    influx_handler = InfluxDBHandler()

    if influx_handler.connect():
        if influx_handler.test_connection():
            symbols = ["AAPL", "MSFT", "GOOG"]
            for symbol in symbols:
                influx_handler.ingest_data(symbol, period="2mo")

            start_time = "-30d"
            end_time = "now()"
            retrieved_df = influx_handler.retrieve_data(symbols, start_time, end_time)

            if retrieved_df is not None and not retrieved_df.empty:
                influx_handler.visualize_data(retrieved_df)
        else:
            print("Failed to test InfluxDB connection")
    else:
        print("Failed to connect to InfluxDB")
