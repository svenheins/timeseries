import yfinance as yf
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import plotly.graph_objects as go
import finnhub  # <-- Add finnhub import
from datetime import datetime, timedelta
import os
import time
from dotenv import load_dotenv

load_dotenv()


class InfluxDBHandler:
    def __init__(self):
        self.token = os.getenv("TOKEN")
        self.org = os.getenv("ORG")
        self.bucket = os.getenv("BUCKET")
        self.finnhub_api_key = os.getenv("FINNHUB_API_KEY")

        self.url = os.getenv("URL")
        self.client = None
        self.finnhub_client = None

    def connect(self):
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            if self.finnhub_api_key:
                self.finnhub_client = finnhub.Client(api_key=self.finnhub_api_key)
                print("Finnhub client initialized.")
            else:
                print(
                    "Warning: FINNHUB_API_KEY not found in environment. News features disabled."
                )
            print("Connected to InfluxDB")
            return True
        except Exception as e:
            print(f"Error connecting to InfluxDB or initializing Finnhub client: {e}")
            return False

    def test_connection(self):
        try:
            self.client.ping()
            print("InfluxDB connection test successful")
            return True
        except Exception as e:
            print(f"InfluxDB connection test failed: {e}")
            return False

    def _check_data_exists(self, symbol, start_date, end_date, measurement):
        """Checks if data exists for a given symbol, measurement, and time range."""
        try:
            query_api = self.client.query_api()
            # Convert datetime objects to RFC3339 strings if they are not already strings
            start_str = start_date.isoformat() + "Z" if isinstance(start_date, datetime) else start_date
            end_str = end_date.isoformat() + "Z" if isinstance(end_date, datetime) else end_date

            query = (
                f'from(bucket: "{self.bucket}")\n'
                f'|> range(start: {start_str}, stop: {end_str})\n'
                f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")\n'
                f'|> filter(fn: (r) => r.symbol == "{symbol}")\n'
                f'|> limit(n: 1)\n' # Only need one record to confirm existence
                f'|> count()' # Check if count > 0 (more robust might be needed)
            )
            # A simple check: query for one record. If result is not empty, data exists.
            result = query_api.query(query, org=self.org)
            return len(result) > 0 and len(result[0].records) > 0 and result[0].records[0].get_value() > 0

        except Exception as e:
            print(f"Error checking data existence for {measurement} {symbol}: {e}")
            return False # Assume data doesn't exist if check fails

    def _ingest_stock_data(self, symbol, start_date, end_date):
        """Fetches and ingests stock data from yfinance."""
        try:
            # yfinance typically uses YYYY-MM-DD format
            start_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, datetime) else start_date
            end_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, datetime) else end_date

            print(f"Fetching stock data for {symbol} from {start_str} to {end_str}...")
            data = yf.download(symbol, start=start_str, end=end_str)
            if data.empty:
                print(f"No stock data found for {symbol} between {start_str} and {end_str}.")
                return True # No data is not an error in this context

            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            points_to_write = []
            for index, row in data.iterrows():
                # Ensure index is timezone-aware (UTC) for InfluxDB
                ts = pd.Timestamp(index).tz_localize('UTC') if pd.Timestamp(index).tzinfo is None else pd.Timestamp(index).tz_convert('UTC')

                point = (
                    Point("stock_data")
                    .tag("symbol", symbol)
                    .time(ts) # Use timezone-aware timestamp
                    .field("open", float(row["Open"])) # No .iloc[0] needed here
                    .field("high", float(row["High"]))
                    .field("low", float(row["Low"]))
                    .field("close", float(row["Close"]))
                    .field("volume", float(row["Volume"]))
                )
                if "Adj Close" in row:
                    point = point.field("adj_close", float(row["Adj Close"]))
                points_to_write.append(point)

            if points_to_write:
                write_api.write(bucket=self.bucket, org=self.org, record=points_to_write)
                print(f"Ingested {len(points_to_write)} stock data points for symbol '{symbol}'")
            return True
        except Exception as e:
            print(f"Error ingesting stock data for symbol '{symbol}': {e}")
            return False

    def _ingest_news_data(self, symbol, start_date, end_date):
        """Fetches and ingests market news from Finnhub."""
        if not self.finnhub_client:
            print("Finnhub client not initialized. Skipping news ingestion.")
            return False

        try:
            # Finnhub uses YYYY-MM-DD format
            start_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, datetime) else start_date
            end_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, datetime) else end_date

            print(f"Fetching news data for {symbol} from {start_str} to {end_str}...")
            # Finnhub might fetch news slightly outside the exact range, filter later if needed
            news = self.finnhub_client.company_news(symbol, _from=start_str, to=end_str)

            if not news:
                print(f"No news found for symbol '{symbol}' between {start_str} and {end_str}.")
                return True # No news is not an error

            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            points_to_write = []
            for item in news:
                # Convert Finnhub timestamp (seconds since epoch) to datetime
                news_time = datetime.utcfromtimestamp(item["datetime"])

                # Optional: Filter news strictly within the requested date range
                # if not (start_date <= news_time.date() <= end_date):
                #    continue

                point = (
                    Point("market_news")
                    .tag("symbol", symbol)
                    .time(news_time, write_precision="s") # Use the news timestamp
                    .field("headline", str(item["headline"]))
                    .field("summary", str(item["summary"]))
                    .field("source", str(item["source"]))
                    .field("url", str(item["url"]))
                    .field("id", int(item["id"])) # Finnhub news ID
                    .field("category", str(item.get("category", "N/A")))
                )
                points_to_write.append(point)

            if points_to_write:
                write_api.write(bucket=self.bucket, org=self.org, record=points_to_write)
                print(f"Ingested {len(points_to_write)} news items for symbol '{symbol}'")
            else:
                 print(f"No valid news points generated for symbol '{symbol}' between {start_str} and {end_str}.")

            # Finnhub has rate limits, add a small delay
            time.sleep(1)
            return True
        except Exception as e:
            # Handle potential rate limiting errors from Finnhub more specifically if needed
            print(f"Error ingesting news for symbol '{symbol}': {e}")
            return False

    def ingest_data(self, symbol, start_date, end_date):
        """
        Ingests stock data and market news for a given symbol and date range,
        checking if data already exists in InfluxDB first.
        """
        # Ensure dates are datetime objects for comparison and formatting
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Check and ingest stock data
        stock_exists = self._check_data_exists(symbol, start_date, end_date, "stock_data")
        if not stock_exists:
            print(f"Stock data for {symbol} ({start_date.date()} to {end_date.date()}) not found or incomplete. Ingesting...")
            stock_success = self._ingest_stock_data(symbol, start_date, end_date)
            if not stock_success:
                print(f"Failed to ingest stock data for {symbol}.")
                # Decide if you want to stop or continue with news
        else:
            print(f"Stock data for {symbol} ({start_date.date()} to {end_date.date()}) already exists. Skipping stock ingestion.")
            stock_success = True # Treat existing data as success

        # Check and ingest news data
        news_exists = self._check_data_exists(symbol, start_date, end_date, "market_news")
        if not news_exists:
            print(f"News data for {symbol} ({start_date.date()} to {end_date.date()}) not found or incomplete. Ingesting...")
            news_success = self._ingest_news_data(symbol, start_date, end_date)
            if not news_success:
                print(f"Failed to ingest news data for {symbol}.")
        else:
            print(f"News data for {symbol} ({start_date.date()} to {end_date.date()}) already exists. Skipping news ingestion.")
            news_success = True # Treat existing data as success

        return stock_success and news_success # Return overall success
    def retrieve_data(self, symbols, start_time, end_time):
        "Retrieves stock closing prices and market news for given symbols and time range."
        try:
            query_api = self.client.query_api()
            symbols_flux_array = "[" + ", ".join([f'"{s}"' for s in symbols]) + "]"

            # Query for stock closing prices
            stock_query = (
                'from(bucket: "'
                + self.bucket
                + '")\n'
                + "|> range(start: "
                + start_time
                + ", stop: "
                + end_time
                + ")\n"
                + '|> filter(fn: (r) => r["_measurement"] == "stock_data")\n'
                + '|> filter(fn: (r) => r["_field"] == "close")\n'
                + "|> filter(fn: (r) => contains(value: r.symbol, set: "
                + symbols_flux_array
                + "))\n"
                + '|> yield(name: "stock_prices")'
            )

            # Query for market news headlines and summaries
            news_query = (
                'from(bucket: "'
                + self.bucket
                + '")\n'
                + "|> range(start: "
                + start_time
                + ", stop: "
                + end_time
                + ")\n"
                + '|> filter(fn: (r) => r["_measurement"] == "market_news")\n'
                + '|> filter(fn: (r) => r["_field"] == "headline" or r["_field"] == "summary" or r["_field"] == "url")\n'
                + "|> filter(fn: (r) => contains(value: r.symbol, set: "
                + symbols_flux_array
                + "))\n"
                + '|> pivot(rowKey:["_time", "symbol"], columnKey: ["_field"], valueColumn: "_value")\n'
                + '|> yield(name: "news_events")'
            )

            # Combine queries
            full_query = stock_query + "\n" + news_query
            result_tables = query_api.query(full_query, org=self.org)

            stock_data = {}
            news_data = {}
            for symbol in symbols:
                stock_data[symbol] = {}
                news_data[symbol] = []

            # Process results
            for table in result_tables:
                measurement = (
                    table.records[0].get_measurement() if table.records else None
                )
                for record in table.records:
                    symbol = record.values.get("symbol")
                    time = record.get_time()

                    if symbol not in symbols:
                        continue

                    if measurement == "stock_data":
                        value = record.get_value()
                        if time and value is not None:
                            stock_data[symbol][time] = value
                    elif measurement == "market_news":
                        headline = record.values.get("headline")
                        summary = record.values.get("summary", "")
                        url = record.values.get("url", "")
                        if time and headline:
                            news_data[symbol].append(
                                {
                                    "time": time,
                                    "headline": headline,
                                    "summary": summary,
                                    "url": url,
                                }
                            )

            stock_df = pd.DataFrame.from_dict(stock_data)
            if not stock_df.empty:
                stock_df.index = pd.to_datetime(stock_df.index, utc=True)
                stock_df = stock_df.sort_index()
            else:
                print("No stock data retrieved.")
                stock_df = pd.DataFrame()

            print("Stock and news data retrieved successfully")
            return (
                stock_df,
                news_data,
            )

        except Exception as e:
            print(f"Error retrieving data: {e}")
            return pd.DataFrame(), {}

    def visualize_data(
        self, stock_df, news_data, output_file="stock_news_visualization.html"
    ):
        "Visualizes stock prices and adds markers for news events."
        try:
            fig = go.Figure()

            for symbol in stock_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=stock_df.index,
                        y=stock_df[symbol],
                        mode="lines",
                        name=f"{symbol} Price",
                    )
                )

                if symbol in news_data and news_data[symbol]:
                    news_times = [item["time"] for item in news_data[symbol]]
                    aligned_prices = stock_df[symbol].reindex(
                        news_times, method="nearest", tolerance=pd.Timedelta("1d")
                    )

                    news_headlines = [item["headline"] for item in news_data[symbol]]
                    news_summaries = [item["summary"] for item in news_data[symbol]]
                    news_urls = [item["url"] for item in news_data[symbol]]

                    hover_texts = [
                        f"<b>{headline}</b><br><br>{summary}<br><a href='{url}' target='_blank'>Link</a>"
                        for headline, summary, url in zip(
                            news_headlines, news_summaries, news_urls
                        )
                    ]

                    fig.add_trace(
                        go.Scatter(
                            x=news_times,
                            y=aligned_prices,
                            mode="markers",
                            marker=dict(size=8, symbol="circle", color="red"),
                            name=f"{symbol} News",
                            hoverinfo="text",
                            hovertext=hover_texts,
                        )
                    )

            fig.update_layout(
                title="Stock Prices with Market News Events",
                xaxis_title="Date",
                yaxis_title="Closing Price",
                hovermode="x unified",
            )
            fig.write_html(output_file)
            print(f"Combined visualization saved to '{output_file}'")
            return True
        except Exception as e:
            print(f"Error visualizing data: {e}")
            return False

    # Removed old ingest_market_news method as its logic is now in _ingest_news_data


if __name__ == "__main__":
    influx_handler = InfluxDBHandler()

    if influx_handler.connect():
        if influx_handler.test_connection():
            symbols = ["AAPL", "MSFT", "GOOG"]
            # Define date range for ingestion
            end_date_dt = datetime.now()
            start_date_dt = end_date_dt - timedelta(days=90)
            start_date_str = start_date_dt.strftime('%Y-%m-%d')
            end_date_str = end_date_dt.strftime('%Y-%m-%d')

            start_date_str = "2024-12-02"  # Example start date
            end_date_str = "2025-01-01"  # Example start date

            for symbol in symbols:
                print(f"\n--- Processing symbol: {symbol} ---")
                # Use the new combined ingest_data method
                influx_handler.ingest_data(symbol, start_date=start_date_str, end_date=end_date_str)

            # Use relative time for retrieval query if desired, or specific dates
            start_time_query = start_date_str #"-60d" # InfluxDB relative time
            end_time_query = end_date_str # "now()"
            stock_df, news_data = influx_handler.retrieve_data(
                symbols, start_time_query, end_time_query
            )

            if stock_df is not None and not stock_df.empty:
                influx_handler.visualize_data(stock_df, news_data)
            elif stock_df is not None:
                print("No stock data found for the specified period. Cannot visualize.")
            else:
                print("Failed to retrieve data for visualization.")
        else:
            print("Failed to test InfluxDB connection")
    else:
        print("Failed to connect to InfluxDB")
