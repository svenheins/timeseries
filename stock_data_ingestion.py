import yfinance as yf
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import plotly.graph_objects as go
import finnhub  # <-- Add finnhub import
from datetime import datetime, timedelta  # <-- Add datetime import

from dotenv import load_dotenv
import os

load_dotenv()


class InfluxDBHandler:
    def __init__(self):
        self.token = os.getenv("TOKEN")
        self.org = os.getenv("ORG")
        self.bucket = os.getenv("BUCKET")
        self.finnhub_api_key = os.getenv("FINNHUB_API_KEY")  # <-- Load Finnhub key

        self.url = os.getenv("URL")
        self.client = None
        self.finnhub_client = None  # <-- Initialize finnhub client attribute

    def connect(self):
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            # Initialize Finnhub client if API key is present
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
        """Retrieves stock closing prices and market news for given symbols and time range."""
        try:
            query_api = self.client.query_api()
            symbols_flux_array = "[" + ", ".join([f'"{s}"' for s in symbols]) + "]"

            # Query for stock closing prices
            stock_query = f"""
                from(bucket: "{self.bucket}")
                |> range(start: {start_time}, stop: {end_time})
                |> filter(fn: (r) => r["_measurement"] == "stock_data")
                |> filter(fn: (r) => r["_field"] == "close")
                |> filter(fn: (r) => contains(value: r.symbol, set: {symbols_flux_array}))
                |> yield(name: "stock_prices")
            """

            # Query for market news headlines and summaries
            news_query = f"""
                from(bucket: "{self.bucket}")
                |> range(start: {start_time}, stop: {end_time})
                |> filter(fn: (r) => r["_measurement"] == "market_news")
                |> filter(fn: (r) => r["_field"] == "headline" or r["_field"] == "summary" or r["_field"] == "url") // Fetch headline, summary, and URL
                |> filter(fn: (r) => contains(value: r.symbol, set: {symbols_flux_array}))
                |> pivot(rowKey:["_time", "symbol"], columnKey: ["_field"], valueColumn: "_value") // Pivot to get headline/summary/url in columns
                |> yield(name: "news_events")
            """

            # Combine queries
            full_query = stock_query + "\n" + news_query
            result_tables = query_api.query(full_query, org=self.org)

            stock_data = {}
            news_data = {}
            # Initialize dictionaries
            for symbol in symbols:
                stock_data[symbol] = {}
                news_data[symbol] = []  # Store news as a list of dicts

            # Process results
            for table in result_tables:
                measurement = (
                    table.records[0].get_measurement() if table.records else None
                )
                for record in table.records:
                    symbol = record.values.get("symbol")
                    time = record.get_time()

                    if symbol not in symbols:  # Skip if symbol not requested
                        continue

                    if measurement == "stock_data":
                        value = record.get_value()
                        if time and value is not None:
                            stock_data[symbol][time] = value
                    elif measurement == "market_news":
                        headline = record.values.get("headline")
                        summary = record.values.get(
                            "summary", ""
                        )  # Handle potentially missing summary
                        url = record.values.get(
                            "url", ""
                        )  # Handle potentially missing url
                        if time and headline:
                            news_data[symbol].append(
                                {
                                    "time": time,
                                    "headline": headline,
                                    "summary": summary,
                                    "url": url,
                                }
                            )

            # Convert stock data to DataFrame
            stock_df = pd.DataFrame.from_dict(stock_data)
            if not stock_df.empty:
                stock_df.index = pd.to_datetime(stock_df.index, utc=True)
                stock_df = stock_df.sort_index()
            else:
                print("No stock data retrieved.")
                stock_df = pd.DataFrame()  # Ensure it's a DataFrame even if empty

            # News data remains as a dictionary of lists
            print("Stock and news data retrieved successfully")
            return (
                stock_df,
                news_data,
            )  # Return both stock DataFrame and news dictionary

        except Exception as e:
            print(f"Error retrieving data: {e}")
            return pd.DataFrame(), {}  # Return empty structures on error

    def visualize_data(
        self, stock_df, news_data, output_file="stock_news_visualization.html"
    ):
        """Visualizes stock prices and adds markers for news events."""
        try:
            fig = go.Figure()

            # Plot stock price lines
            for symbol in stock_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=stock_df.index,
                        y=stock_df[symbol],
                        mode="lines",
                        name=f"{symbol} Price",
                    )
                )

                # Add news event markers for this symbol
                if symbol in news_data and news_data[symbol]:
                    news_times = [item["time"] for item in news_data[symbol]]
                    # Get corresponding stock prices at news times (or nearest) for marker Y position
                    # We need to reindex the stock data to potentially match news times exactly
                    # Or find the closest price point. Using reindex with tolerance for simplicity.
                    aligned_prices = stock_df[symbol].reindex(
                        news_times, method="nearest", tolerance=pd.Timedelta("1d")
                    )

                    news_headlines = [item["headline"] for item in news_data[symbol]]
                    news_summaries = [item["summary"] for item in news_data[symbol]]
                    news_urls = [item["url"] for item in news_data[symbol]]

                    # Create hover text with HTML for line breaks and links
                    hover_texts = [
                        f"<b>{headline}</b><br><br>{summary}<br><a href='{url}' target='_blank'>Link</a>"
                        for headline, summary, url in zip(
                            news_headlines, news_summaries, news_urls
                        )
                    ]

                    fig.add_trace(
                        go.Scatter(
                            x=news_times,
                            y=aligned_prices,  # Place marker on the price line at the news time
                            mode="markers",
                            marker=dict(
                                size=8, symbol="circle", color="red"
                            ),  # Style markers
                            name=f"{symbol} News",
                            hoverinfo="text",  # Use custom hover text
                            hovertext=hover_texts,
                        )
                    )

            fig.update_layout(
                title="Stock Prices with Market News Events",
                xaxis_title="Date",
                yaxis_title="Closing Price",
                hovermode="x unified",  # Improved hover experience
            )
            fig.write_html(output_file)
            print(f"Combined visualization saved to '{output_file}'")
            return True
        except Exception as e:
            print(f"Error visualizing data: {e}")
            return False

    def ingest_market_news(self, symbol, days_back=30):
        """Fetches and ingests market news for a symbol from Finnhub."""
        if not self.finnhub_client:
            print("Finnhub client not initialized. Skipping news ingestion.")
            return False

        try:
            # Calculate date range for news fetching
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days_back)).strftime(
                "%Y-%m-%d"
            )

            # Fetch company news for the symbol
            # Note: Finnhub's free tier has limitations on date range and frequency
            news = self.finnhub_client.company_news(
                symbol, _from=start_date, to=end_date
            )

            if not news:
                print(
                    f"No news found for symbol '{symbol}' in the last {days_back} days."
                )
                return True  # Not an error if no news exists

            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            points_to_write = []
            for item in news:
                # Convert Finnhub's timestamp (seconds since epoch) to datetime
                news_time = datetime.utcfromtimestamp(item["datetime"])

                point = (
                    Point("market_news")  # Use a separate measurement for news
                    .tag("symbol", symbol)
                    .time(
                        news_time, write_precision="s"
                    )  # Store time with second precision
                    .field("headline", str(item["headline"]))
                    .field("summary", str(item["summary"]))
                    .field("source", str(item["source"]))
                    .field("url", str(item["url"]))
                    .field("id", int(item["id"]))  # Store Finnhub's news ID
                    .field(
                        "category", str(item.get("category", "N/A"))
                    )  # Add category if available
                )
                points_to_write.append(point)

            if points_to_write:
                write_api.write(
                    bucket=self.bucket, org=self.org, record=points_to_write
                )
                print(
                    f"Ingested {len(points_to_write)} news items for symbol '{symbol}' into measurement 'market_news'"
                )
            else:
                print(f"No valid news points generated for symbol '{symbol}'.")

            return True
        except Exception as e:
            print(f"Error ingesting news for symbol '{symbol}': {e}")
            # Consider specific error handling for API rate limits if needed
            return False


if __name__ == "__main__":
    influx_handler = InfluxDBHandler()

    if influx_handler.connect():
        if influx_handler.test_connection():
            symbols = ["AAPL", "MSFT", "GOOG"]
            for symbol in symbols:
                # Ingest stock data (adjust period as needed)
                influx_handler.ingest_data(symbol, period="3mo")
                # Ingest market news for the same period (e.g., last 90 days)
                influx_handler.ingest_market_news(symbol, days_back=90)

            start_time = "-30d"
            end_time = "now()"
            # Retrieve both stock prices (df) and news events (dict)
            stock_df, news_data = influx_handler.retrieve_data(
                symbols, start_time, end_time
            )

            # Check if stock_df has data before visualizing
            if stock_df is not None and not stock_df.empty:
                # Pass both stock data and news data to the visualization function
                influx_handler.visualize_data(stock_df, news_data)
            elif (
                stock_df is not None
            ):  # It's an empty DataFrame, means no stock data found
                print("No stock data found for the specified period. Cannot visualize.")
            else:  # Error occurred during retrieval
                print("Failed to retrieve data for visualization.")
        else:
            print("Failed to test InfluxDB connection")
    else:
        print("Failed to connect to InfluxDB")
