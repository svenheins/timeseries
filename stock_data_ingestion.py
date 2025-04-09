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

    def ingest_market_news(self, symbol, days_back=30):
        "Fetches and ingests market news for a symbol from Finnhub."
        if not self.finnhub_client:
            print("Finnhub client not initialized. Skipping news ingestion.")
            return False

        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)

            interval = timedelta(days=7)
            current_date = start_date
            while current_date < end_date:
                next_date = min(current_date + interval, end_date)

                start_date_str = current_date.strftime("%Y-%m-%d")
                end_date_str = next_date.strftime("%Y-%m-%d")

                news = self.finnhub_client.company_news(
                    symbol, _from=start_date_str, to=end_date_str
                )

                if not news:
                    print(
                        f"No news found for symbol '{symbol}' between {start_date_str} and {end_date_str}."
                    )
                else:
                    write_api = self.client.write_api(write_options=SYNCHRONOUS)
                    points_to_write = []
                    for item in news:
                        news_time = datetime.utcfromtimestamp(item["datetime"])

                        point = (
                            Point("market_news")
                            .tag("symbol", symbol)
                            .time(news_time, write_precision="s")
                            .field("headline", str(item["headline"]))
                            .field("summary", str(item["summary"]))
                            .field("source", str(item["source"]))
                            .field("url", str(item["url"]))
                            .field("id", int(item["id"]))
                            .field("category", str(item.get("category", "N/A")))
                        )
                        points_to_write.append(point)

                    if points_to_write:
                        write_api.write(
                            bucket=self.bucket, org=self.org, record=points_to_write
                        )
                        print(
                            f"Ingested {len(points_to_write)} news items for symbol '{symbol}' between {start_date_str} and {end_date_str}"
                        )
                    else:
                        print(
                            f"No valid news points generated for symbol '{symbol}' between {start_date_str} and {end_date_str}."
                        )

                current_date = next_date
                time.sleep(1)

            return True
        except Exception as e:
            print(f"Error ingesting news for symbol '{symbol}': {e}")
            return False


if __name__ == "__main__":
    influx_handler = InfluxDBHandler()

    if influx_handler.connect():
        if influx_handler.test_connection():
            symbols = ["AAPL", "MSFT", "GOOG"]
            for symbol in symbols:
                pass
                # influx_handler.ingest_data(symbol, period="3mo")
                # influx_handler.ingest_market_news(symbol, days_back=60)

            start_time = "-30d"
            end_time = "now()"
            stock_df, news_data = influx_handler.retrieve_data(
                symbols, start_time, end_time
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
