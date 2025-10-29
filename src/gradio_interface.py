from multi_data_fetcher import MultiSourceDataFetcher
import os

API_KEY = os.getenv('YOUR_API_KEY')
fetcher = MultiSourceDataFetcher(API_KEY)
fetcher.fetch_all_sources()