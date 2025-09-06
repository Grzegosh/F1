import os
import requests
import pandas as pd 
from configuration import Configuration

class DataFetcher:
    def __init__(self, config: Configuration) -> None:
        self.config = config

    def fetch_data(self, url: str, params: dict = {}) -> pd.DataFrame:
        """
        Fetch data from a given URL and return it as a pandas DataFrame.

        Args:
            url (str): The API endpoint to fetch data from.
            params (dict): Optional query parameters for the request.
        """
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        return pd.DataFrame(data)

    def fetch_drivers(self) -> pd.DataFrame:
        return self.fetch_data(self.config.drivers_url)
