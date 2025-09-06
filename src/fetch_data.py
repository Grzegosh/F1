import os
import requests
import pandas as pd 
from src.configuration import Configuration

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
    
    # Funtions to fetch specific datasets

    def fetch_drivers(self) -> pd.DataFrame:
        return self.fetch_data(self.config.drivers_url)
    
    def fetch_pit_stops(self) -> pd.DataFrame:
        return self.fetch_data(self.config.pit_url)
    
    def fetch_sessions(self) -> pd.DataFrame:
        return self.fetch_data(self.config.sessions_url)
    
    def fetch_starting_grid(self) -> pd.DataFrame:
        return self.fetch_data(self.config.starting_grid_url)   
    
    def fetch_overtakes(self) -> pd.DataFrame:
        return self.fetch_data(self.config.overtakes_url)

    def fetch_session_results(self) -> pd.DataFrame:
        return self.fetch_data(self.config.session_results_url)
