from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd
import numpy as np
import time

class DataAggregator:
    def __init__(self, config: Configuration, fetcher: DataFetcher) -> None:
        self.config = Configuration('src/config.cfg')
        self.fetcher = DataFetcher(self.config)

    def aggregate_overtakes(self):
        overtakes_df = self.fetcher.fetch_overtakes()
        time.sleep(5)  # To avoid hitting API rate limits
        drivers_df = self.fetcher.fetch_drivers()
        drivers_df = drivers_df[['session_key', 'driver_number', 'team_name', 'full_name']]
        time.sleep(10)
        sessions_with_overtakes = ['Race', 'Sprint']
        sessions_df = self.fetcher.fetch_sessions()
        sessions_df = sessions_df[sessions_df['session_name'].isin(sessions_with_overtakes)]
        sessions_df = sessions_df[['session_key', 'location', 'date_start']]


        driver_overtakes_agg = overtakes_df.groupby(
            [
                'session_key',
                'overtaking_driver_number'
            ]
        ).size().reset_index(name='overtakes_count')

        driver_overtaken_agg = overtakes_df.groupby(
            [
                'session_key',
                'overtaken_driver_number'
            ]
        ).size().reset_index(name='overtaken_count')


        merged_df = sessions_df.merge(
            drivers_df,
            on = 'session_key',
            how = 'inner'
        ).merge(
            driver_overtakes_agg,
            left_on = ['session_key', 'driver_number'],
            right_on = ['session_key', 'overtaking_driver_number'],
            how = 'inner'
        ).merge(
            driver_overtaken_agg,
            left_on = ['session_key', 'driver_number'],
            right_on = ['session_key', 'overtaken_driver_number'],
            how = 'inner'
        )

        calculate_team_overtakes = merged_df.groupby(
            ['session_key',
            'team_name'
        ]).agg(
            {
                'overtakes_count': 'sum',
                'overtaken_count': 'sum'
            }
        ).reset_index().rename(columns={
            'overtakes_count': 'team_overtakes_count',
            'overtaken_count': 'team_overtaken_count'
        })
        
        overtakes_aggregation = merged_df.merge(
            calculate_team_overtakes,
            on = ['session_key', 'team_name'],
            how = 'inner'
        )

        return overtakes_aggregation[['session_key', 'location', 'date_start', 'driver_number', 'team_name',
                                      'full_name', 'overtakes_count', 'overtaken_count',
                                      'team_overtakes_count', 'team_overtaken_count']]
        


        #return merged_df[['session_key', 'location', 'date_start', 'driver_number', 'team_name','full_name',
                         # 'overtakes_count', 'overtaken_count']]
        