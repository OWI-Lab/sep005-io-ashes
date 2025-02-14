# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 15:38:12 2024

@author: AA000139
"""

import pandas as pd
import os
from datetime import datetime
import re


class ReadAshes():

    def __init__(self,
                 filepaths=dict[str],
                 statistic_start_time=None):

        self.filepaths = filepaths
        if statistic_start_time is None:
            self.statistic_start_time = 0
        else:
            self.statistic_start_time = statistic_start_time
        self._read_ashes_files()

    def _read_ashes_files(self):
        dfs = []
        for key, filepath in self.filepaths.items():
            # Read data into DataFrame with appropriate skiprows
            if os.path.exists(filepath):
                if "Sensor Mooring line" in filepath:
                    # For Mooring file, skip 12 rows and drop 6 rows
                    df = pd.read_table(filepath, skiprows=12, dtype=str)
                    df.drop(range(6), inplace=True)
                    df.reset_index(drop=True, inplace=True)
                    df.columns = [
                        f"{col} " + re.findall(r'\[.*?\]', filepath)[0][1:-1]
                        if "Time" not in col else col for col in df.columns
                    ]

                elif "Sensor Blade [Time]" in filepath:
                    # For Blade file, skip 12 rows and drop 12 rows
                    df = pd.read_table(filepath, skiprows=12, dtype=str)
                    df.drop(range(6), inplace=True)
                    df.reset_index(drop=True, inplace=True)
                    df.columns = [
                        f"{col} " + re.findall(r'\[.*?\]', filepath)[1][1:-1]
                        if "Time" not in col else col for col in df.columns
                    ]

                elif "Sensor Node" in filepath:
                    # For Node file, skip 11 rows and drop 17 rows
                    df = pd.read_table(filepath, skiprows=11, dtype=str)
                    df.drop(range(6), inplace=True)
                    df.dropna(inplace=True)
                    df.reset_index(drop=True, inplace=True)
                    df.columns = [
                        f"{col} " + re.findall(r'\[.*?\]', filepath)[0][1:-1]
                        if "Time" not in col else col for col in df.columns
                    ]

                elif "Sensor Beam element" in filepath:
                    # For Node file, skip 11 rows and drop 17 rows
                    df = pd.read_table(filepath, skiprows=11, dtype=str)
                    df.drop(range(12), inplace=True)
                    df.reset_index(drop=True, inplace=True)
                    df.columns = [
                        f"{col} " + re.findall(r'\[.*?\]', filepath)[0][1:-1]
                        if "Time" not in col else col for col in df.columns
                    ]

                elif "Electrical" in filepath:
                    pass

                else:
                    # For other files, skip 11 rows and drop 6 rows
                    df = pd.read_table(filepath, skiprows=11, dtype=str)
                    df.drop(range(6), inplace=True)
                    df.reset_index(drop=True, inplace=True)
                    if "Sensor Rotor" in filepath:
                        df.columns = [
                            f"{col} rotor" if "Time" not in col else col for col in df.columns
                        ]
                    elif "Generator" in filepath:
                        df.columns = [
                            f"{col} generator" if "Time" not in col else col for col in df.columns
                        ]
                df = df.astype(float)
                dfs.append(df)

        combined_df = pd.concat(dfs, axis=1)
        combined_df = combined_df.T[~combined_df.T.index.duplicated()].T
        combined_df.columns = combined_df.columns.str.strip()
        combined_df.columns = [" ".join(col) for col in combined_df.columns.str.split()]

        combined_df = combined_df[combined_df['Time [s]'] >= self.statistic_start_time]
        combined_df['Time [s]'] = combined_df['Time [s]'] - self.statistic_start_time
        combined_df.reset_index(inplace=True, drop=True)

        self.signals = combined_df

    def to_pandas(self):
        return self.signals

    def to_sep005(self):
        units = []
        # Extract units from column headers
        df = self.signals
        for column in df.columns:
            unit = column.split("[")[-1].split("]")[0]
            units.append(unit)
        units_df = pd.DataFrame([units], columns=df.columns)

        # Extract the first encounter of Time [s] column and set it as the index
        time_column_index = next(
            (i for i, col in enumerate(df.columns) if "Time" in col), None
        )
        if time_column_index is not None:
            df.index = pd.to_datetime(
                df.iloc[:, time_column_index], unit="s"
            )
            df = df.drop(
                columns=[col for col in df.columns if "Time" in col]
            )
            units_df = units_df.drop(
                columns=[col for col in units_df.columns if "Time" in col]
            )

        # Define a fictive measurement start datetime for Ashes data
        fictive_measurement_start_ashes = datetime(2022, 1, 1)
        df.index += fictive_measurement_start_ashes - df.index[0]

        # Initialize an empty list to store the time vector
        start_timestamp = df.index[0]
        time_seconds = []
        # Extract time vector from the datetime index
        for timestamp in df.index:
            time_seconds.append((timestamp - start_timestamp).total_seconds())
        # time = np.array(time_seconds, dtype=float)

        # Clean dataframe's columns
        df.columns = df.columns.str.replace(
            r"\[.*?\]", "", regex=True
        ).str.strip()
        units_df.columns = units_df.columns.str.replace(
            r"\[.*?\]", "", regex=True
        ).str.strip()

        # Write in Sep005 format
        fs = (
            1 / (df.index[1] - df.index[0]).total_seconds()
        )  # Sampling frequency in Hz
        duration = len(df) / fs
        signals_sep005 = []

        for channel in df.columns:
            df[channel] = pd.to_numeric(df[channel], errors="coerce")
            data = df[channel].to_numpy()
            fs_signal = len(data) / duration
            signal = {
                "name": channel,
                "data": data,
                "start_timestamp": str(start_timestamp),
                "fs": fs_signal,
                "unit_str": str(units_df[channel].iloc[0]),
            }
            signals_sep005.append(signal)
        return signals_sep005
