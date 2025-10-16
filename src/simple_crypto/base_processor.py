import pandas as pd
from typing import Literal
import os
from datetime import datetime, timedelta
import numpy as np

def get_peaks(data: str | list[str] | pd.DataFrame, column: str = "close", time_col: str = "close_time",area: int = 20):
    if isinstance(data, str):
        if not os.path.isfile(data):
            raise ValueError("File does not exist")
        data = pd.read_csv(data)
    elif isinstance(data, list):
        for item in data:
            if not isinstance(item, str):
                raise ValueError("All items in the data list must be strings (file paths)")
            if not os.path.isfile(item):
                raise ValueError(f"File {item} does not exist")
        data = pd.concat([pd.read_csv(item) for item in data], ignore_index=True)
    elif not isinstance(data, pd.DataFrame):
        raise ValueError("Data must be a string (file path), list of strings, or pandas DataFrame")

    if column not in data.columns:
        raise ValueError(f"Column '{column}' does not exist in data")

    if time_col not in data.columns:
        raise ValueError(f"Time column '{time_col}' does not exist in data")

    if not isinstance(area, int) or area <= 0:
        raise ValueError("Area must be a positive integer")

    peaks = []
    # TODO: Implement using same strategy as sound_processor

    return peaks