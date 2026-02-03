import duckdb as ddb
import pandas as pd
from datetime import datetime
import time
import argparse
import os


path = "/Users/Justin/Downloads/2022_place_canvas_history5.parquet"

print("SCRIPT START")


sql_detect_admin =  """
  SELECT *
  FROM read_parquet(?)
  WHERE length(coordinate) - length(replace(coordinate, ',', '')) = 3;

  """



detect_admin = ddb.execute(sql_detect_admin, [path]).df()


print(detect_admin)