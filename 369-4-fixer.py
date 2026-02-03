import duckdb as ddb
import pandas as pd
from datetime import datetime
import time
import argparse
import os


path = "/Users/Justin/Downloads/2022_place_canvas_history5.parquet"

print("SCRIPT START")


X_MIN, X_MAX = 77, 126
Y_MIN, Y_MAX = 531, 577


sql_fixers = """
WITH base AS (
    SELECT
        user_id,
        timestamp AS ts,
        pixel_color,
        TRY_CAST(split_part(coordinate, ',', 1) AS INTEGER) AS x,
        TRY_CAST(split_part(coordinate, ',', 2) AS INTEGER) AS y
    FROM read_parquet(?)
    ),
    region AS (
  SELECT * FROM base
  WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ?
  ),
  seq AS (
    SELECT
        x, y, ts, pixel_color, user_id,
        lag(pixel_color) OVER  (PARTITION BY x, y ORDER BY ts) AS prev_pixel_color,
        lag(user_id) OVER (PARTITION BY x, y ORDER BY ts) AS prev_user,
        lag(ts) OVER (PARTITION BY x, y ORDER BY ts) AS prev_ts
    FROM region
    ), 
    reverts AS (
    SELECT
        user_id,
        COUNT(*) AS rapid_reverts,
        AVG(EXTRACT(EPOCH FROM (ts - prev_ts))) AS avg_latency_s
    FROM seq
    WHERE prev_pixel_color IS NOT NULL
        AND pixel_color <> prev_pixel_color
        AND user_id <> prev_user
        AND EXTRACT(EPOCH FROM (ts - prev_ts)) <= 60
    GROUP BY user_id
    )
    SELECT *
    FROM reverts
    WHERE rapid_reverts >= 10
    ORDER BY rapid_reverts DESC, avg_latency_s ASC;
"""
fixers = ddb.execute(sql_fixers, [path, X_MIN, X_MAX, Y_MIN, Y_MAX]).df()
print(fixers)
print(len(fixers))