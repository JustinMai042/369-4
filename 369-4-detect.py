import duckdb as ddb
import pandas as pd
from datetime import datetime
import time
import argparse
import os


path = "/Users/Justin/Downloads/2022_place_canvas_history5.parquet"

print("SCRIPT START")

sql_detect_events = """

WITH per_min_color AS (
    SELECT
        date_trunc('minute', timestamp) AS minute,
        pixel_color,
        count(*) AS n
    FROM read_parquet(?)
     GROUP BY minute, pixel_color
     ),
    per_min_total AS (
    SELECT 
        minute, sum(n) AS total_n
    FROM per_min_color
    GROUP BY minute
    ),
    joined AS (
    SELECT
        c.minute,
        c.pixel_color,
        c.n,
        t.total_n,
        (c.n::DOUBLE / t.total_n) AS frac
    FROM per_min_color c
    JOIN per_min_total t USING (minute)
    )
    SELECT
        minute,
        pixel_color AS dominant_color,
        frac,
        n AS dominant_n,
        total_n
        FROM joined
    QUALIFY row_number() OVER (PARTITION BY minute ORDER BY frac DESC) = 1
    AND frac >= 0.90
    ORDER BY total_n DESC;
"""


detect_event = ddb.execute(sql_detect_events, [path]).df()
print(detect_event)