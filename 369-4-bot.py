import duckdb as ddb
import pandas as pd
from datetime import datetime
import time
import argparse
import os


path = "/Users/Justin/Downloads/2022_place_canvas_history5.parquet"

print("SCRIPT START")


sql_detect_bot = """
WITH u AS (
SELECT
 user_id,
 timestamp,
 epoch(timestamp) AS t,
 date_part('hour', timestamp) AS hr
 FROM read_parquet(?)
 ),
 d AS (
 SELECT
    user_id,
    hr,
    t - lag(t) OVER (PARTITION BY user_id ORDER BY t) AS dt_sec
  FROM u
  )
  SELECT
    user_id,
    count(*) AS placements,
    count(DISTINCT hr) AS active_hours,                  
    stddev_samp(dt_sec) AS dt_stddev_sec,
    avg(dt_sec) AS dt_avg_sec,
    quantile_cont(dt_sec, 0.50) AS dt_p50_sec,
    quantile_cont(dt_sec, 0.90) AS dt_p90_sec
    FROM d
    WHERE dt_sec IS NOT NULL AND dt_sec > 0
    GROUP BY user_id
    HAVING placements >= 200 
    AND active_hours >= 20                            
    AND dt_stddev_sec IS NOT NULL AND dt_stddev_sec <= 120

ORDER BY active_hours DESC, dt_stddev_sec ASC, placements DESC;
"""


detect_burst = ddb.execute(sql_detect_bot, [path]).df()
print(detect_burst)