import duckdb as ddb
import matplotlib.pyplot as plt
import pandas as pd

path = "/Users/Justin/Downloads/2022_place_canvas_history5.parquet"

#For fig 1
# sql_minute = """
# WITH per_min_color AS (
#   SELECT
#     date_trunc('minute', timestamp) AS minute,
#     upper(pixel_color) AS pixel_color,
#     count(*) AS n
#   FROM read_parquet(?)
#   GROUP BY minute, pixel_color
# ),
# per_min_total AS (
#   SELECT minute, sum(n) AS total_n
#   FROM per_min_color
#   GROUP BY minute
# ),
# dist AS (
#   SELECT
#     c.minute, c.pixel_color, c.n, t.total_n,
#     (c.n::DOUBLE / t.total_n) AS frac
#   FROM per_min_color c
#   JOIN per_min_total t USING (minute)
# ),
# per_min_summary AS (
#   SELECT
#     minute,
#     total_n,
#     arg_max(pixel_color, frac) AS dominant_color,
#     max(frac) AS dominant_frac,
#     sum(CASE WHEN pixel_color = '#FFFFFF' THEN frac ELSE 0 END) AS white_fraction
#   FROM dist
#   GROUP BY minute, total_n
# )
# SELECT *
# FROM per_min_summary
# WHERE total_n >= 10000
# ORDER BY minute;
# """

# df = ddb.execute(sql_minute, [path]).df()

# fig, ax1 = plt.subplots()
# ax1.plot(df["minute"], df["white_fraction"])
# ax1.set_ylabel("white_fraction")

# ax2 = ax1.twinx()
# ax2.plot(df["minute"], df["total_n"])
# ax2.set_ylabel("total_n per minute")

# ax1.set_xlabel("minute")
# plt.title("Figure 1: White fraction and total placements per minute")
# plt.tight_layout()
# plt.show()


#For fig 3
# sql_detect_bot = """
# WITH u AS (
#   SELECT
#     user_id,
#     timestamp,
#     epoch(timestamp) AS t,
#     date_part('hour', timestamp) AS hr
#   FROM read_parquet(?)
# ),
# d AS (
#   SELECT
#     user_id,
#     hr,
#     t - lag(t) OVER (PARTITION BY user_id ORDER BY t) AS dt_sec
#   FROM u
# )
# SELECT
#   user_id,
#   count(*) AS placements,
#   count(DISTINCT hr) AS active_hours,                  
#   stddev_samp(dt_sec) AS dt_stddev_sec,
#   avg(dt_sec) AS dt_avg_sec,
#   quantile_cont(dt_sec, 0.50) AS dt_p50_sec,
#   quantile_cont(dt_sec, 0.90) AS dt_p90_sec
# FROM d
# WHERE dt_sec IS NOT NULL AND dt_sec > 0
# GROUP BY user_id
# HAVING placements >= 200
#    AND active_hours >= 20                            
#    AND dt_stddev_sec IS NOT NULL AND dt_stddev_sec <= 120
# ORDER BY active_hours DESC, dt_stddev_sec ASC, placements DESC;
# """

# detect_burst = ddb.execute(sql_detect_bot, [path]).df()


# plt.figure()
# plt.scatter(detect_burst["dt_avg_sec"], detect_burst["dt_stddev_sec"])
# plt.xlabel("Average seconds between placements")
# plt.ylabel("Timing jitter (seconds)")
# plt.title("Bot suspects: stable timing vs jitter")
# plt.tight_layout()
# plt.show()


#fig 7
# sql_admin = """
# SELECT coordinate
# FROM read_parquet(?)
# WHERE length(coordinate) - length(replace(coordinate, ',', '')) = 3
# """

# admin = ddb.execute(sql_admin, [path]).df()

# def rect_area(coord: str) -> int:
#     x1, y1, x2, y2 = [int(s) for s in coord.split(",")]
#     xmin, xmax = sorted([x1, x2])
#     ymin, ymax = sorted([y1, y2])
#     return (xmax - xmin + 1) * (ymax - ymin + 1)

# admin["area"] = admin["coordinate"].apply(rect_area)

# plt.figure()
# plt.hist(admin["area"], bins=50)
# plt.xlabel("rectangle area (pixels)")
# plt.ylabel("count")
# plt.title("Admin rectangle draw sizes")
# plt.tight_layout()
# plt.savefig("admin_rectangle_area_hist.png", dpi=200)
# plt.show()

#Fig 8
# sql = """
# SELECT
#   date_trunc('minute', timestamp) AS minute,
#   count(*) AS admin_actions
# FROM read_parquet(?)
# WHERE length(coordinate) - length(replace(coordinate, ',', '')) = 3
# GROUP BY minute
# ORDER BY minute;
# """

# df = ddb.execute(sql, [path]).df()

# plt.figure(figsize=(10,4))
# plt.plot(df["minute"], df["admin_actions"])
# plt.xlabel("minute")
# plt.ylabel("admin actions")
# plt.title("Admin rectangle actions over time")
# plt.tight_layout()
# plt.savefig("admin_actions_timeseries.png", dpi=250)
# plt.show()
