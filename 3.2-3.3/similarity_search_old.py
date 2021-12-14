import geopandas as gpd
import psycopg2.extras
from tslearn.metrics import dtw, dtw_path
import sys
# initialising some variables for later comparisons
reference_id = 1

dtw_score_latlon = sys.maxsize
dtw_score_ts = sys.maxsize
id_of_best_latlon = reference_id
id_of_best_ts = reference_id


# connecting to DB
print('Connecting with DB...')
con = psycopg2.connect(database="sdb",
                       user="postgres",
                       password="1234",
                       host="localhost",
                       port="5433")
print('Connected')

# getting table from DB
print('Fetching main table...')
traj_sql = "SELECT * FROM curated ORDER BY trip_id,ts ;"
traj_gdf = gpd.GeoDataFrame.from_postgis(traj_sql, con, geom_col="geom")
print('Table fetched')
con.close()


# initialising the trip that we are going to compare to

gdf1 = traj_gdf[traj_gdf['trip_id'] == reference_id]
lat1 = gdf1['lat']
lon1 = gdf1['lon']
ts1 = gdf1['ts']

# starting the loop, we are going to compare to each trip of the data set in order to find the one with the smallest score
for trip in traj_gdf.trip_id.unique():
    if trip == reference_id:
        continue
    gdf = traj_gdf[traj_gdf['trip_id'] == trip]
    temp_lat = gdf['lat']
    temp_lon = gdf['lon']
    temp_ts = gdf['ts']
    score_lat = dtw(lat1, temp_lat)
    score_lon = dtw(lon1, temp_lon)
    score_ts = dtw(ts1, temp_ts)
    latlon_score = score_lat + score_lon
    ts_score = latlon_score + score_ts
    # if core is less than the previous recorded sve it
    if latlon_score < dtw_score_latlon:
        dtw_score_latlon = latlon_score
        id_of_best_latlon = trip
    if ts_score < dtw_score_ts:
        dtw_score_ts = ts_score
        id_of_best_ts = trip

print(f'The best matching trip for just lat,lon is {id_of_best_latlon}')
print(f'With a score of {dtw_score_latlon}')

print(f'The best matching trip for lat,lon and time is {id_of_best_ts}')
print(f'With a score of {dtw_score_ts}')





