import geopandas as gpd
import psycopg2.extras
import pandas as pd

print('Connecting with DB...')
con = psycopg2.connect(database="sdb",
                       user="postgres",
                       password="1234",
                       host="localhost",
                       port="5433")
print('Connected')

# getting table from DB
print('Fetching main table...')
traj_sql = "SELECT ts,mmsi ,lat,lon, trip_id,geom FROM curated ORDER BY trip_id,ts limit 60000;"
traj_gdf = gpd.GeoDataFrame.from_postgis(traj_sql, con, geom_col="geom")
print('Table fetched')
con.close()

# creating a DF to save the data after resampling
resampled_data = pd.DataFrame()
print(traj_gdf.head(10))
# start the resampling
print('Resampling is initiated')
for trip in traj_gdf.trip_id.unique():
    # take each trip
    gdf = traj_gdf[traj_gdf['trip_id'] == trip]

    # remove geom column
    tmp = gdf.drop(['geom'], axis=1)
    tmp.drop_duplicates(subset='ts', keep = False, inplace = True)
    tmp['ts'] = pd.to_datetime(tmp['ts'], unit='s')
    tmp.set_index(['ts'],inplace=True)

    # resample to a new index then interpolate each column separately
    rs = tmp.resample("2S").asfreq()
    rs['mmsi'] = rs['mmsi'].interpolate()
    rs['lat'] = rs['lat'].interpolate()
    rs['lon'] = rs['lon'].interpolate()
    rs['trip_id'] = rs['trip_id'].interpolate()
    resampled_data = resampled_data.append(rs)

print('Resampling finished')
print(resampled_data.head(10))
print('Creating CSV file')

# saving to csv file
resampled_data.dropna(subset=['trip_id','lat','lon']).to_csv(r'E:\unipi\GIS\resampled2s.csv', header=True)



