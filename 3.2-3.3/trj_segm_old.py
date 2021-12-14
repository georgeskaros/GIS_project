import pickle
import pandas as pd
import geopandas as gpd
import numpy as np
import psycopg2
import psycopg2.extras
from matplotlib import style


style.use('ggplot')

PLT_FIG_WIDTH = 4.487
PLT_FIG_HEIGHT = PLT_FIG_WIDTH / 1.618
print('Connecting with DB...')
con = psycopg2.connect(database="sdb",
                       user="postgres",
                       password="1234",
                       host="localhost",
                       port="5433")
print('Connected')
print('Fetching main table...')
traj_sql = "SELECT * FROM trimmed limit 1000000;"
traj_gdf = gpd.GeoDataFrame.from_postgis(traj_sql, con, geom_col="geom")
# traj_gdf['ts'] = traj_gdf['ts']/1000
print('Table fetched')

print('Fetching ports')
ports_sql = "SELECT * FROM ports"
ports_gdf = gpd.GeoDataFrame.from_postgis(ports_sql, con, geom_col="geom")

ports_gdf.geom = ports_gdf.geom.to_crs({'init': 'epsg:2100'}).buffer(2000).to_crs({'init': 'epsg:4326'})
print('Ports fetched')
piraeus_port = ports_gdf[ports_gdf['port_name'] == 'Piraeus port']
piraeus_port.reset_index(drop=True, inplace=True)
# creating R-tree
sindex = traj_gdf.sindex

# variables to execute the range query on the data set
temp = piraeus_port['geom'].iloc[0]
points_within_geom = pd.DataFrame()

# R-tree search
# find the points that are inside the port of Piraeus after the given time
poss_matches_index = list(sindex.intersection(temp.bounds))
poss_matches = traj_gdf.iloc[poss_matches_index]
precise_matches = poss_matches[poss_matches.intersects(temp)]
points_within_geom = precise_matches.drop_duplicates(subset=['mmsi', 'ts'])
points_outside_geom = traj_gdf[~traj_gdf.isin(points_within_geom)].dropna(how='all')
points_in_piraeus = points_within_geom[points_within_geom['ts'] > 1519000000]
# saving fle to csv
# points_in_piraeus.to_csv(r'E:\unipi\GIS\piraeus.csv', index=False, header=True)


# create an empty DF to ad the points after the check with the ports
points_within_geometry = pd.DataFrame()

for poly in ports_gdf.geom:
    # find approximate matches with r_tree , then precise matches from those approximate ones
    possible_matches_index = list(sindex.intersection(poly.bounds))
    possible_matches = traj_gdf.iloc[possible_matches_index]
    precise_matches = possible_matches[possible_matches.intersects(poly)]
    points_within_geometry = points_within_geometry.append(precise_matches)


points_within_geometry = points_within_geometry.drop_duplicates(subset=['mmsi', 'ts'])
points_outside_geometry = traj_gdf[~traj_gdf.isin(points_within_geometry)].dropna(how='all')

traj_gdf.loc[:, 'traj_id'] = np.nan
traj_gdf.loc[traj_gdf.index.isin(points_within_geometry.index), 'traj_id'] = -1
traj_gdf.loc[traj_gdf.index.isin(points_outside_geometry.index), 'traj_id'] = 0
tmp = traj_gdf[traj_gdf.traj_id == -1]

traj_gdf.loc[:, 'label'] = traj_gdf['traj_id']
traj_gdf_trimmed = traj_gdf.groupby('mmsi', group_keys=False).apply(lambda gdf: gdf.loc[gdf.traj_id[gdf.traj_id.replace(-1, np.nan).ffill(limit=1).bfill(limit=1).notnull()].index])


traj_gdf_trimmed.reset_index(inplace=True, drop=True)

traj_segments = np.split(traj_gdf_trimmed, traj_gdf_trimmed.loc[traj_gdf_trimmed.traj_id == -1].index)
traj_segments = [df for df in traj_segments if len(df) != 0]    # remove the fragments that have 0 points
traj_segments[0].loc[:,'traj_id'] = 0

for i in range(1,len(traj_segments)):
        if (len(traj_segments[i]) == 1):
                traj_segments[i].loc[:,'traj_id'] = traj_segments[i-1].traj_id.max()
        else:
                traj_segments[i].loc[:,'traj_id'] = traj_segments[i-1].traj_id.max()+1

traj_segments_id_fix = pd.concat(traj_segments)
traj_segments_id_fix.sort_values('ts', inplace=True)
traj_segments_id_fix.reset_index(inplace=True, drop=True)
print(f'(Initial) Number of segments: {len(traj_segments)}')
print(f'(Final-Useful) Number of port-based segments produced: {len(traj_segments_id_fix["traj_id"].unique())}')

TEMPORAL_THRESHOLD = 60*60*12 # 12 hrs
CARDINALITY_THRESHOLD = 3

traj_ves_trips = []
print(f'(Initial) Number of port-based segments: {len(traj_segments_id_fix["traj_id"].unique())}')

for traj_id, sdf in traj_segments_id_fix.groupby('traj_id'):
    df = sdf.reset_index()
    break_points = df.loc[df['ts'].diff() > TEMPORAL_THRESHOLD].index

    sdfs = np.split(df, break_points)
    traj_ves_trips.extend(sdfs)

print(f'(Intermediate) Number of temporal-gap-based segments: {len(traj_ves_trips)}')

traj_ves_trips = [tmp_df for tmp_df in traj_ves_trips if len(tmp_df) >= CARDINALITY_THRESHOLD]
print(f'(Final-Useful) Number of trips produced: {len(traj_ves_trips)}')

traj_ves_trips[0].loc[:, 'trip_id'] = 0
for idx in range(1, len(traj_ves_trips)):
    traj_ves_trips[idx].loc[:, 'trip_id'] = traj_ves_trips[idx - 1].trip_id.max() + 1

traj_ves_trips_gdf = pd.concat(traj_ves_trips)
traj_ves_trips_gdf.sort_values('ts', inplace=True)
traj_ves_trips_gdf.reset_index(inplace=True, drop=True)


traj_ves_trips_gdf.drop(['geom'], axis=1).to_csv(r'E:\unipi\GIS\trips.csv', index=False, header=True)


