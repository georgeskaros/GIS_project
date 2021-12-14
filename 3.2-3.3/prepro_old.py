import pickle
import pandas as pd
import geopandas as gpd
import numpy as np
import psycopg2
import psycopg2.extras
from IPython import get_ipython
from tqdm import tqdm
from datetime import datetime
from shapely.geometry import Point, Polygon
import os, sys
import matplotlib.pyplot as plt
from matplotlib import style
from bokeh.plotting import figure, output_file, reset_output, output_notebook, save, show
from bokeh.models import ColumnDataSource, HoverTool, WheelZoomTool
import preprocessing_old as gspp

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
print('Fetching table...')
traj_sql = "SELECT * FROM all_data;"
traj_gdf = gpd.GeoDataFrame.from_postgis(traj_sql, con, geom_col="geom")
traj_gdf['ts'] = traj_gdf['ts']/1000
print('Table fetched')

print('Calculating sampling rate')
samp_rate = traj_gdf.copy().groupby('mmsi', group_keys=False).apply(lambda gdf: gspp.calculate_sampling_rate(gdf))['time_diff']
traj_gdf.loc[:, 'time_diff'] = samp_rate
traj_gdf.dropna(subset=['time_diff'], inplace=True)

out = pd.cut(traj_gdf.time_diff, [0, 4, 6, 8, 10, 12, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, traj_gdf.time_diff.max()])
ax = out.value_counts(sort=False).plot.bar(figsize=(PLT_FIG_WIDTH, PLT_FIG_HEIGHT), fontsize=8, width=0.75, cmap='tab20', rot=40)

plt.suptitle(r'Emission rate of the AIS antennas ', fontsize=8, y=1)
plt.xlabel(r'#Signals emitted per second', fontsize=8)
plt.ylabel(r'#vessels', fontsize=8)
plt.show()

# Remove duplicate points
traj_gdf.drop_duplicates(subset=['ts', 'mmsi'], inplace=True)
print('Preprocessing initiated')
print('Step 1. Calculating Speed')
# Calculate speed
calc_velocity = traj_gdf.copy().groupby('mmsi', group_keys=False).apply(lambda gdf: gspp.calculate_velocity(gdf))['velocity']

print ('Step 2. Calculating Bearing')
# Calculate bearing
calc_heading = traj_gdf.copy().groupby('mmsi', group_keys=False).apply(lambda gdf: gspp.calculate_bearing(gdf))['bearing']

print ('Step 3. Concatenating Results')
traj_gdf.loc[:, 'velocity'] = calc_velocity
traj_gdf.loc[:, 'bearing'] = calc_heading

print ('Step 4. Calculating Acceleration')
# Calculate acceleration"
traj_gdf = traj_gdf.groupby('mmsi', group_keys=False).apply(lambda gdf: gspp.calculate_acceleration(gdf))

# Drop NaN values (in case they exist)
traj_gdf.dropna(subset=['velocity', 'bearing', 'acceleration'], inplace=True)

# Drop speed column
traj_gdf.drop('speed', axis=1)

# Drop values from df where velocity and acceleration is too high
print ('Step 5. Filtering results for outliers')
# Filter velocity
traj_gdf = traj_gdf[traj_gdf.velocity < 60]
# Filter acceleration
traj_gdf = traj_gdf[traj_gdf.acceleration < 10]
traj_gdf = traj_gdf[traj_gdf.acceleration > -10]


out = pd.cut(traj_gdf.velocity, [0, 5, 10, 15, 20, 25, 30, np.round(traj_gdf.velocity.max())+1, np.round(traj_gdf.velocity.max())+3])
ax = out.value_counts(sort=False).plot.area(figsize=(PLT_FIG_WIDTH, PLT_FIG_HEIGHT), fontsize=8, cmap='tab20', rot=0)
ax.set_xticklabels([int(c.left) for c in out.cat.categories])

plt.suptitle(r'Vessel speed distribution', fontsize=8, y=1)
plt.xlabel(r'speed (knots)', fontsize=8)
plt.ylabel(r'#records', fontsize=8)
plt.show()
# plt.savefig(os.path.join('.', 'png', 'Vessel_Velocity_Distribution_V2.png'), dpi=300, bbox_inches='tight')

no_of_bins = [np.round(traj_gdf.acceleration.min()), -5, -2, -0.5, -0.25, -0.1, -0.04, 0, 0.1, 0.25, 0.5, 2, 5, np.round(traj_gdf.acceleration.max())]
out = pd.cut(traj_gdf.acceleration, no_of_bins)
ax = out.value_counts(sort=False).plot.area(figsize=(PLT_FIG_WIDTH, PLT_FIG_HEIGHT), fontsize=8, cmap='tab20', rot=20)

# plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
plt.suptitle(r'Vessel acceleration distribution', fontsize=8)
plt.xlabel(r'acceleration (knots/s)', fontsize=8)
plt.ylabel(r'#records', fontsize=8)
plt.show()
print('Exporting csv file ')

traj_gdf.drop(['geom'], axis=1).to_csv(r'E:\unipi\GIS\all_data.csv', index=False, header=True)







