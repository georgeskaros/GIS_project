import pickle
import pandas as pd
import geopandas as gpd
import numpy as np
import psycopg2
import psycopg2.extras
from sklearn.cluster import DBSCAN
from sklearn.cluster import OPTICS
import helper
# TODO make some plots

print('Connecting with DB...')
con = psycopg2.connect(database=    "sdb",
                       user=        "postgres",
                       password=    "1234",
                       host=        "localhost",
                       port=        "5433")
print('Connected')
print('Fetching main table...')
traj_sql = "SELECT * FROM curated limit 60000;"
traj_gdf = gpd.GeoDataFrame.from_postgis(traj_sql, con, geom_col="geom")

con.close()
print('Table fetched')


X = np.radians(traj_gdf[['lat', 'lon']])
print("Starting dbscan")
db = DBSCAN(eps=1/6371, min_samples=len(traj_gdf)//50, algorithm='ball_tree', metric='haversine').fit(X)
print('Finding labels of clusters')
set(db.labels_)
print(f'We have{len(np.unique(db.labels_))} : {np.unique(db.labels_)} clusters')
cent = helper.get_clusters_centers(X, db.labels_)
print(cent)

print("Starting OPTICS")
optics = OPTICS(max_eps=1/6371, min_samples=len(traj_gdf)//50, metric='haversine').fit(X)
print('Finding labels of clusters')
set(optics.labels_)
print(f'We have{len(np.unique(optics.labels_))} : {np.unique(optics.labels_)} clusters')
opt = helper.get_clusters_centers(X, optics.labels_)
print(opt)


