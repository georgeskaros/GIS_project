import pandas as pd
import numpy as np
from shapely.geometry import Point
import geopandas as gpd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler, MinMaxScaler, scale
import matplotlib.pyplot as plt
import helper
# #################
# #################TRASH
# #################
PLT_FIG_WIDTH = 4.487
PLT_FIG_HEIGHT = PLT_FIG_WIDTH / 1.618

df1 = pd.read_csv(r'D:/unipi/THESIS/TEST_PAPER/points.csv')


first_trip = df1[df1['TRIP_ID'] == 1372699407620000520]

# PCA and reconstruction of first trip with timestamp
first_trip['geom'] = first_trip[['LONGITUDE', 'LATITUDE']].apply(lambda x: Point(x[0], x[1]), axis=1)
gdf = gpd.GeoDataFrame(first_trip, geometry='geom')
helper.map_plot(gdf, figsize=(12, 8), title='First Trip')

# creating the table that is going to get reconstructed
X = first_trip[['TIMESTAMP', 'LONGITUDE', 'LATITUDE']]
# X['LATITUDE'].iloc[1] = 0
mu = np.mean(X, axis=0)
print(X)
# using pca
pca = PCA()
pca.fit(X)

# reconstructing table
nComp = 2
Xhat = np.dot(pca.transform(X)[:,:nComp], pca.components_[:nComp,:])
Xhat += mu
after_pca = pd.DataFrame(Xhat, columns=['TIMESTAMP', 'LONGITUDE', 'LATITUDE'])

# plotting reconstructed table
after_pca['geom'] = after_pca[['LONGITUDE', 'LATITUDE']].apply(lambda x: Point(x[0], x[1]), axis=1)
gdf1 = gpd.GeoDataFrame(after_pca, geometry='geom')
helper.map_plot(gdf1, figsize=(12, 8), title='Reconstruction of the fist trip')

scores = pca.transform(X)
scores_df = pd.DataFrame(scores, columns=['PC1', 'PC2', 'PC3'])
print(scores_df)

explained_variance = pca.explained_variance_ratio_
print(explained_variance)
'''

# PCA and reconstruction of first trip without timestamp 
first_trip['geom'] = first_trip[['LONGITUDE', 'LATITUDE']].apply(lambda x: Point(x[0], x[1]), axis=1)
gdf = gpd.GeoDataFrame(first_trip, geometry='geom')
helper.map_plot(gdf, figsize=(12, 8), title='First Trip')

# creating the table that is going to get reconstructed
# X = np.radians(first_trip[['LONGITUDE', 'LATITUDE']])
X = first_trip[['LONGITUDE', 'LATITUDE']]
mu = np.mean(X, axis=0)
print(X)

# using pca
pca = PCA()
pca.fit(X)

# reconstructing table
nComp = 1
Xhat = np.dot(pca.transform(X)[:,:nComp], pca.components_[:nComp,:])
Xhat += mu

# plotting reconstructed table
after_pca = pd.DataFrame(Xhat, columns=['LONGITUDE', 'LATITUDE'])
print(Xhat)
after_pca['geom'] = after_pca[['LONGITUDE', 'LATITUDE']].apply(lambda x: Point(x[0], x[1]), axis=1)
gdf1 = gpd.GeoDataFrame(after_pca, geometry='geom')
helper.map_plot(gdf1, figsize=(12, 8), title='Reconstruction of the fist trip')


scores = pca.transform(X)
scores_df = pd.DataFrame(scores, columns=['PC1', 'PC2'])

print(scores_df)

explained_variance = pca.explained_variance_ratio_
print(explained_variance)
'''


'''

# pca to all of the trips 
# Plotting the size of the trips
ps = df1.groupby(['TRIP_ID', 'CALL_TYPE']).count()
ps = ps.reset_index()

out = pd.cut(ps.LONGITUDE, [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 120, 130, 140, 150, 160, 170, 180, 190, 250, 350, 450, ps.LONGITUDE.max()])
ax = out.value_counts(sort=False).plot.bar(figsize=(PLT_FIG_WIDTH, PLT_FIG_HEIGHT), fontsize=8, width=0.75, cmap='tab20', rot=40)

plt.suptitle(r'size of trip ', fontsize=30, y=1)
plt.xlabel(r'#number of signals ', fontsize=15)
plt.ylabel(r'#number of taxi cars', fontsize=15)
plt.show()

# Filtering the trips to exclude too small or too long trips
filter = ps[(ps['LONGITUDE'] > 4) & (ps['LONGITUDE'] < 480)].index
ps.drop(filter, inplace=True)
df1 = df1[~df1.TRIP_ID.isin(ps.TRIP_ID)]


all_PCA_scores = pd.DataFrame()
df_explained_all = pd.DataFrame(columns=['PC1', 'PC2', "PC3"])
for trip in df1.TRIP_ID.unique():
    # take each trip
    temp = df1[df1['TRIP_ID'] == trip]

    # Tried to test pca with and without timestamp
    # Timestamp had better comulative variance
    # X = np.radians(temp[['TIMESTAMP', 'LONGITUDE', 'LATITUDE']])
    X = temp[['TIMESTAMP', 'LONGITUDE', 'LATITUDE']]
    X = scale(X)
    pca = PCA(n_components=3)
    pca.fit(X)

    scores = pca.transform(X)
    scores_df = pd.DataFrame(scores, columns=['PC1', 'PC2', "PC3"])
    all_PCA_scores = all_PCA_scores.append(scores_df)

    # taking the explained variance from every pc
    explained_variance = pca.explained_variance_ratio_
    explained_variance = np.round(explained_variance, decimals=5)
    # putting the pc in an dataframe
    df_explained = pd.DataFrame(data=[explained_variance], columns=['PC1', 'PC2', "PC3"])
    # adding pc of the dataframe to the sum of pc

    df_explained.loc[:, 'TRIP_ID'] = str(trip)
    df_explained = df_explained.set_index(['TRIP_ID'])
    df_explained_all = df_explained_all.append(df_explained)



print(all_PCA_scores.head(10))
print(df_explained_all.head(10))

# calculate the comulative values
df_explained_all.loc[:, 'COMULATIVE OF PC1+PC2'] = df_explained_all['PC1']+df_explained_all['PC2']

print(df_explained_all['PC1'].mean())
print(df_explained_all['COMULATIVE OF PC1+PC2'].mean())


all_PCA_scores.to_csv(r'D:/unipi/THESIS/TEST_PAPER/pca_points.csv', index=True, header=True)

'''
