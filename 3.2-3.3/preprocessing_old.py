
import pandas as pd
import geopandas as gpd
import numpy as np
from haversine import haversine
from shapely.geometry import Point,Polygon
pd.options.mode.chained_assignment = None 


def getGeoDataFrame_v2(df, coordinate_columns=['lon', 'lat'], crs={'init':'epsg:4326'}):
    '''
        Create a GeoDataFrame from a DataFrame in a much more generalized form.
    '''

    df.loc[:, 'geom'] = np.nan
    df.geom = df[coordinate_columns].apply(lambda x: Point(*x), axis=1)

    return gpd.GeoDataFrame(df, geometry='geom', crs=crs)
    

def haversine(p_1, p_2):
    lon1, lat1, lon2, lat2 = map(np.deg2rad, [p_1[0], p_1[1], p_2[0], p_2[1]])   
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1    
    a = np.power(np.sin(dlat * 0.5), 2) + np.cos(lat1) * np.cos(lat2) * np.power(np.sin(dlon * 0.5), 2)    
    
    return 2 * 6371.0088 * np.arcsin(np.sqrt(a))
    
    
def calculate_acceleration(gdf):
    '''
    Return given dataframe with an extra acceleration column that
    is calculated using the rate of change of velocity over time.
    '''
    # if there is only one point in the trajectory its acceleration will be zero (i.e. constant speed)
    if len(gdf) == 1:
        gdf.loc[:, 'acceleration'] = 0
        return gdf
    
    gdf.loc[:, 'acceleration'] = gdf.velocity.diff(-1).divide(gdf.ts.diff(-1).abs())
    gdf = gdf.loc[gdf['mmsi'] != 0]
    gdf.dropna(subset=['mmsi', 'geom'], inplace=True)
    
    return gdf


def calculate_sampling_rate(gdf):
    gdf.loc[:, 'time_diff'] = gdf.ts.diff(-1).abs()
    gdf.dropna(subset=['time_diff'], inplace=True)

    return gdf


def calculate_velocity(gdf):
    '''
    Return given dataframe with an extra velocity column that 
    is calculated using the distance covered in a given amount of time.
    TODO - use the get distance method to save some space
    '''
    # if there is only one point in the trajectory its velocity will be the one measured from the speedometer
    if len(gdf) == 1:
        gdf.loc[:, 'velocity'] = gdf.speed
        return gdf

    # create columns for current and next location. Drop the last columns that contains the nan value
    gdf.loc[:, 'current_loc'] = gdf.geom.apply(lambda x: (x.x,x.y))
    gdf.loc[:, 'next_loc'] = gdf.geom.shift(-1)
    gdf.loc[:, 'dt'] = gdf.ts.diff(-1).abs()
    
    gdf = gdf.iloc[:-1]
    gdf.next_loc = gdf.next_loc.apply(lambda x : (x.x,x.y)) 
        
    # get the distance traveled in n-miles and multiply by the rate given (3600/secs for knots)
    gdf.loc[:, 'velocity'] = gdf[['current_loc', 'next_loc']].apply(lambda x: haversine(x[0], x[1])*0.539956803, axis=1).multiply(3600/gdf.dt)

    gdf.drop(['current_loc', 'next_loc', 'dt'], axis=1, inplace=True)
    gdf = gdf.loc[gdf['mmsi'] != 0]
    gdf.dropna(subset=['mmsi', 'geom'], inplace=True)
    
    return gdf
    

def calculate_angle(point1, point2):
    '''
        Calculating initial bearing between two points
    '''
    lon1, lat1 = point1[0], point1[1]
    lon2, lat2 = point2[0], point2[1]

    dlat = (lat2 - lat1)
    dlon = (lon2 - lon1)
    numerator = np.sin(dlon) * np.cos(lat2)
    denominator = (
        np.cos(lat1) * np.sin(lat2) -
        (np.sin(lat1) * np.cos(lat2) * np.cos(dlon))
    )

    theta = np.arctan2(numerator, denominator)
    theta_deg = (np.degrees(theta) + 360) % 360
    return theta_deg


def calculate_bearing(gdf):
    '''
    Return given dataframe with an extra bearing column that
    is calculated using the course over ground (in degrees in range [0, 360))
    '''
    # if there is only one point in the trajectory its bearing will be the one measured from the accelerometer
    if len(gdf) == 1:
        gdf.loc[:, 'bearing'] = gdf.course
        return gdf

    # create columns for current and next location. Drop the last columns that contains the nan value
    gdf.loc[:, 'current_loc'] = gdf.geom.apply(lambda x: (x.x,x.y))
    gdf.loc[:, 'next_loc'] = gdf.geom.shift(-1)
    gdf = gdf.iloc[:-1]
    
    gdf.next_loc = gdf.next_loc.apply(lambda x : (x.x,x.y))
    
    gdf.loc[:,'bearing'] = gdf[['current_loc', 'next_loc']].apply(lambda x: calculate_angle(x[0], x[1]), axis=1)

    gdf.drop(['current_loc', 'next_loc'], axis=1, inplace=True)
    gdf = gdf.loc[gdf['mmsi'] != 0]
    gdf.dropna(subset=['mmsi', 'geom'], inplace=True)
    
    return gdf
