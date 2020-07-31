import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from h3 import h3
from shapely.geometry import Point

def hex_to_polygon(hexid):
    """Transforms single hexid to shapely hexagonal polygon
    """
    list_of_coords_list=h3.h3_to_geo_boundary(h3_address=hexid,geo_json=False)
    return Polygon([tuple(i) for i in list_of_coords_list])



def hexlist_to_geodataframe(list_hexagons):
    """Transforms a list of hex ids (h3 indexes) to GeoDataFrame"""
    df=pd.DataFrame(list_hexagons, columns=['hexid'])
    def f(x):
        #return h3.h3_to_geo_boundary(h3_address=x['hexid'],geo_json=False)
        return hex_to_polygon(x['hexid'])

    gdf = gpd.GeoDataFrame(df, geometry=df.apply(f, axis=1))
    return gdf

def df_with_hexid_to_gdf(df, hexcolname='_id'):

    """Transforms dataframe with hexid column to a geodataframe
    :param hexcolname: name of the hexid column
    :returns gdf
    """
    df_geometry=hexlist_to_geodataframe(df[hexcolname].to_list())
    #Creando el geodataframe
    gdf=gpd.GeoDataFrame(df, geometry=df_geometry['geometry'])
    gdf.crs = {'init': 'epsg:4326', 'no_defs': True}
    return gdf


def df_with_hexid_to_centroids_gdf(df, hexcolname='hexid'):

    """ Transforms dataframe with hexid column to a geodataframe with centroids as geometries
    :param hexcolname: name of the hexid column
    :returns gdf
    """
    seriesofcoordinates=df[hexcolname].apply(h3.h3_to_geo)
    geometria=seriesofcoordinates.apply(lambda row: Point(row[0],row[1]))
    gdf=gpd.GeoDataFrame(df, geometry=geometria)
    return gdf