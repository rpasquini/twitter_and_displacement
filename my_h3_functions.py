import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from h3 import h3
from shapely.geometry import Point

def hex_to_polygon(hexid):
    """Transforms single hexid to shapely hexagonal polygon
    """
    list_of_coords_list=h3.h3_to_geo_boundary(hexid,geo_json=True)
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
    gdf.crs = 'EPSG:4326'
    return gdf


def df_with_hexid_to_centroids_gdf(df, hexcolname='hexid'):

    """ Transforms dataframe with hexid column to a geodataframe with centroids as geometries
    :param hexcolname: name of the hexid column
    :returns gdf
    """
    seriesofcoordinates=df[hexcolname].apply(h3.h3_to_geo)
    geometria=seriesofcoordinates.apply(lambda row: Point(row[1],row[0]))  ## Patty reversed indices
    gdf=gpd.GeoDataFrame(df, geometry=geometria)
    return gdf


def kring_smoothing(df, hex_col, metric_col, k):
    dfk = df[[hex_col]]
    dfk.index = dfk[hex_col]
    dfs =  (dfk[hex_col]
                 .apply(lambda x: pd.Series(list(h3.k_ring(x,k)))).stack()
                 .to_frame('hexk').reset_index(1, drop=True).reset_index()
                 .merge(df[[hex_col,metric_col]]).fillna(0)
                 .groupby(['hexk'])[[metric_col]].sum().divide((1 + 3 * k * (k + 1)))
                 .reset_index()
                 .rename(index=str, columns={"hexk": hex_col}))
    dfs['lat'] = dfs[hex_col].apply(lambda x: h3.h3_to_geo(x)[0])
    dfs['lng'] = dfs[hex_col].apply(lambda x: h3.h3_to_geo(x)[1])
    return dfs



def kring_smoother(hexgdf, metric_col='totalpobl', hexcolname='hexid'):

    """ Applies a Kring smoother to hex dataframe, returns a gdf ready to plot
    :param hexgdf: name of hex level geodataframe
    :param metric_col: column to be smooth
    :return: Hex gdf with smoothed column
    """""
    smooth_df=kring_smoothing(hexgdf, hexcolname, metric_col=metric_col, k=2)
    smooth_df2=pd.read_json(smooth_df.to_json(orient='records'), orient='records') #wrap> reconstructing the dataframe to avoid unkown bug with kring_smoothing
    hexsmoothgdf=df_with_hexid_to_gdf(smooth_df2, hexcolname=hexcolname)

    return hexsmoothgdf
