__author__ = 'Ricardo Pasquini'

#individual dataframe
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape
import matplotlib.pyplot as plt
import numpy as np

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
#barrios = gpd.read_file(currentdir+"/data/barrios_badata.shp")


crs_ciudad={'proj': 'tmerc',
 'lat_0': -34.6297166,
 'lon_0': -58.4627,
 'k': 0.999998,
 'x_0': 100000,
 'y_0': 100000,
 'ellps': 'intl',
 'units': 'm',
 'no_defs': True}


def df_to_gdf(input_df, crs=4326, lon='lon', lat='lat'):
    """
    Convert a DataFrame with lon  and lat as columns to GeoDataFrame.

    Following options are useful if changing the names of the latitude and longitude fields:
    lon: name of longitude field
    lat: name of latitude field
    """
    df = input_df.copy()

    geometry = [Point(xy) for xy in zip(df[lon], df[lat])]
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    # reproject
    gdf.crs = {'init': 'epsg:4326'}
    # gdf=gdf.loc[(gdf['lon']>-59) & (gdf['lon']<-58) & (gdf['lat']>-35) & (gdf['lat']<-34)]
    # gdf=gdf.to_crs(crs_ciudad)
    return gdf

def muestraprecisionproyeccionCABA():
    #Prueba para ver a cuanto equivalen la diferencia de  grados en la proyeccion de CABA
    # Basicamente muestro que 1 grado son 11092 metros, osea 0.001 grados son 11.09 metros, que es la precision maxima que voy a usar

    testdf=pd.DataFrame.from_dict({'lat':[34.000000,34.000001,34.00001,34.0001,34.001,34.01,34.1],'lon':[-58,-58,-58,-58,-58,-58,-58]})
    testdf=df_to_gdf(testdf).to_crs({'proj': 'tmerc',
     'lat_0': -34.6297166,
     'lon_0': -58.4627,
     'k': 0.999998,
     'x_0': 100000,
     'y_0': 100000,
     'ellps': 'intl',
     'units': 'm',
     'no_defs': True})
    for i in range(1,7):
        print(testdf.iloc[0].geometry.distance(testdf.iloc[i].geometry))
        #.iloc[0]


class Homelocation:
    "Stores home location algorithm results"

    # EL OUTPUT RELEVANTE ES ['distanciaatipica','distalcentroide_estandar','desvio_MEAN_distancias']

    def __init__(self):
        self.completed = False
        self.reason = 'unknown'

    def loadresults(self, gdfi, freqdfi, homecoordinates, workcoordinates):
        self.gdfi = gdfi
        self.freqdfi = freqdfi
        self.homecoordinates = homecoordinates
        self.workcoordinates = workcoordinates
        self.completed = True

    def tweetsfromhome(self):
        return self.gdfi[
            (self.gdfi['latr'] == self.homecoordinates['latr']) & (self.gdfi['lonr'] == self.homecoordinates['lonr'])]

    def tweetsfromwork(self):
        return self.gdfi[
            (self.gdfi['latr'] == self.workcoordinates['latr']) & (self.gdfi['lonr'] == self.workcoordinates['lonr'])]


def findhome(db, uid, method='latlon', map=True, dataformat='raw'):
    """
    Finds home for user id.

    :param db: mongo database connection
    :param uid: user id
    :return: Homelocation class element. Contains georeferenced tweets, frequency table and home coordinates
    """
    dfi = pd.DataFrame(list(db.tweets.find({'u_id': uid})))
    #print(dfi)

    if dataformat!='raw':
        # si la data no esta en formato raw, es porque esta en formato geolocation de mongo, y entonces tengo que hacer el unfold
        # in the new data it is necessary to unfold the json containing coordinates into columns
        dfi = pd.concat([dfi, dfi.location.apply(lambda x: x['coordinates'][0]).rename('lon'),
                         dfi.location.apply(lambda x: x['coordinates'][1]).rename('lat')], axis=1)

    # Adding hex resolution 9 to dfi
    dfi = pd.concat([dfi, dfi.hex.apply(lambda x: x['9']).rename('hex9')], axis=1)

    if dfi.shape[0] > 30:

        # 1. Adding relevant info to dfi
        dfi['hour'] = pd.to_datetime(dfi['created_at'] // 1000, unit='s').dt.hour
        # nighttime dummy
        dfi['night'] = (dfi['hour'] < 7) | (dfi['hour'] > 22)
        dfi['dayofweek'] = pd.to_datetime(dfi['created_at'] // 1000, unit='s').dt.dayofweek
        # dayofweekdummy
        # Monday=0, Sunday=6. so weekend is 5 or 6
        dfi['weekend'] = (dfi['dayofweek'] == 5) | (dfi['dayofweek'] == 6)

        # 2. Frequency aggregation

        if method == 'hex9':

            spatialgroup = ["hex9"]

        else:  # method='latlon':

            # method='latlon' computes frequency on rounded coordinates
            # this is a critical step, which imposes that coordinates precision in degress will be up to the second decimal (equivalent to 110 meters in CABA proyection)
            rounded_coordinates = dfi[['lat', 'lon']].round({'lat': 2, 'lon': 2}).rename(
                columns={'lat': 'latr', 'lon': 'lonr'})
            dfi = pd.concat([dfi, rounded_coordinates], axis=1)

            spatialgroup = ["latr", "lonr"]

        # freqdfi is dataframe at the location level aimed to counts tweets by location.
        freqdfi = dfi.groupby(spatialgroup).size().reset_index(name="freq").sort_values(by=['freq'], ascending=False)

        rangedfi = pd.concat([dfi.groupby(spatialgroup)['hour'].agg({'hourrange': lambda x: x.max() - x.min()})],
                             axis=1)

        nightdf = dfi.loc[dfi['night'] == True].groupby(spatialgroup).size().reset_index(name="night_freq").sort_values(
            by=["night_freq"], ascending=False)

        weekenddf = dfi.loc[dfi['weekend'] == True].groupby(spatialgroup).size().reset_index(
            name="weekend_freq").sort_values(by=["weekend_freq"], ascending=False)

        uniquehours = dfi.groupby(spatialgroup)['hour'].nunique().reset_index(name="uniquehours")

        freqdfi = pd.merge(freqdfi, uniquehours, how='left', left_on=spatialgroup, right_on=spatialgroup)
        freqdfi = pd.merge(freqdfi, rangedfi, how='left', left_on=spatialgroup, right_on=spatialgroup)
        freqdfi = pd.merge(freqdfi, nightdf, how='left', left_on=spatialgroup, right_on=spatialgroup)
        freqdfi = pd.merge(freqdfi, weekenddf, how='left', left_on=spatialgroup, right_on=spatialgroup)

        freqdfi.loc[freqdfi['night_freq'].isna(), 'night_freq'] = 0
        freqdfi.loc[freqdfi['weekend_freq'].isna(), 'weekend_freq'] = 0

        # Distances computation. In case the method is lat lon, it also retrieves distances between most frequent coordinate and the following

        if method == 'latlon':
            freqdfi = df_to_gdf(freqdfi, lon='lonr', lat='latr')  # requires lat and lon coordinates
            freqdfi['distance'] = [freqdfi.iloc[0].geometry.distance(freqdfi.iloc[i].geometry) * 11092.82 for i in
                                   range(0, freqdfi.shape[0])]

        ############################################
        # Candidates selection

        # 1) seleccion ubicaciones con una frecuencia atipicamente alta: el porcentaje de tweets es atipicamente alto.

        # cuanta importancia representa en relacion a la ubicacion más frecuente
        freqdfi['freqp1'] = freqdfi['freq'] / freqdfi['freq'].iloc[0]

        freqdfi = freqdfi.loc[freqdfi['freqp1'] > 0.1]

        ############################################
        # Home location criteria

        # 2) entre estos, la casa es la que tiene alta frecuencia durante la noche y ademas el fin de semana
        # el trabajo es el que no tiene frecuencia durante la noche y tiene frecuencia en el horario laboral.

        # pocentaje durante la noche
        freqdfi['pnight_freq'] = freqdfi['night_freq'] / freqdfi['freq']

        # porcentaje de fin de semana
        freqdfi['pweekend_freq'] = freqdfi['weekend_freq'] / freqdfi['freq']

        freqdfi['interactnightyweekend'] = freqdfi['pnight_freq'] * freqdfi['pweekend_freq']

        # busco la maxima
        homecoordinates = freqdfi.sort_values(by=['interactnightyweekend'], ascending=False).iloc[0]

        ############################################
        # Work criteria
        # work here is any place where the person goes frequently outside his/her home. Could be school, university etc
        # between the candidate locations is the one that maximizes day and week

        freqdfi['pday_freq'] = 1 - freqdfi['pnight_freq']
        freqdfi['pweekday_freq'] = 1 - freqdfi['pweekend_freq']
        freqdfi['interactdayyweekday'] = freqdfi['pday_freq'] * freqdfi['pweekday_freq']

        # elimino la fila de homecoordinates y luego maximizo dia y weekday
        try:
            workcoordinates = \
            freqdfi.drop([homecoordinates.name]).sort_values(by=['interactdayyweekday'], ascending=False).iloc[0]
        except IndexError:
            homeresults = Homelocation()
            homeresults.reason = 'No work coordinates'
            return homeresults

        gdfi = df_to_gdf(dfi, crs='+init=epsg:4326')
        gdfi = gdfi.to_crs(crs_ciudad)

        if map == True:
            # plt.gca().patch.set_facecolor('white')
            # plt.rcParams['figure.facecolor'] = 'white'
            # fig = plt.figure()
            # fig.patch.set_facecolor('white')
            plt.rcParams['figure.figsize'] = [10, 10]  # this sets the size of the figure

            Provincia = gpd.read_file(currentdir+"/data/Provincia_2010.shp")
            base = Provincia.to_crs(crs_ciudad).plot(markersize=6, color="gray", alpha=0.2, edgecolor='white',
                                                     linewidth=4)

            minx, miny, maxx, maxy = gdfi.total_bounds
            minx = minx - 10000
            miny = miny - 10000
            maxx = maxx + 10000
            maxy = maxy + 10000
            base.set_xlim(minx, maxx)
            base.set_ylim(miny, maxy)
            # rdf.plot(ax=base,color='blue', markersize=5)

            gdfi.plot(ax=base)

            print('Home in red')
            print('Notice than more than one point might result in red due to coordinates precision')

            gdfi[(gdfi['latr'] == homecoordinates['latr']) & (gdfi['lonr'] == homecoordinates['lonr'])].plot(ax=base,
                                                                                                         color='red')

            # mapping points with home coordinates
            gdfi['home'] = 0
            gdfi.loc[(gdfi['latr'] == homecoordinates['latr']) & (gdfi['lonr'] == homecoordinates['lonr']), 'home'] = 1

        homeresults = Homelocation()
        homeresults.loadresults(gdfi, freqdfi, homecoordinates, workcoordinates)

    else:

        homeresults = Homelocation()
        homeresults.reason = 'less than 30 tweets'

    return homeresults



def timeofdayplot(db,uid):
    """
    Plots coordinates to time of day
    :param db:
    :param uid:
    :return:
    """
    import matplotlib.pyplot as plt
    plt.rcParams['figure.figsize'] = [5, 5] #this sets the size of the figure

    dfi=pd.DataFrame(list(db.tweets.find({'u_id':uid})))
    dfi = pd.concat([dfi, dfi.location.apply(lambda x: x['coordinates'][0]).rename('lon'),
                     dfi.location.apply(lambda x: x['coordinates'][1]).rename('lat')], axis=1)
    dfi['hour']=pd.to_datetime(dfi['created_at'] // 1000, unit='s').dt.hour
    x=pd.to_datetime(dfi['created_at'] // 1000, unit='s').dt.hour
    lat=dfi.lat
    plt.scatter(x,lat)
    plt.xlabel('hour', fontsize=16)
    plt.ylabel('lat', fontsize=16)
    plt.show()

    ax=dfi['lat'].hist()
    plt.xlabel("lat")
    plt.show()

    lon=dfi.lon
    plt.scatter(x,lon)
    plt.xlabel('hour', fontsize=16)
    plt.ylabel('lon', fontsize=16)
    plt.show()

    ax=dfi['lon'].hist()
    plt.xlabel("lon")
    plt.show()


##########The following are db related functions and the iteration jobs

def updatehomelocation(db, uid, homedata):
    "Updates home location in users collection. Upsert function"
    """
    :param uid: user id
    :param homedata: home data json

    """

    query = {'u_id': uid}
    newvalues = {'$set': homedata}

    db.users.update(query, newvalues, upsert=True)

def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        new[key1] = val1

    return new

def findhomeandpopulate(uid, db, method='latlon'):
    "Find home for user id and populate users with result function"

    if method == 'hex9':
        result = findhome(db=db, uid=uid, method='hex9', map=False)
    else:
        result = findhome(db=db, uid=uid, map=False)

    if result.completed is not False:
        homedata = result.homecoordinates.to_dict()

        if method == 'latlon':  # only w/homelocation latlon method delete geometry field
            del homedata['geometry']
            homedata = correct_encoding(homedata)
            homedata2 = homedata.copy()
            del homedata2['latr']
            del homedata2['lonr']
            dicttopopulate = {'home': {'home_stats': homedata2, 'location': {'type': "Point",
                                                                             'coordinates': [homedata['lonr'],
                                                                                             homedata['latr']]}}}
            updatehomelocation(db=db, uid=uid, homedata=dicttopopulate)
            # print(dicttopopulate)

        if method == 'hex9':
            homedata = correct_encoding(homedata)
            dicttopopulate = {'hex9': homedata}
            updatehomelocation(db=db, uid=uid, homedata=dicttopopulate)
            # print(dicttopopulate)



def job_findhomeandpopulate_hex9(db):

    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)

    """Iteration over all users that do not have hex9
    Find home and populate hex9"""
    print('Pending users to process...', db.users.count_documents({'hex9': { '$exists': False } }))

    cursorpendientes=db.users.find( { 'hex9': { '$exists': False } } )
    j=1
    for doc in cursorpendientes:
        uid=doc['u_id']
        findhomeandpopulate(uid=uid, db=db, method='hex9')

        if (j/50).is_integer(): #printing each 10 documents
            print('iter:',j)
        j=j+1





