import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import json
import time
plt.rcParams['figure.figsize'] = [10, 10]


def read_radios_from_db(db, collectionname='radios'):
    """Takes results stored in the radios Mongo collection, prepares a gdf for analysis"""


    df = pd.DataFrame(list(db[collectionname].find()))

    # rearrange total counts
    df['totalcount'] = df.apply(lambda x: x['tweets']['totalcount'], axis=1)

    df['geometry'] = df.apply(lambda x: Polygon(x['geometry']['coordinates'][0]), axis=1)
    # gpd.GeoDataFrame({'geometry':df['geometry']})

    gdf = gpd.GeoDataFrame(df)

    # setting crs
    gdf.crs = {'init': 'epsg:4326', 'no_defs': True}

    gdf = addsurface(gdf)

    return gdf



def graph_total_counts(gdf):
    """Graphs total counts"""
    fig, ax = plt.subplots()
    base=gdf.loc[gdf['totalcount']<10000].plot(column='totalcount', ax=ax)
    base.set_xlim(-59, -58.0)
    base.set_ylim(-35, -34.25)
    plt.title("Total tweets by censal radius")


    # only the city of buenos aires. Trimming outliers above 10,000
    gdf2=gdf.copy()
    gdf2.loc[gdf2['totalcount']>=10000,'totalcount']=10000
    fig, ax = plt.subplots(1, 1)
    base=gdf2.plot(column='totalcount', ax=ax, legend=True)
    base.set_xlim(-58.55, -58.36)
    base.set_ylim(-34.7, -34.54)
    plt.title("Total tweets by censal radius. City of Buenos Aires")


def addsurface(gdf2):
    """ Add surface of polygons in km2, using the City of Buenos Aires projection"""
    gdf2.crs={'init': 'epsg:4326', 'no_defs': True}
    crs_ciudad={'proj': 'tmerc',
     'lat_0': -34.6297166,
     'lon_0': -58.4627,
     'k': 0.999998,
     'x_0': 100000,
     'y_0': 100000,
     'ellps': 'intl',
     'units': 'm',
     'no_defs': True}
    gdf2['surface_km2']=gdf2['geometry'].to_crs(crs_ciudad).map(lambda p: p.area / 10**6)
    return gdf2



def iteratorofpendinggeometries(db,collection_destination_name):

    """Returns an iterator of pending to process geometries
    When a new geometry is processed, it leaves a record in the collection_destination_name
    So the first thing will be to check which radios are not there.
    For those not in the destination collection, an interator including the radius code and the geometry in geojson format

    :param db: mongo database connection
    :return: nested json with time based aggregations

    """

    # retrieve the complete collection of radios
    gdf = read_radios_from_db(db, collectionname='radios')

    # now query the destination collection to check for pending radios
    df2= pd.DataFrame(list(db[collection_destination_name].find()))

    try: gdf['completed']=gdf['COD_2010_1'].isin(df2['COD_2010_1'])

    except KeyError:
        print("Destination collection was empty.. iterator will include all")
        gdf['completed']=False


    # recall the transformation to geojson is taken care by geopandas.__geointerface__ etc
    def f(x):
        return {'COD_2010_1': x['COD_2010_1'],
                'geometry': gpd.GeoSeries(x['geometry']).__geo_interface__['features'][0]['geometry']}

    print('Number of pending geometries...', gdf.loc[gdf['completed'] == False].shape[0])
    return iter(list(gdf.loc[gdf['completed'] == False].apply(f, axis=1)))



def  count_by_residents_and_timefreq(db, geometry, freq='Q'):

    """Process to obtain counts of residents and non-residents tweets, and related time based aggregations by censal radius

    :param geometry: geometry in geojson format
    :param freq: managed by timebasedaggregation, is the frequency of aggregation
    :param db: mongo database connection


    :return: nested json with time based aggregations

    """

    #Find users in radio and convert to df
    usersinradio = db.users.find({'location': {'$geoWithin': {'$geometry': geometry}}})
    usersinradio_df = pd.DataFrame(list(usersinradio))

    #Find tweets in radio and convert to df
    tweetsinradio = db.tweets2.find({'location': {'$geoWithin': {'$geometry': geometry}}})
    tweetsinradio = pd.DataFrame(list(tweetsinradio))

    #chequeo que haya tweets en el radio
    if tweetsinradio.shape[0]>0:

        totalcountsdict = json.loads(timebasedaggregation(tweetsinradio, 'totalcounts', frequency=freq))

        if usersinradio_df.shape[0]>0:
            tweetsinradio['userlivesinradio'] = tweetsinradio['u_id'].isin(usersinradio_df['u_id'])
            residentsdict=json.loads(timebasedaggregation(tweetsinradio.loc[tweetsinradio['userlivesinradio']==True], 'residents', frequency=freq))
            nonresidentsdict=json.loads(timebasedaggregation(tweetsinradio.loc[tweetsinradio['userlivesinradio']==False], 'nonresidents', frequency=freq))

            # aggregation of dicts
            result={**totalcountsdict, **residentsdict, **nonresidentsdict}

        else: # there are no users living in the radius
            residentsdict={'residents': {}}
            tweetsinradio['userlivesinradio'] =False
            nonresidentsdict = json.loads(timebasedaggregation(tweetsinradio.loc[tweetsinradio['userlivesinradio'] == False],
                                                    'nonresidents', frequency=freq))
            # aggregation of dicts
            result = {**totalcountsdict, **residentsdict, **nonresidentsdict}


    else:  #returns an empy dict
        result={'totalcounts': {},
         'residents': {},
         'nonresidents': {}}

    #print(result)
    return json.dumps(result)






def timebasedaggregation(df3, name, frequency='Q'):
    """Timestamp based counts

    :return Json """

    df3['timestamp'] = pd.to_datetime(df3['created_at'] // 1000, unit='s')
    df3['date'] = df3['timestamp'].dt.date
    df3['hour'] = df3['timestamp'].dt.hour
    df3.index = df3['timestamp']

    df4=pd.DataFrame(df3['timestamp'].resample(frequency).count())
    df4=df4.rename(columns={'timestamp':name})
    df4.index=df4.index.rename('hola')
    return df4.to_json()




def counterjob(db, sizeofchunk=20):

    "Counter Job. Inserts with chunks"
    starttime=time.time()
    pendingradiositerator = iteratorofpendinggeometries(db, 'radioscounts')
    therearependingjobs=True
    jobtimes=[]
    while therearependingjobs:
        sizeofchunk=sizeofchunk
        startjobtime=time.time()
        listofjobresults=[]
        for i in range(sizeofchunk):
            try: nextinlineradio=next(pendingradiositerator)
            except StopIteration:
                therearependingjobs=False
                break
            countresultsdict=json.loads(count_by_residents_and_timefreq(db, nextinlineradio['geometry']))
            countresultsdict.update({'COD_2010_1' : nextinlineradio['COD_2010_1']})
            listofjobresults.append(countresultsdict)
        db.radioscounts.insert_many(listofjobresults)
        jobtimes.append(time.time()-startjobtime)
        print("job time:",time.time()-startjobtime)

    endtime = time.time()
    print('total elapsed time ',endtime-starttime)


if __name__ == "__main__":

    import communicationwmongo as commu
    db = commu.connecttoLocaldb(database='twitter')
    counterjob(db)