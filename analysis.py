import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import json
import time
from h3 import h3
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





def  count_tweets_by_residents_and_timefreq(db, geometry, freq='Q'):

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




def count_users(db, geometry, freq='Q'):

    """Process to obtain counts of users by censal radius

    :param geometry: geometry in geojson format
    :param freq: managed by timebasedaggregation, is the frequency of aggregation
    :param db: mongo database connection

    :return: count in json

    """
    #Find users in radio and convert to df
    userslivinginradio = db.users.find({'home.location': {'$geoWithin': {'$geometry': geometry}}}).count()

    result = {'totalusers': userslivinginradio}

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




def counterjob(db, sizeofchunk=20, methodtorun=count_tweets_by_residents_and_timefreq, destination_collection_name='radiocounts'):

    """This function administers the implementation of methods at the geometry level. Checks which geometries are pending, and writes the resutls with chunks.
    This version proceeds in order using the iterator

    :param db: mongo database connection
    :param methodtorun: algorithm to apply to the given geometry
    :param sizeofchunk: population is done with insert_many

    :return

    """

    starttime=time.time()
    # the following creates an iterator of the geometries that were not already processed and stored in collection destination_collection_name
    pendingradiositerator = iteratorofpendinggeometries(db, destination_collection_name)
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
            countresultsdict=json.loads(methodtorun(db, nextinlineradio['geometry']))
            countresultsdict.update({'COD_2010_1' : nextinlineradio['COD_2010_1']})
            listofjobresults.append(countresultsdict)
        db[destination_collection_name].insert_many(listofjobresults)
        jobtimes.append(time.time()-startjobtime)
        print("job time:",time.time()-startjobtime)

    endtime = time.time()
    print('total elapsed time ',endtime-starttime)



def tweets_in_hex_df(db, hexid, resolution='9'):
    """
    resolution:  9 or 10 in string """

    hexfieldname_indb = 'hex.' + resolution
    columnname_indataframe = 'hex' + resolution

    tweets_in_hex_cursor = db.tweets.find({hexfieldname_indb: hexid})

    tweets_in_hex_df = pd.DataFrame(list(tweets_in_hex_cursor))

    # extract the hex9 data in the json into a new column
    tweets_in_hex_df = pd.concat([tweets_in_hex_df,
                                  tweets_in_hex_df.apply(lambda x: x['hex'][resolution], axis=1).rename(
                                      columnname_indataframe)], axis=1)

    return tweets_in_hex_df


def users_in_hex_list(db, hexid, resolution='9'):
    """
    resolution:  9 or 10 in string """

    hexfieldname_indb = 'hex' + resolution + "." + 'hex' + resolution
    users_in_hex_cursor = db.users.find({hexfieldname_indb: hexid})
    users_in_hex_df = pd.DataFrame(list(users_in_hex_cursor))
    try:
        users_in_hex_list = list(users_in_hex_df['u_id'])
    except KeyError:
        users_in_hex_list =[] #returns empty

    return users_in_hex_list


def users_in_hex_plus_neighbors_list(db, hexid, contiguity=1, resolution='9'):
    """Adding neigbors of specified contiguity using h3 ring functions

    # comment> Shouldnt be necessary to specify resolution once hexid is given> check h3 documentation to obtain resoltuion on the basis of hexid
    """

    neighboring_hex_list = list(h3.k_ring_distances(hexid, ring_size=contiguity)[contiguity])

    # funcion para graficar los poligonos
    # gdf=hexlist_to_geodataframe(neighboring_hex_list)
    # gdf.plot()

    users_in_hex_plus_neighbors_list = []

    users_in_hex_list2 = users_in_hex_list(db, hexid, resolution=resolution)

    users_in_hex_plus_neighbors_list.extend(users_in_hex_list2)  # adding first those living in hex

    hexfieldname_indb = 'hex' + resolution + "." + 'hex' + resolution

    for n_hexid in neighboring_hex_list:

        users_in_n_hex_cursor = db.users.find({hexfieldname_indb: n_hexid})
        users_in_n_hex_df = pd.DataFrame(list(users_in_n_hex_cursor))

        try:
            users_in_n_hex_list = list(users_in_n_hex_df['u_id'])
        except KeyError:
            pass
        else:
            users_in_hex_plus_neighbors_list.extend(users_in_n_hex_list)
    return users_in_hex_plus_neighbors_list


def tweets_from_hex_residents(db, hexid, resolution='9'):
    tweets_in_hex_df2 = tweets_in_hex_df(db, hexid, resolution=resolution)

    users_in_hex_list2 = users_in_hex_list(db, hexid, resolution=resolution)

    # queda pendiente separar los tweets de los residentes:
    # tweets de residentes
    tweets_from_residents = tweets_in_hex_df2[tweets_in_hex_df2['u_id'].isin(users_in_hex_list2)]

    return tweets_from_residents


def tweets_from_hex_non_residents(db, hexid, resolution='9'):
    tweets_in_hex_df2 = tweets_in_hex_df(db, hexid, resolution=resolution)

    users_in_hex_list2 = users_in_hex_list(db, hexid, resolution=resolution)

    # tweets de no residentes
    tweets_from_non_residents = tweets_in_hex_df2[~tweets_in_hex_df2['u_id'].isin(users_in_hex_list2)]

    return tweets_from_non_residents


def tweets_from_non_residents_and_non_neighbors(db, hexid, contiguity=1, resolution='9'):
    tweets_in_hex_df2 = tweets_in_hex_df(db, hexid, resolution=resolution)

    users_in_hex_list2 = users_in_hex_list(db, hexid, resolution=resolution)

    users_in_hex_plus_neighbors_list2 = users_in_hex_plus_neighbors_list(db, hexid, contiguity=1, resolution='9',db=db)

    # tweets de no residentes y no vecinos
    tweets_from_non_residents_and_non_neighbors = tweets_in_hex_df2[
        ~tweets_in_hex_df2['u_id'].isin(users_in_hex_plus_neighbors_list2)]

    return tweets_from_non_residents_and_non_neighbors




def countsby_residents_and_non_residents(db, hexid, contiguity=1, resolution='9', freq='Q'):
    """
    Counts by residents and non residents.

    including neighbors

    :param hexid:
    :param contiguity:
    :param resolution:
    :param freq:
    :return: json of counts

    """
    tweets_in_hex_df2 = tweets_in_hex_df(db, hexid, resolution=resolution)

    users_in_hex_list2 = users_in_hex_list(db, hexid, resolution=resolution)

    users_in_hex_plus_neighbors_list2 = users_in_hex_plus_neighbors_list(db, hexid, contiguity=contiguity, resolution='9')

    if tweets_in_hex_df2.shape[0] > 0:

        totalcountsdict = json.loads(timebasedaggregation(tweets_in_hex_df2, 'totalcounts', frequency=freq))

        if len(users_in_hex_list2) > 0:  # therefore there are residents

            tweets_from_residents2 = tweets_in_hex_df2[tweets_in_hex_df2['u_id'].isin(users_in_hex_list2)]
            tweets_from_non_residents2 = tweets_in_hex_df2[~tweets_in_hex_df2['u_id'].isin(users_in_hex_list2)]
            tweets_from_non_residents_and_non_neighbors2 = tweets_in_hex_df2[
                ~tweets_in_hex_df2['u_id'].isin(users_in_hex_plus_neighbors_list2)]

            residentsdict = json.loads(timebasedaggregation(tweets_from_residents2, 'residents', frequency=freq))
            nonresidentsdict = json.loads(
                timebasedaggregation(tweets_from_non_residents2, 'nonresidents', frequency=freq))
            nonresidentsandnonneighborsdict = json.loads(
                timebasedaggregation(tweets_from_non_residents_and_non_neighbors2, 'nonresidentsandnonneighbors',
                                     frequency=freq))

            # aggregation of dicts
            result = {**totalcountsdict, **residentsdict, **nonresidentsdict, **nonresidentsandnonneighborsdict}

        else:  # there are no users living in the radius (no residents), therefore all non residents
            residentsdict = {'residents': {}}
            tweets_from_non_residents2 = tweets_in_hex_df2[~tweets_in_hex_df2['u_id'].isin(users_in_hex_list2)]
            tweets_from_non_residents_and_non_neighbors2 = tweets_in_hex_df2[
                ~tweets_in_hex_df2['u_id'].isin(users_in_hex_plus_neighbors_list2)]
            nonresidentsdict = json.loads(
                timebasedaggregation(tweets_from_non_residents2, 'nonresidents', frequency=freq))
            nonresidentsandnonneighborsdict = json.loads(
                timebasedaggregation(tweets_from_non_residents_and_non_neighbors2, 'nonresidentsandnonneighbors',
                                     frequency=freq))
            # aggregation of dicts
            result = {**totalcountsdict, **residentsdict, **nonresidentsdict, **nonresidentsandnonneighborsdict}


    else:  # returns an empy dict
        result = {'totalcounts': {},
                  'residents': {},
                  'nonresidents': {},
                  'nonresidentsandnonneighbors': {}}

    # print(result)
    return json.dumps(result)



def countandpopulatejob():
    """
    Simple job to implement countsby_residents_and_non_residents
    and populate into hexcounts collection

    :return:
    """
    cursorx = db.hexcounts.find({'totalcounts': {'$exists': False}})
    continuar = 1
    while continuar == 1:
        try:
            hexid = next(cursorx)['_id']

        except StopIteration:
            print('fin')
            break

        result = a.countsby_residents_and_non_residents(db, hexid, contiguity=1, resolution='9', freq='Q')
        db.hexcounts.update_one({'_id': hexid}, {'$set': json.loads(result)}, upsert=False)



if __name__ == "__main__":

    import communicationwmongo as commu
    db = commu.connecttoLocaldb(database='twitter')
    counterjob(db)