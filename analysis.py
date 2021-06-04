import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import json
import time
from h3 import h3
import datetime
import numpy as np
plt.rcParams['figure.figsize'] = [10, 10]
import my_h3_functions as myh3

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

    if  df3['created_at'].dtype == '<M8[ns]': # if data is already timestamped then just copy
        df3['timestamp'] =df3['created_at']
    else:
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


    else:  # returns an empty dict
        result = {'totalcounts': {},
                  'residents': {},
                  'nonresidents': {},
                  'nonresidentsandnonneighbors': {}}

    # print(result)
    return json.dumps(result)



def countandpopulatejobDEPRECATED(db):

    """
    Simple job to implement countsby_residents_and_non_residents
    and populate into hexcounts collection

    """
    print('Hexagons pending to analyze..', db.hexcounts.count_documents({'totalcounts': { '$exists': False} }))

    cursorx = db.hexcounts.find({'totalcounts': {'$exists': False}})
    continuar = 1
    j=1
    while continuar == 1:
        try:
            hexid = next(cursorx)['_id']
            if (j/50).is_integer(): #printing each 50 hexs
                print('iter:',j)
            j=j+1


        except StopIteration:
            print('fin')
            break

        result = countsby_residents_and_non_residents(db, hexid, contiguity=1, resolution='9', freq='Q')
        db.hexcounts.update_one({'_id': hexid}, {'$set': json.loads(result)}, upsert=False)


def countandpopulatejob(db):

    """
    Simple job to implement countsby_residents_and_non_residents
    and populate into hexcounts collection

    """
    numberof_hexcounts_without_totalcounts=db.hexcounts.count_documents({'totalcounts': { '$exists': False} })
    print('Hexagons pending to analyze..', numberof_hexcounts_without_totalcounts)

    while numberof_hexcounts_without_totalcounts>0:

        cursorx = db.hexcounts.find({'totalcounts': {'$exists': False}},batch_size=10, limit=5)

        continuar = 1
        j=1
        while continuar == 1:
            try:
                hexid = next(cursorx)['_id']
                print(hexid)
                if (j/50).is_integer(): #printing each 50 hexs
                    print('iter:',j)
                j=j+1

            except StopIteration:
                print('fin')
                break

            result = countsby_residents_and_non_residents(db, hexid, contiguity=1, resolution='9', freq='Q')
            #print(result)
            db.hexcounts.update_one({'_id': hexid}, {'$set': json.loads(result)}, upsert=False)


        numberof_hexcounts_without_totalcounts=db.hexcounts.count_documents({'totalcounts': { '$exists': False} })
        print('Hexagons pending to analyze..', numberof_hexcounts_without_totalcounts)

def hexcountsresults_to_df_DEPRECATED(db, save=False):

    """ Converts hexcounts collection containing resuts to a dataframe"""

    #para pasar de la coleccion al dataframe
    #voy a loopear la coleccion, convertir cada documento en un dataframe y despues unirlos
    cursor=db.hexcounts.find()
    listofdfis=[]

    for doc in cursor:
        dfi=pd.DataFrame(doc).reset_index().rename(columns={"index": "time"})

        if  dfi['time'].dtype != '<M8[ns]': # if data is already timestamped then just copy
            dfi['time']=pd.to_datetime(pd.to_numeric(dfi['time'], errors='coerce') // 1000, unit='s' )

        listofdfis.append(dfi)

    #print(listofdfis)
    df=pd.concat(listofdfis)
    df=df.reset_index(drop=True)

    if save:
        df.to_pickle("./hexcountsdf.pkl")

    return df


from pandas.io.json import json_normalize

def hexcountsresults_to_df(db, save=False):

    """ Converts hexcounts collection containing resuts to a dataframe"""

    cursor=db.hexcounts.find()
    #.limit(10)

    prueba=pd.DataFrame(list(cursor) )

    df1=pd.DataFrame(pd.concat([prueba[['_id']],json_normalize(prueba["nonresidents"])],axis=1 ).set_index('_id').stack()).reset_index().rename(columns={0:'nonresidents'})
    df2=pd.DataFrame(pd.concat([prueba[['_id']],json_normalize(prueba["nonresidentsandnonneighbors"])],axis=1 ).set_index('_id').stack()).reset_index().rename(columns={0:'nonresidentsandnonneighbors'})
    df3=pd.DataFrame(pd.concat([prueba[['_id']],json_normalize(prueba["residents"])],axis=1 ).set_index('_id').stack()).reset_index().rename(columns={0:'residents'})
    df4=pd.DataFrame(pd.concat([prueba[['_id']],json_normalize(prueba["totalcounts"])],axis=1 ).set_index('_id').stack()).reset_index().rename(columns={0:'totalcounts'})


    df1=df1.merge(df2,on=['_id','level_1'],how='outer')
    df1=df1.merge(df3,on=['_id','level_1'],how='outer')
    df1=df1.merge(df4,on=['_id','level_1'],how='outer')


    if  df1['level_1'].dtype == '<M8[ns]': # if data is already timestamped then just copy
        df1['time'] =df1['level_1']
    else:
        df1['time']=pd.to_datetime(pd.to_numeric(df1['level_1'], errors='coerce') // 1000, unit='s' )

    df1.drop(columns='level_1')

    if save:
        df1.to_pickle("./hexcountsdf.pkl")

    return df1


def percent_change_two_periods_df(df, datebeforeandafterperiod=datetime.datetime(2013,6,30), period_statistic="mean"):

    """Creates a geodataframe with rate of change in hex counts between two periods determined by a chosen date
    :param df: Hexcounts dataframe, which is a panel database at hex and time (quaterly)
    :param datebeforeandafterperiod:
    :return: geodataframe
    """

    """_ch stands for rate changes
       dataframe also returns baseline period level data denoted p0
       this is to check that there are enough data in baseline period"""

    #Devuelve: Las variables que me interesan son nonresidents_ch	nonresidentsandnonneighbors_ch	residents_ch	totalcounts_ch
    #que son los cambios porcentuales en tweeter usage.

    df['period']=np.where(df.time>datebeforeandafterperiod,1,0)

    if period_statistic=="sum":
        #Tomar el promedio por periodo de hexcount
        df2=df.groupby(['_id','period']).sum()
        #df2
    else:
        df2=df.groupby(['_id','period']).mean()


    # Diferencias entre periodos para cada una de las variables
    df2dif=df2.groupby('_id')['nonresidents', 'nonresidentsandnonneighbors', 'residents', 'totalcounts'].diff(1)

    # Me voy a quedar por un lado con las diferencias en df2dif, y por otro lado con el periodo 0 en df20
    df2dif=df2dif.reset_index()
    df2dif=df2dif.loc[df2dif.period==1]
    #df2dif

    df2b=df2.reset_index()
    df20=df2b.loc[df2b.period==0]

    # Junto todo en un merge
    dfnew=df2dif.merge(df20, left_on='_id', right_on='_id', suffixes=('_dif', '_p0'))
    dfnew=dfnew.drop(columns=['period_dif', 'period_p0'])
    #dfnew

    # computo las tasas de crecimiento en las variables _ch
    for var in ['nonresidents', 'nonresidentsandnonneighbors','residents','totalcounts']:
        dfnew[var+'_ch']=dfnew[var+'_dif']/dfnew[var+'_p0']


    # Las versiones b de las tasas de crecimiento son solo las tasas para aquellos lugares que tenian mas de 50 tweets en periodo 0
    for var in ['nonresidents', 'nonresidentsandnonneighbors','residents','totalcounts']:
        dfnew[var+'_ch'+'b']=np.where(dfnew[var+'_p0']>50,dfnew[var+'_ch'],np.NaN)

    #Transformo a gdf usando la funcion que construi especialmente
    gdfchanges=myh3.df_with_hexid_to_gdf(dfnew)

    return gdfchanges







if __name__ == "__main__":

    import communicationwmongo as commu
    db = commu.connecttoLocaldb(database='twitter')
    counterjob(db)