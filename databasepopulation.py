__author__ = 'Ricardo Pasquini'

import pandas as pd
import communicationwmongo as commu
from h3 import h3
from pymongo import InsertOne, UpdateOne
import time
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId
import pymongo

def populatetweets(db, path='D:\\twitter\\', cityprefix='ba', yearstart=2012, yearend=2015, chunksize=1000):

    """ Populates tweets in csv file to mongodb twitter.tweets collection

    :param path: Path to csv location
    :param cityprefix: city prefix in the csv file name
    :param databasename: Database name


    """

    for year in range(yearstart, yearend+1):
        print('Now populating year ',year)
        #df = pd.read_csv(path+cityprefix+'_'+str(year)+'.csv')
        for df in pd.read_csv(path+cityprefix+'_'+str(year)+'.csv', chunksize=chunksize):
            db.tweets.insert_many(df.to_dict('records'))

    print('process completed')



def add_hexs_and_prepare_bulk_request(df, dataformat='raw'):
    """
    Apply geo_to_h3 to a chunk of tweets and prepare bulk request
    :param df: dataframe of tweets (a chunk of the tweets collection)
    :return: request job for bulk insert

    Example:
    [InsertOne({'_id': ObjectId('5e10ced16e7ccd7b44e9ee07'), 'hex': {'9': '89dd6876033ffff'}}),

    """
    if dataformat=='raw':
        df2=df.apply(lambda row: h3.geo_to_h3(row['latitude'],row['longitude'],9), axis=1)
        df2.name='9'

        df3=df.apply(lambda row: h3.geo_to_h3(row['latitude'],row['longitude'],10), axis=1)
        df3.name='10'
    else: #coordinates have been reshaped to location shape in mongo

        # apply geo_to_h3
        resolution=9
        df2=df['location'].apply(lambda row: h3.geo_to_h3(row["coordinates"][0],row["coordinates"][1],resolution))
        df2.name='9'

        resolution=10
        df3=df['location'].apply(lambda row: h3.geo_to_h3(row["coordinates"][0],row["coordinates"][1],resolution))
        df3.name='10'



    # join (concatenating) tweets with new data
    df4=pd.concat([df,df2,df3], axis=1)

    # Esta funcion me arma el diccionario y lo mete en el InsertOne method que luego voy a pasar en formato de lista
    def f(x):
        #return InsertOne({'_id': ObjectId(x['_id']),'hex':{'9':x['9'],'10':x['10']}})
        return UpdateOne({'_id': ObjectId(x['_id'])}, {'$set': {'hex':{'9':x['9'],'10':x['10']}}})
    #            'geometry': gpd.GeoSeries(x['geometry']).__geo_interface__['features'][0]['geometry']}

    # return list of requests
    return list(df4.apply(f, axis=1))



def addhexjob(db, chunksize = 1000, dataformat='raw'):

    """ Add hex ids to tweets collection. Smart job in chunks
    :param dataformat: raw refers to raw twitter data, otherwise, if tweets have been already reshaped to mongo geolocation choose 'mongo'

    """
    collectionname='tweets'


    start_time = time.time()

    collectionsize = db[collectionname].count()

    iteration = 1

    while iteration < (int(collectionsize / chunksize) + 1):
        iter_start_time = time.time()
        if iteration > 1:
            cursor = db[collectionname].find({'_id': {'$gt': last_object_id}}).sort('_id', 1).limit(chunksize)
            # alternatively I could have done '$lt' and sort -1 as suggest in the documentation
        else:
            cursor = db[collectionname].find().sort('_id', 1).limit(chunksize)

        df = pd.DataFrame(list(cursor))

        requests = add_hexs_and_prepare_bulk_request(df, dataformat=dataformat)

        try:
            db[collectionname].bulk_write(requests, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        # obtengo el last id del dataframe
        last_object_id = df.iloc[-1]['_id']

        iter_end_time = time.time()
        print(' iter:', iteration, ' time:', iter_end_time - iter_start_time)
        iteration += 1

    end_time = time.time()
    print('total elapsed time:', end_time - start_time)


def create_indexes(db):

    """ Create indexes in tweets collection"""

    db.tweets.create_index([("u_id", pymongo.ASCENDING)])
    db.tweets.create_index([("hex.9", pymongo.ASCENDING)])
    db.tweets.create_index([("hex.10", pymongo.ASCENDING)])
    db.tweets.create_index([("u_id", pymongo.ASCENDING)])



def populate_users_collection(db):

    """ Populate users collection """

    db.tweets.aggregate( [ { '$group' : { '_id' : "$u_id" } } ,
                             { "$project": { "_id": 0, "u_id": "$_id"}},
                             { '$out' : "users" }] )


def populate_hexcounts_collection(db):
    """ Populate users collection
    :param db: database connection
    """
    db.tweets.aggregate( [ { '$group' : { '_id' : "$hex.9" } } ,
                           { "$project": { "_id": 0, "_id": "$_id"}}, { '$out' : "hexcounts" }] )


if __name__ == "__main__":
    db=populatetweets('twitter')
