__author__ = 'Ricardo Pasquini'

import pandas as pd
import communicationwmongo as commu
from h3 import h3



def populatetweets(collection):
    db=commu.connecttoLocaldb(database='twitter')
    """
    Populates a collection
    """
    for year in range(2012, 2016):
        df = pd.read_csv('D:\\twitter\\ba_'+year+'.csv')
        db[collection].insert_many(df.to_dict('records'))



###

def addhexjob(db, chunksize = 1000):

    """
    Add hex ids to tweets collection
    Smart job in chunks
    """

    from pymongo import InsertOne, UpdateOne
    import time

    start_time = time.time()

    collectionsize = db.tweets.count()

    iteration = 1

    while iteration < (int(collectionsize / chunksize) + 1):
        iter_start_time = time.time()
        if iteration > 1:
            cursor = db.tweets.find({'_id': {'$gt': last_object_id}}).sort('_id', 1).limit(chunksize)
            # alternatively I could have done '$lt' and sort -1 as suggest in the documentation
        else:
            cursor = db.tweets.find().sort('_id', 1).limit(chunksize)

        df = pd.DataFrame(list(cursor))

        requests = add_hexs_and_prepare_bulk_request(df)

        try:
            db.tweets.bulk_write(requests, ordered=False)
        except BulkWriteError as bwe:
            pprint(bwe.details)

        # obtengo el last id del dataframe
        last_object_id = df.iloc[-1]['_id']

        iter_end_time = time.time()
        print(' iter:', iteration, ' time:', iter_end_time - iter_start_time)
        iteration += 1

    end_time = time.time()
    print('total elapsed time:', end_time - start_time)



def add_hexs_and_prepare_bulk_request(df):
    """
    Apply geo_to_h3 to a chunk of tweets and prepare bulk request
    :param df: dataframe of tweets (a chunk of the tweets collection)
    :return: request job for bulk insert

    Example:
    [InsertOne({'_id': ObjectId('5e10ced16e7ccd7b44e9ee07'), 'hex': {'9': '89dd6876033ffff'}}),

    """
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




if __name__ == "__main__":
    db=populatetweets('twitter')