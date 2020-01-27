__author__ = 'Richard'

from pymongo import MongoClient
import numpy as np
import home_location as home

def connecttoLocaldb(database='twitter'):
    client = MongoClient('localhost', 27017)
    db = client[database]
    return db


def obtain_list_of_user_ids(db):
    "Cursor with all distinct user ids"
    newcursor=db.tweets.find().distinct('u_id')
    return newcursor

def pending_users_to_process(db):
    "Reports last user id processed and number of pending uids"
    newcursor=obtain_list_of_user_ids(db)
    print('total number of u_ids :', len(newcursor))
    idoflast=db.users.find().sort('u_id',-1).limit(1)[0]
    print('last u_id processed:', idoflast['u_id'])
    newlist = list(filter(lambda x: x > idoflast['u_id'], newcursor))
    print('number of pending u_ids :', len(newlist))
    return newlist



def updatehomelocation(db, uid, homedata):
    "Updates home location in users collection. Upsert function"
    """
    :param uid: user id
    :param homedata: home data json

    """

    query = {'u_id': uid}
    newvalues = {'$set': homedata}

    db.users.update(query, newvalues, upsert=True)

def findhomeandpopulate(uid,db):
    "Find home for user id and populate users with result function"

    result=home.findhome(db,uid, map=False)
    if result.completed is not False:
        homedata=result.homecoordinates.to_dict()
        del homedata['geometry']
        homedata=correct_encoding(homedata)
        updatehomelocation(db,uid,homedata)


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


def counttweetsandupdate(db):
    from bson.objectid import ObjectId

    cursor=db.radios.find()
    #db.tweets2.find( { 'location': { '$geoWithin': { '$geometry': cursor['geometry'] } } } ).count()
    for radio in cursor:
        totalradiocount=db.tweets2.find( { 'location': { '$geoWithin': { '$geometry': radio['geometry'] } } } ).count()
        recordid=radio['_id']
        db.radios.update({'_id': recordid},  {'$set': {"tweets.totalcount": totalradiocount}})






if __name__ == "__main__":
    db=connecttoLocaldb(database='twitter')