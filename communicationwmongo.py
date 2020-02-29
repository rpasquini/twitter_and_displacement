__author__ = 'Ricardo Pasquini'

from pymongo import MongoClient
import numpy as np
import home_location as home

def connecttoLocaldb(database='twitter'):
    client = MongoClient('localhost', 27017)
    db = client[database]
    return db


def obtain_list_of_user_ids(db):
    "From tweets collection, retrieves cursor with all distinct user ids"
    newcursor=db.tweets.find().distinct('u_id')
    return newcursor

def users_in_tweets_not_in_userscollection(db):
    """This function is used to populate users collection for the first time
    Reports last user id processed and number of pending uids"""
    newcursor=obtain_list_of_user_ids(db)
    print('total number of u_ids :', len(newcursor))
    idoflast=db.users.find().sort('u_id',-1).limit(1)[0]
    print('last u_id processed:', idoflast['u_id'])
    newlist = list(filter(lambda x: x > idoflast['u_id'], newcursor))
    print('number of pending u_ids :', len(newlist))
    return newlist


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