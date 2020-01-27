__author__ = 'Richard'

import pandas as pd
import communicationwmongo as commu


def populatetweets(collection):
    db=commu.connecttoLocaldb(database='twitter')
    """
    Populates a collection
    """
    for year in range(2012, 2016):
        df = pd.read_csv('D:\\twitter\\ba_'+year+'.csv')
        db[collection].insert_many(df.to_dict('records'))



if __name__ == "__main__":
    db=populatetweets('twitter')