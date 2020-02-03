import giantbomb.giantbomb as giantbomb
import os
import datetime
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import logging
import time

logging.basicConfig(filename='games_giantbomb.log', level=logging.DEBUG)

gb = giantbomb.Api(os.environ['API_KEY'], os.environ['USER_AGENT'])

client = MongoClient(os.environ["MONGODB_HOST"],
                     username=os.environ['MONGODB_USER'],
                     password=os.environ['MONGODB_PASS'],
                     authSource=os.environ['MONGODB_DB'],
                     authMechanism=os.environ['MONGODB_AUTH'])

db = client[os.environ['MONGODB_DB']]
games = db["games"]


def format_date(date_time):
    if date_time is None:
        return None
    return datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")


x = gb.get("/games", {})

total = x['number_of_total_results']
offset = 0
limit = x['limit']

while offset < total:
    # do anything with x and perform another request with
    for game in x['results']:
        logging.info("Getting info of the id %s, game: %s" % (game["id"], game["name"]))
        tmp = games.find_one({"_id": game['id'],
                              'date_last_updated': {"$lte": game['date_last_updated']}})
        if tmp is None:
            time.sleep(1)
            g = gb.get("/game/" + game["guid"])
            while g["error"] != 'OK':
                logging.warning("Sleeping for 60s before trying again.")
                time.sleep(60)
                g = gb.get("/game/" + game["guid"])

            if g["error"] == 'OK':
                logging.info("Saving game %s" % (game["guid"]))
                g['results']['_id'] = g['results']['id']
                g['results']['date_added'] = format_date(g['results']['date_added'])
                g['results']['date_last_updated'] = format_date(g['results']['date_last_updated'])
                g['results']['original_release_date'] = format_date(g['results']['original_release_date'])
                try:
                    games.insert_one(g["results"])
                except DuplicateKeyError:
                    logging.warning("Trying to insert existing document id %s" % (g['results']['_id']))
                    tmp = games.find_one({"_id": g['results']['id'],
                                          'date_last_updated': {"$lt": g['results']['date_last_updated']}})
                    if tmp is not None:
                        logging.warning("Removing old document id %s." % g['results']['id'])
                        games.delete_one({"_id": g['results']['id']})
                        logging.warning("Inserting updated document id %s." % g['results']['id'])
                        games.insert_one(g["results"])
                    else:
                        logging.info("There is no update for document id %s." % g['results']['id'])
                    pass
    offset = offset + limit
    x = gb.get("/games", dict({"offset": offset}))
