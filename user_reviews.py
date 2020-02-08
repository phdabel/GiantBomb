import giantbomb.giantbomb as giantbomb
import os, sys, getopt
import datetime
from pymongo import MongoClient
import logging
import time
import timeit


def format_date(date_time):
    if date_time is None:
        return None
    return datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")


argv = sys.argv[1:]
opts, args = getopt.getopt(argv, ":o:", ['offset'])

if opts.__len__() != 0:
    if opts[0][0] == '-o':
        offset = int(opts[0][1])
    else:
        offset = 0
else:
    offset = 0

logging.basicConfig(filename='user_reviews_giantbomb.log', level=logging.DEBUG)
gb = giantbomb.Api(os.environ['API_KEY'], os.environ['USER_AGENT'])
client = MongoClient(os.environ["MONGODB_HOST"],
                     username=os.environ['MONGODB_USER'],
                     password=os.environ['MONGODB_PASS'],
                     authSource=os.environ['MONGODB_DB'],
                     authMechanism=os.environ['MONGODB_AUTH'])

db = client[os.environ['MONGODB_DB']]
user_reviews = db["user_reviews"]

x = gb.get("/user_reviews", {"offset": offset})

total = x['number_of_total_results']
limit = x['limit']

logging.info("=============================================================")
logging.info("Crawling")
logging.info("Date: %s" % datetime.datetime.today().isoformat())
logging.info("Resource: user_reviews")

while offset < total:
    # do anything with x and perform another request with
    start = timeit.timeit()
    for review in x['results']:
        logging.info("Getting info of the id %s, reviewer: %s" % (review["id"], review["deck"]))
        tmp = None
        tmp = user_reviews.find_one({"_id": review['id']})
        if tmp is None:
            time.sleep(1)
            review['_id'] = review['id']
            review['date_added'] = format_date(review['date_added'])
            review['date_last_updated'] = format_date(review['date_last_updated'])
            user_reviews.insert_one(review)

    offset = offset + limit
    x = gb.get("/user_reviews", {"offset": offset})

logging.info("Crawling ended")
logging.info("Date: %d" % datetime.datetime.today().isoformat())
logging.info("=============================================================")
