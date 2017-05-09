# Imports
import pandas as pd
import numpy as np
import tweepy
import sys
import traceback
import logging
import time
import json

from sqlalchemy import create_engine

# Load config
with open('conf.json') as conf:
    cfg = json.loads(conf)

# Some credentials stuff
APP_KEY = cfg['app_key']
APP_SECRET = cfg['app_secret']

USER_KEY = cfg['user_key']
USER_SECRET = cfg['user_secret']

# Setting up tweepy authentication
auth = tweepy.OAuthHandler(APP_KEY, APP_SECRET)
auth.set_access_token(USER_KEY, USER_SECRET)

api = tweepy.API(auth)

# Define logger
logger = logging.getLogger()
fh = logging.FileHandler('twitterstream.log')
fh.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

logger.info('Started new session')

# Defining our stream handler class
class apnStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(apnStreamListener, self).__init__()
        self.eng = self.initDB()

    def initDB(self):

        engine = create_engine("mysql://{db_user}:{db_pass}@{db_host}/{db_schema}?charset=utf8".format(
            db_user = cfg['db_user'],
            db_pass = cfg['db_pass'],
            db_host = cfg['db_host'],
            db_schema = cfg['db_schema']
        ))
        return engine


    def toDB(self, tw_id, uid, loc, coords, text):

        query = """INSERT INTO tweet_log (tweet_id, user_id, loc, coords, text)
                VALUES ('{0}', '{1}', '{2}', '{3}', '{4}')""".format(tw_id,
                                                                     uid,
                                                                     loc.replace("'", "''"),
                                                                     ",".join(map(str, coords)),
                                                                     text.replace("'", "''"))

        try:

            with self.eng.connect() as conn:
                logger.info(query)
                conn.execute(query)

        except:
            logger.error(query)
            logger.error(traceback.format_exc())


    def on_status(self, status):

        txt = status.text
        tw_id = status.id_str
        loc = status.user.location
        coords = status.coordinates['coordinates'] if status.coordinates is not None else []
        uid = status.user.id_str

        txt = txt.encode('utf-8') if txt is not None else ""
        loc = loc.encode('utf-8') if loc is not None else ""

        self.toDB(tw_id, uid, loc, coords, txt)


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            logger.error(status_code)


while 1 == 1:

    try:
        listener = apnStreamListener()
        stream = tweepy.Stream(auth=api.auth, listener=listener)
        stream.filter(track=['appnexus','appnexussummit'])

    except KeyboardInterrupt:

        stream.disconnect()
        print "Stopping stream..."

        sys.exit(0)

    except:
        print "Error encountered...retry in 5."
        logger.error(traceback.format_exc())
        time.sleep(5)
