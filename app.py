from flask import Flask
import pymongo
import requests
import pandas as pd
import xmltodict
import ast
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from manage_db import DB_connection
import json
from bson import json_util


app = Flask('API_newsfeedsPT')

OUT_RESP = None

def get_feed_jn():

    global OUT_RESP

    print('Updating JN feed...')

    ## Establishing mongodb connection

    db_connection = DB_connection()

    ## Getting new rss feed and update in database if not existing

    r = requests.get('https://ws.globalnoticias.pt/feed/jn/articles/ultimas/rss')

    resp_json = xmltodict.parse(r.text)
    resp_json = resp_json['rss']['channel']['item']

    for item in resp_json:
        item_sm = {
            'titulo': item['title'],
            'link': item['link'],
            'categoria': item['category'],
            'pubDate': item['pubDate']
        }
        db_connection.insert_if_not_exists(item_sm)
    
    ## Get up-to-date set of records from database

    out_resp = []

    records = db_connection.newsfeeds_collection.find().sort('pubDate', pymongo.DESCENDING).limit(100)
    for record in records:
        out_resp.append(record)

    OUT_RESP = out_resp[:100]
    print('Updating JN completed...')


@app.route('/get_jn')
def start():
    global OUT_RESP
    print(OUT_RESP)

    return json.loads(json_util.dumps(OUT_RESP))

get_feed_jn()
sched = BackgroundScheduler(daemon=True)
sched.add_job(get_feed_jn, 'interval', seconds=60)
sched.start()
atexit.register(lambda: sched.shutdown())

if __name__ == '__main__':
    app.run()