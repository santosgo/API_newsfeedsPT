from flask import Flask
import requests
import pandas as pd
import xmltodict
import ast
import atexit
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask('API_newsfeedsPT')

OUT_RESP = None
OUT_RESP_DF = None


def get_feed_jn():

    global OUT_RESP
    global OUT_RESP_DF

    print('Updating JN feed...')

    ## Getting new data in dataframe format

    r = requests.get('https://ws.globalnoticias.pt/feed/jn/articles/ultimas/rss')

    resp_json = xmltodict.parse(r.text)
    resp_json = resp_json['rss']['channel']['item']

    resp_json_sm = []

    for item in resp_json:
        item_sm = {
            'titulo': item['title'],
            'link': item['link'],
            'categoria': item['category'],
            'pubDate': item['pubDate']
        }
        resp_json_sm.append(item_sm)
        

    if OUT_RESP:

        new_feed_df = pd.DataFrame.from_records(resp_json_sm)

        out_resp_json = []

        for item in OUT_RESP:
            item_json = {
                'titulo': item['titulo'],
                'link': item['link'],
                'categoria': item['categoria'],
                'pubDate': item['pubDate']
            }
            out_resp_json.append(item_json)
        
        old_df = pd.DataFrame.from_records(out_resp_json)
    
        old_df.categoria = old_df.categoria.apply(str)
        new_feed_df.categoria = new_feed_df.categoria.apply(str)

        merged_df = pd.merge(old_df, new_feed_df, how="outer", on=["titulo", "link", "categoria", "pubDate"])
        merged_df = merged_df.sort_values('pubDate', ascending=False).drop_duplicates(subset=['pubDate'])
        merged_df.categoria = merged_df.categoria.apply(lambda x: ast.literal_eval(x))

        out_resp = merged_df.to_dict(orient="records")
    
    else:
        out_resp = resp_json_sm

    print('Updating JN completed...')
    OUT_RESP = out_resp[:100]
    print(OUT_RESP)

@app.route('/get_jn')
def start():
    global OUT_RESP
    print(OUT_RESP)
    return OUT_RESP

get_feed_jn()
sched = BackgroundScheduler(daemon=True)
sched.add_job(get_feed_jn, 'interval', seconds=10)
sched.start()
atexit.register(lambda: sched.shutdown())

if __name__ == '__main__':
    app.run()