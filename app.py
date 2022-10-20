from flask import Flask
import requests
import pandas as pd
import xmltodict
import ast
import atexit
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask('API_newsfeedsPT')

OUT_RESP = None

def get_feed_jn():
    print('Updating JN feed...')
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
        
    new_feed_df = pd.DataFrame.from_records(resp_json_sm)

    try:
        old_df = pd.read_csv('jn.csv')
        old_df.categoria = old_df.categoria.apply(str)
        new_feed_df.categoria = new_feed_df.categoria.apply(str)

        merged_df = pd.merge(old_df, new_feed_df, how="outer", on=["titulo", "link", "categoria", "pubDate"])
        merged_df = merged_df.sort_values('pubDate', ascending=False).drop_duplicates(subset=['pubDate'])
        merged_df.categoria = merged_df.categoria.apply(lambda x: ast.literal_eval(x))

        pd.DataFrame.to_csv(merged_df, 'jn.csv', index=False)
        out_resp = merged_df.to_dict(orient="records")
    except FileNotFoundError:
        pd.DataFrame.to_csv(new_feed_df, 'jn.csv', index=False)
        out_resp = resp_json_sm

    print('Updating JN completed...')
    global OUT_RESP
    OUT_RESP = out_resp[:30]

@app.route('/get_jn')
def start():
    global OUT_RESP
    print(OUT_RESP)
    return OUT_RESP

if __name__ == '__main__':
    get_feed_jn()
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(get_feed_jn, 'interval', seconds=60)
    sched.start()
    app.run()
    atexit.register(lambda: sched.shutdown())