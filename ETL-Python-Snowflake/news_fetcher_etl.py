import pandas as pd
import json
import requests
import os
from base64 import b64decode
import datetime
from datetime import date
import uuid

def runner():
    today = date.today()
    api_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

    base_url = 'https://newsapi.org/v2/everything?q={}&from={}&to={}&sortBy=popularity&apiKey={}&language=en'
    #print(base_url)
    start_date_value = str(today - datetime.timedelta(days = 1))
    end_date_value = str(today)

    df = pd.DataFrame(columns = ['newsTitle', 'timeStamp', 'urlSource', 'content', 'source', 'author', 'urlToImage'])

    url_extractor = base_url.format('Inflation', start_date_value, end_date_value, api_key)
    #print(url_extractor)

    response = requests.get(url_extractor)
    d = response.json()

    for i in d['articles']:
        newsTitle = i['title']
        timeStamp = i['publishedAt']
        urlSource = i['url']
        source = i['source']
        author = i['author']
        urlToImage = i['urlToImage']
        partial_content = ""
        if str(i['content']) != 'None':
            partial_content = i['content']
            if len(partial_content) > 400:
                partial_content = partial_content[0:400]
        
        df = pd.concat([df, pd.DataFrame(
            {'newsTitle' : newsTitle, 'timeStamp' : timeStamp, 'urlSource' : urlSource, 
             'content' : partial_content, 'source' : source, 'author' : author, 'urlToImage' : urlToImage}
        )], ignore_index = True)

        
    filename = str(uuid.uuid4())
    output_file = '/home/ubuntu/{}.parquet'.format(filename)

    df1 = df.drop_duplicates()
    df1.to_parquet(output_file)

    return output_file
        

        


