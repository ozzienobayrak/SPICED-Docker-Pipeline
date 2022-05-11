import time
import pymongo
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import logging
from sqlalchemy import create_engine
import re

#create the analyzer to use later
s  = SentimentIntensityAnalyzer()

# Establish a connection to the MongoDB server
client = pymongo.MongoClient(host="mongodb", port=27017)
time.sleep(10)  # seconds
# Select the database you want to use withing the MongoDB server
db = client.twitter

##Step 8: Connect to Postgres
pg = create_engine('postgresql://postgres:1234@postgresdb:5432/tweets', echo=True)
##(pg = create_engine('postgresql://user:password@host:5432/dbname', echo=True) 
# these are defined in docker-compose.yml)
## Step 9: Creating table
pg.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
    text VARCHAR(500),
    sentiment NUMERIC
);
''')

mentions_regex= '@[A-Za-z0-9]+'
url_regex='https?:\/\/\S+' #this will not catch all possible URLs
hashtag_regex= '#'
rt_regex= 'RT\s'

def clean_tweets(text):
    text = re.sub(mentions_regex, '', text)  #removes @mentions
    text = re.sub(hashtag_regex, '', text) #removes hashtag symbol
    text = re.sub(rt_regex, '', text) #removes RT to announce retweet
    text = re.sub(url_regex, '', text) #removes most URLs
    
    return text

##Step 7: Print all entries
docs = db.tweets.find(limit=10)
for doc in docs:
    print(doc)
##step 10
    text = doc['text']
    ##pattern = '[A-Za-z+]{4,}'
    text=clean_tweets(text)
    sentiment = s.polarity_scores(text)  # assuming your JSON docs have a text field
    score = sentiment['compound']  # placeholder value
    logging.critical(score)
    query = "INSERT INTO tweets VALUES (%s, %s);"
    pg.execute(query, (text, score))
    #print(sentiment)
    # replace placeholder from the ETL chapter
    #print(score)
    

df=pd.DataFrame(docs)
df.to_sql('tweets', pg, if_exists='replace')
print(df)
