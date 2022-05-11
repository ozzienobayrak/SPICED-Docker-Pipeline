import time
from datetime import datetime
import Credentials
import logging
import tweepy
import pymongo
#Bearer_Token=''

########################
# Get User Information #
########################

# https://docs.tweepy.org/en/stable/client.html#tweepy.Client.get_user
# https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user


#print(dict(user))
#########################
# Get a user's timeline #
#########################
#cursor = tweepy.Paginator(
#    method=client.get_users_tweets,
#    id=user.id,
#    exclude=['replies', 'retweets'],
#    tweet_fields=['author_id', 'created_at', 'public_metrics']
#).flatten(limit=20)


#for tweet in cursor:
#    print(tweet.text)

#####################
# Search for Tweets #
#####################
search_query = "Fox News -is:retweet -is:reply -is:quote lang:en -has:links"


######
# Create a connection to the MongoDB database server
mongo_client = pymongo.MongoClient(host='mongodb') # hostname = servicename for docker-compose pipeline

# Create/use a database
db = mongo_client.twitter
# equivalent of CREATE DATABASE twitter;

# Define the collection
db.tweets.drop()
collection = db.tweets
# equivalent of CREATE TABLE tweets;
######
#for tweet in cursor:
#    print(tweet.text+'\n')
twiter_client = tweepy.Client(
    bearer_token=Credentials.Bearer_Token,
    wait_on_rate_limit=True,
)
response = twiter_client.get_user(
    username='FoxNews', ##the target twitter account to retrieve tweets
    user_fields=['created_at', 'description', 'location',
                 'public_metrics', 'profile_image_url']
)
user=response.data
while True:
    #tweet={'FoxNews': }
    cursor = tweepy.Paginator(
        method=twiter_client.search_recent_tweets,
        query=search_query,
        tweet_fields=['author_id', 'created_at', 'public_metrics'],
    ).flatten(limit=20)
    tweets=[dict(tweet) for tweet in cursor]

    # Insert the tweet into the collection
    logging.warning('-----Tweet being written into MongoDB-----')
    logging.warning(tweets)
    db.tweets.insert_many(tweets) #equivalent of INSERT INTO tweet_data VALUES (....);

    logging.warning(str(datetime.now()))
    logging.warning('----------\n')

    time.sleep(3)