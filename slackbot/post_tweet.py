
# 1) connecting to postgres
# this requires us to install sqlalchemy to connect

# 2) querying data from postgres

# 3) posting the data on slack

#! pip install pyjokes

import pyjokes
import requests

webhook_url = "https://hooks.slack.com/services/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"

tweet = 'Southern California man arrested 3 times in day https://www.foxnews.com/us/southern-california-man-arrested-3-times-in-1-day'
data = {'text': tweet}
requests.post(url=webhook_url, json = data)
