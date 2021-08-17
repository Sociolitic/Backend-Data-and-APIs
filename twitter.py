import tweepy
import pymongo
from pymongo import MongoClient
import datetime
import dns
import datetime
from sentiment import *


consumer_key = "zr6Q2eouAo0eID46lwC0rbeUo"
consumer_secret = "5G5MwRU2L3F9LSkjqmrhkdqnAG8f4hSIxt5XNaP54YrGZ2K6Yr"
access_token = "1361882810848452610-1wxJm975Mm5B85y9XupGoHbkNqwnIh"
access_token_secret = "Vb4XvjlLOAUrmWqraKQ7LAgsC7jMp4ZoHrwhHvhgUZRLz"
client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
Twitter = db.twitter


auth= tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

import json,time

class MyStreamListener(tweepy.StreamListener):

    def __init__(self,q, time_limit=15*60):
        self.start_time = time.time()
        self.query = q
        self.limit = time_limit
        super(MyStreamListener, self).__init__()

    def on_connect(self):
        print("Connected to Twitter API.")

    def on_status(self, status):
        try:
            tweet_id = status.id
            if status.text[:4] == "RT @":
                original_id = status.retweeted_status.id
                if status.retweeted_status.truncated == True:
                    tweet_txt = status.retweeted_status.extended_tweet['full_text']
                else:
                    tweet_txt = status.retweeted_status.text
            elif status.truncated == True:
                tweet_txt = status.extended_tweet['full_text']
                original_id = 0
            else:
                tweet_txt = status.text
                original_id = 0

            user_id = status.user.id
            user_geo = status.geo
            created_at = status.created_at
            lang = status.lang

            entities = status.entities
            retweet_count = status.retweet_count
            print("tweet_id :",tweet_id)
            Sentiment = sentiment_analysis(tweet_txt)
            twitter_data = {
                        "text": tweet_txt,
                        "tweet_id": str(tweet_id) ,
                        "user_id": str(user_id),
                        "geo": user_geo,
                        "lang": lang,
                        "retweet_count": int(retweet_count),
                        "created_time" :   created_at,
                        "tag" : self.query,
                        "sentiment" : Sentiment,
                        "original_id": original_id,
                        "entities": entities,
                        "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                    }
            Twitter.insert_one(twitter_data)
            if (time.time() - self.start_time) > self.limit:
                # print(time.time(), self.start_time, self.limit)
                return False
        except :
            if (time.time() - self.start_time) > self.limit:
        #         print(time.time(), self.start_time, self.limit)
                return False
            pass
        if (time.time() - self.start_time) > self.limit:
            # print(time.time(), self.start_time, self.limit)
            return False
    def on_error(self, status_code):
        return False

def twitter_past(q,count=3000):
    Count = 0
    for status in tweepy.Cursor(api.search, q,tweet_mode="extended").items(3200):
        tweet_id = status.id
        if (Count == count ):
            return "Done"
        if (db.twitter.find({"tweet_id":tweet_id}).count() > 0)== False:
            Count +=1
            print(tweet_id)
            if status.full_text[:2] == "RT @ ":
                is_retweet = True
                user_id = status.user.id
                user_geo = status.retweeted_status.geo
                original_id=status.retweeted_status.id
                if status.retweeted_status.truncated == True:
                    tweet_txt = status.retweeted_status.extended_tweet['full_text']
                else:
                    tweet_txt = status.retweeted_status.full_text

            elif status.truncated == True:
                is_retweet = False
                user_id = status.user.id
                user_geo = status.geo
                original_id= 0
                tweet_txt = status.extended_tweet['full_text']

            else:
                is_retweet = False
                user_id = status.user.id
                user_geo = status.geo
                original_id= 0
                tweet_txt = status.full_text
            created_at = status.created_at
            lang = status.lang
            entities = status.entities
            retweet_count = status.retweet_count
            Sentiment = sentiment_analysis(tweet_txt)

            twitter_data = {
                                "text": tweet_txt,
                                "tweet_id": str(tweet_id ),
                                "user_id": str(user_id),
                                "geo": user_geo,
                                "lang": lang,
                                "retweet_count": int(retweet_count),
                                "created_time" : created_at,
                                "tag" : q,
                                "sentiment" : Sentiment,
                                "original_id":original_id,
                                "entities" : entities,
                                "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                            }
            Twitter.insert_one(twitter_data)
    return ("Extracted twitter data")

def twitter_stream(q,t=15*60):
    myStream = tweepy.Stream(auth=api.auth, listener=MyStreamListener(q=q,time_limit= t),tweet_mode="extended")
    myStream.filter(track=[q])
    return "Done"
