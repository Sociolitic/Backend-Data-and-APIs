import tweepy
import pymongo
from pymongo import MongoClient
import datetime
import dns
import datetime
from sentiment import *


consumer_key = "OaE3eeWXCp1G3ACVdDUNLnPkw"
consumer_secret = "y87lVHLWdq27bmo18xWASIJKjmxQQ4NjEQflBLFg8AnpTwEccV"
access_token = "1361882810848452610-Z1dQ2INKhBzuGtxsKiSjj495EoHzRg"
access_token_secret = "9IFhQtI0H5D3nOR4ok2fUfpreFJqjmLNTLNYif7EEvO3S"
client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
Twitter = db.twitter
Twitter_count = db.twitter_count
Total_count = db.total_count


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
            if status.text[:2] == "RT @":
                user_id = status.retweeted_status.user.id
                user_geo = status.retweeted_status.geo
                if status.retweeted_status.truncated == True:
                    tweet_txt = status.retweeted_status.extended_tweet['full_text']
                else:
                    tweet_txt = status.retweeted_status.text
            elif status.truncated == True:
                user_id = status.user.id
                user_geo = status.geo
                tweet_txt = status.extended_tweet['full_text']
            else:
                user_id = status.user.id
                user_geo = status.geo
                tweet_txt = status.text
            created_at = status.created_at
            lang = status.lang
            retweet_count = status.retweet_count
            print("tweet_id :",tweet_id)
            Sentiment = sentiment_analysis(tweet_txt)
            if Sentiment == "Positive":
                pos = 1
                neg = 0
                neu = 0
            elif Sentiment == "Negative":
                pos = 0
                neg = 1
                neu = 0
            else:
                pos = 0
                neg = 0
                neu = 1
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
                        "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                    }
            Twitter.insert_one(twitter_data)
            count_data = {
            "tag" : self.query,
            "Total_reviews": 1,
            "positive":pos,
            "negative":neg,
            "neutral":neu,
            "createdAt": datetime.datetime.now(),
            "updatedAt": datetime.datetime.now()
            }
            if (db.twitter_count.find({'tag':self.query}).count() > 0)== False:
                Twitter_count.insert_one(count_data)
            else :
                tum_cnt = db.twitter_count.find_one({'tag':self.query})
                reviews = tum_cnt["Total_reviews"]
                positive = tum_cnt["positive"]
                negative = tum_cnt["negative"]
                neutral = tum_cnt["neutral"]
                updated_count_data ={
                "$set":
                {"Total_reviews": reviews+1,
                "positive":positive+pos,
                "negative":negative+neg,
                "neutral":neutral+neu,
                "updatedAt": datetime.datetime.now()
                }
                }
                Twitter_count.update_one(tum_cnt,updated_count_data)
            if (db.total_count.find({'tag':self.query}).count() > 0)== False:
                Total_count.insert_one(count_data)
            else:
                ttl_cnt = db.total_count.find_one({'tag':self.query})
                reviews = ttl_cnt["Total_reviews"]
                positive = ttl_cnt["positive"]
                negative = ttl_cnt["negative"]
                neutral = ttl_cnt["neutral"]
                updated_total_count_data ={
                "$set":
                {"Total_reviews": reviews+1,
                "positive":positive+pos,
                "negative":negative+neg,
                "neutral":neutral+neu,
                "updatedAt": datetime.datetime.now()
                }
                }
                Total_count.update_one(ttl_cnt,updated_total_count_data)
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

def twitter_past(q):
    for status in tweepy.Cursor(api.search, q,tweet_mode="extended").items(3000):
        tweet_id = status.id
        if status.full_text[:2] == "RT @ ":
            is_retweet = True
            user_id = status.retweeted_status.user.id
            user_geo = status.retweeted_status.geo
            if status.retweeted_status.truncated == True:
                tweet_txt = status.retweeted_status.extended_tweet['full_text']
            else:
                tweet_txt = status.retweeted_status.full_text

        elif status.truncated == True:
            is_retweet = False
            user_id = status.user.id
            user_geo = status.geo
            tweet_txt = status.extended_tweet['full_text']

        else:
            is_retweet = False
            user_id = status.user.id
            user_geo = status.geo
            tweet_txt = status.full_text
        created_at = status.created_at
        lang = status.lang
        retweet_count = status.retweet_count
        Sentiment = sentiment_analysis(tweet_txt)
        if Sentiment == "Positive":
            pos = 1
            neg = 0
            neu = 0
        elif Sentiment == "Negative":
            pos = 0
            neg = 1
            neu = 0
        else:
            pos = 0
            neg = 0
            neu = 1

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
                            "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                        }
        if (db.twitter.find({"tweet_id":tweet_id}).count() > 0)== False:
            Twitter.insert_one(twitter_data)
            count_data = {
            "tag" : q,
            "Total_reviews": 1,
            "positive":pos,
            "negative":neg,
            "neutral":neu,
            "createdAt": datetime.datetime.now(),
            "updatedAt": datetime.datetime.now()
            }
            if (db.twitter_count.find({'tag':q}).count() > 0)== False:
                Twitter_count.insert_one(count_data)
            else :
                tum_cnt = db.twitter_count.find_one({'tag':q})
                reviews = tum_cnt["Total_reviews"]
                positive = tum_cnt["positive"]
                negative = tum_cnt["negative"]
                neutral = tum_cnt["neutral"]
                updated_count_data ={
                "$set":
                {"Total_reviews": reviews+1,
                "positive":positive+pos,
                "negative":negative+neg,
                "neutral":neutral+neu,
                "updatedAt": datetime.datetime.now()
                }
                }
                Twitter_count.update_one(tum_cnt,updated_count_data)

            if (db.total_count.find({'tag':q}).count() > 0)== False:
                Total_count.insert_one(count_data)
            else:
                ttl_cnt = db.total_count.find_one({'tag':q})
                reviews = ttl_cnt["Total_reviews"]
                positive = ttl_cnt["positive"]
                negative = ttl_cnt["negative"]
                neutral = ttl_cnt["neutral"]
                updated_total_count_data ={
                "$set":
                {"Total_reviews": reviews+1,
                "positive":positive+pos,
                "negative":negative+neg,
                "neutral":neutral+neu,
                "updatedAt": datetime.datetime.now()
                }
                }
                Total_count.update_one(ttl_cnt,updated_total_count_data)


def twitter_stream(q,t=15*60):
    myStream = tweepy.Stream(auth=api.auth, listener=MyStreamListener(q=q,time_limit= t),tweet_mode="extended")
    myStream.filter(track=[q])
    return "Done"
