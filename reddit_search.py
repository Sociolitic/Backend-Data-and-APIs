import praw
import pandas as pd
import datetime as dt
from praw.models import MoreComments
import sys
import json
import pymongo
from pymongo import MongoClient
import dns
import datetime
from sentiment import *
from dateutil.parser import parse


client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
Reddit = db.reddit

def reddithot(Search,number=100):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).hot(limit=number)
    top_subreddit = subreddit
    try:
        for submission in top_subreddit:
            if (db.reddit.find({"id":submission.id}).count() > 0)== False:
                comments = {"comment":[],"sentiment":[]}
                submissions = reddit.submission(submission.id)
                for top_level_comment in submissions.comments:
                    if isinstance(top_level_comment, MoreComments):
                        continue
                    comments["comment"].append(top_level_comment.body)
                    comments["sentiment"].append(sentiment_analysis(top_level_comment.body))
                Sentiment = sentiment_analysis(submission.title)
                reddit_data = {
                "source": "reddit",
                "text": submission.title,
                "id": submission.id,
                "tag" : Search,
                "sentiment" : Sentiment,
                "created_time" : parse(datetime.datetime.fromtimestamp(submission.created).strftime('%Y-%m-%d %H:%M:%S')),
                "misc":{"score": submission.score,
                "comments" : comments,
                "url": submission.url,
                "comments_num": submission.num_comments,
                "body": submission.selftext},
                "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                }
                Reddit.insert_one(reddit_data)
    except:
        pass

    return "Done"

def reddittop(Search,number=100):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).top(limit=number)
    top_subreddit = subreddit
    try:
        for submission in top_subreddit:
            if (db.reddit.find({"id":submission.id}).count() > 0)== False:
                comments = {"comment":[],"sentiment":[]}
                submissions = reddit.submission(submission.id)
                for top_level_comment in submissions.comments:
                    if isinstance(top_level_comment, MoreComments):
                        continue
                    comments["comment"].append(top_level_comment.body)
                    comments["sentiment"].append(sentiment_analysis(top_level_comment.body))
                Sentiment = sentiment_analysis(submission.title)
                reddit_data = {
                "source": "reddit",
                "text": submission.title,
                "id": submission.id,
                "tag" : Search,
                "sentiment" : Sentiment,
                "created_time" : parse(datetime.datetime.fromtimestamp(submission.created).strftime('%Y-%m-%d %H:%M:%S')),
                "misc":{"score": submission.score,
                "comments" : comments,
                "url": submission.url,
                "comments_num": submission.num_comments,
                "body": submission.selftext},
                "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                }
                Reddit.insert_one(reddit_data)
    except:
        pass
    return "Done"

def redditnew(Search,number=100):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).new(limit=number)
    top_subreddit = subreddit
    try:
        for submission in top_subreddit:
            if (db.reddit.find({"id":submission.id}).count() > 0)== False:
                comments = {"comment":[],"sentiment":[]}
                submissions = reddit.submission(submission.id)
                for top_level_comment in submissions.comments:
                    if isinstance(top_level_comment, MoreComments):
                        continue
                    comments["comment"].append(top_level_comment.body)
                    comments["sentiment"].append(sentiment_analysis(top_level_comment.body))

                Sentiment = sentiment_analysis(submission.title)
                reddit_data = {
                "source": "reddit",
                "text": submission.title,
                "id": submission.id,
                "tag" : Search,
                "sentiment" : Sentiment,
                "created_time" : parse(datetime.datetime.fromtimestamp(submission.created).strftime('%Y-%m-%d %H:%M:%S')),
                "misc":{"score": submission.score,
                "comments" : comments,
                "url": submission.url,
                "comments_num": submission.num_comments,
                "body": submission.selftext},
                "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                }
                Reddit.insert_one(reddit_data)
    except:
        pass

    return "Done"
