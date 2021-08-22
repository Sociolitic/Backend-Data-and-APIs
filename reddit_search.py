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


client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
Reddit = db.reddit

def reddithot(Search,number=100):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).hot(limit=number)
    top_subreddit = subreddit
    topics_dict = { "Source":[],\
                    "title":[], \
                    "score":[], \
                    "id":[], "url":[],"comms_num": [], \
                    "created": [], \
                    "body":[],"comments":[]}
    try:
        for submission in top_subreddit:
            if (db.reddit.find({"id":submission.id}).count() > 0)== False:
                comments = {"comment":[],"sentiment":[]}
                topics_dict["Source"].append("Reddit")
                topics_dict["title"].append(submission.title)
                topics_dict["score"].append(submission.score)
                topics_dict["id"].append(submission.id)
                topics_dict["url"].append(submission.url)
                topics_dict["comms_num"].append(submission.num_comments)
                topics_dict["created"].append(submission.created)
                topics_dict["body"].append(submission.selftext)
                submissions = reddit.submission(submission.id)
                for top_level_comment in submissions.comments:
                    if isinstance(top_level_comment, MoreComments):
                        continue
                    comments["comment"].append(top_level_comment.body)
                    comments["sentiment"].append(sentiment_analysis(top_level_comment.body))
                topics_dict["comments"].append(comments)
                Sentiment = sentiment_analysis(submission.title)
                reddit_data = {
                "title": submission.title,
                "score": submission.score,
                "id": submission.id,
                "url": submission.url,
                "comments_num": submission.num_comments,
                "Body": submission.selftext,
                "created_time" : datetime.datetime.fromtimestamp(submission.created).strftime('%Y-%m-%d %H:%M:%S'),
                "comments" : comments,
                "tag" : Search,
                "sentiment" : Sentiment,
                "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                }
                Reddit.insert_one(reddit_data)
    except:
        pass

    topics_data = pd.DataFrame(topics_dict)
    json = topics_data.to_json(orient = "records")
    return json

def reddittop(Search,number=100):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).top(limit=number)
    top_subreddit = subreddit
    topics_dict = { "Source":[],\
                    "title":[], \
                    "score":[], \
                    "id":[], "url":[],"comms_num": [], \
                    "created": [], \
                    "body":[],"comments":[]}
    try:
        for submission in top_subreddit:
            if (db.reddit.find({"id":submission.id}).count() > 0)== False:
                comments = {"comment":[],"sentiment":[]}
                topics_dict["Source"].append("Reddit")
                topics_dict["title"].append(submission.title)
                topics_dict["score"].append(submission.score)
                topics_dict["id"].append(submission.id)
                topics_dict["url"].append(submission.url)
                topics_dict["comms_num"].append(submission.num_comments)
                topics_dict["created"].append(submission.created)
                topics_dict["body"].append(submission.selftext)
                submissions = reddit.submission(submission.id)
                for top_level_comment in submissions.comments:
                    if isinstance(top_level_comment, MoreComments):
                        continue
                    comments["comment"].append(top_level_comment.body)
                    comments["sentiment"].append(sentiment_analysis(top_level_comment.body))
                topics_dict["comments"].append(comments)
                Sentiment = sentiment_analysis(submission.title)
                reddit_data = {
                "title": submission.title,
                "score": submission.score,
                "id": submission.id,
                "url": submission.url,
                "comments_num": submission.num_comments,
                "Body": submission.selftext,
                "created_time" : datetime.datetime.fromtimestamp(submission.created).strftime('%Y-%m-%d %H:%M:%S'),
                "comments" : comments,
                "tag" : Search,
                "sentiment" : Sentiment,
                "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                }
                Reddit.insert_one(reddit_data)
    except:
        pass

    topics_data = pd.DataFrame(topics_dict)
    json = topics_data.to_json(orient = "records")
    return json

def redditnew(Search,number=100):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).new(limit=number)
    top_subreddit = subreddit
    topics_dict = { "Source":[],\
                    "title":[], \
                    "score":[], \
                    "id":[], "url":[],"comms_num": [], \
                    "created": [], \
                    "body":[],"comments":[]}
    try:
        for submission in top_subreddit:
            if (db.reddit.find({"id":submission.id}).count() > 0)== False:
                comments = {"comment":[],"sentiment":[]}
                topics_dict["Source"].append("Reddit")
                topics_dict["title"].append(submission.title)
                topics_dict["score"].append(submission.score)
                topics_dict["id"].append(submission.id)
                topics_dict["url"].append(submission.url)
                topics_dict["comms_num"].append(submission.num_comments)
                topics_dict["created"].append(submission.created)
                topics_dict["body"].append(submission.selftext)
                submissions = reddit.submission(submission.id)
                for top_level_comment in submissions.comments:
                    if isinstance(top_level_comment, MoreComments):
                        continue
                    comments["comment"].append(top_level_comment.body)
                    comments["sentiment"].append(sentiment_analysis(top_level_comment.body))
                topics_dict["comments"].append(comments)

                Sentiment = sentiment_analysis(submission.title)
                reddit_data = {
                "title": submission.title,
                "score": submission.score,
                "id": submission.id,
                "url": submission.url,
                "comments_num": submission.num_comments,
                "Body": submission.selftext,
                "created_time" : datetime.datetime.fromtimestamp(submission.created).strftime('%Y-%m-%d %H:%M:%S'),
                "comments" : comments,
                "tag" : Search,
                "sentiment" : Sentiment,
                "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
                }
                Reddit.insert_one(reddit_data)
    except:
        pass

    topics_data = pd.DataFrame(topics_dict)
    json = topics_data.to_json(orient = "records")
    return json
