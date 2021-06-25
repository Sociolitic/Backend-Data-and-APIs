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
Reddit_count = db.reddit_count
Total_count = db.total_count
Reddit_comments = db.reddit_comment

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
                    Sentiment_ = sentiment_analysis(top_level_comment.body)
                    if Sentiment_ == "Positive":
                        pos_ = 1
                        neg_ = 0
                        neu_ = 0
                    elif Sentiment_ == "Negative":
                        pos_ = 0
                        neg_ = 1
                        neu_ = 0
                    else:
                        pos_ = 0
                        neg_ = 0
                        neu_ = 1
                    comment_data = {
                    "postId":submission.id,
                    "comments":1,
                    "positive":pos_,
                    "negative":neg_,
                    "neutral":neu_ ,
                    "createdAt": datetime.datetime.now(),
                    "updatedAt": datetime.datetime.now()
                    }
                    if (db.reddit_comment.find({'postId':submission.id}).count() > 0)== False:
                        Reddit_comments.insert_one(comment_data)
                    else :
                        red_cmt = db.reddit_comment.find_one({'postId':submission.id})
                        reviews = red_cmt["comments"]
                        positive_ = red_cmt["positive"]
                        negative_ = red_cmt["negative"]
                        neutral_ = red_cmt["neutral"]
                        updated_count_data_ ={
                        "$set":
                        {"comments": reviews+1,
                        "positive":positive_+pos_,
                        "negative":negative_+neg_,
                        "neutral":neutral_+neu_,
                        "updatedAt": datetime.datetime.now()
                        }
                        }
                        Reddit_comments.update_one(red_cmt,updated_count_data_)
            #     print(comment)
                topics_dict["comments"].append(comments)

                Sentiment = sentiment_analysis(submission.title)
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
                count_data = {
                "tag" : Search,
                "Total_reviews": 1,
                "positive":pos,
                "negative":neg,
                "neutral":neu,
                "createdAt": datetime.datetime.now(),
                "updatedAt": datetime.datetime.now()
                }
                if (db.reddit_count.find({'tag':Search}).count() > 0)== False:
                    Reddit_count.insert_one(count_data)
                else :
                    tum_cnt = db.reddit_count.find_one({'tag':Search})
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
                    Reddit_count.update_one(tum_cnt,updated_count_data)


                if (db.total_count.find({'tag':Search}).count() > 0)== False:
                    Total_count.insert_one(count_data)
                else:
                    ttl_cnt = db.total_count.find_one({'tag':Search})
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
                    Sentiment_ = sentiment_analysis(top_level_comment.body)
                    if Sentiment_ == "Positive":
                        pos_ = 1
                        neg_ = 0
                        neu_ = 0
                    elif Sentiment_ == "Negative":
                        pos_ = 0
                        neg_ = 1
                        neu_ = 0
                    else:
                        pos_ = 0
                        neg_ = 0
                        neu_ = 1
                    comment_data = {
                    "postId":submission.id,
                    "comments":1,
                    "positive":pos_,
                    "negative":neg_,
                    "neutral":neu_ ,
                    "createdAt": datetime.datetime.now(),
                    "updatedAt": datetime.datetime.now()
                    }
                    if (db.reddit_comment.find({'postId':submission.id}).count() > 0)== False:
                        Reddit_comments.insert_one(comment_data)
                    else :
                        red_cmt = db.reddit_comment.find_one({'postId':submission.id})
                        reviews = red_cmt["comments"]
                        positive_ = red_cmt["positive"]
                        negative_ = red_cmt["negative"]
                        neutral_ = red_cmt["neutral"]
                        updated_count_data_ ={
                        "$set":
                        {"comments": reviews+1,
                        "positive":positive_+pos_,
                        "negative":negative_+neg_,
                        "neutral":neutral_+neu_,
                        "updatedAt": datetime.datetime.now()
                        }
                        }
                        Reddit_comments.update_one(red_cmt,updated_count_data_)
            #     print(comment)
                topics_dict["comments"].append(comments)

                Sentiment = sentiment_analysis(submission.title)
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
                count_data = {
                "tag" : Search,
                "Total_reviews": 1,
                "positive":pos,
                "negative":neg,
                "neutral":neu,
                "createdAt": datetime.datetime.now(),
                "updatedAt": datetime.datetime.now()
                }
                if (db.reddit_count.find({'tag':Search}).count() > 0)== False:
                    Reddit_count.insert_one(count_data)
                else :
                    tum_cnt = db.reddit_count.find_one({'tag':Search})
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
                    Reddit_count.update_one(tum_cnt,updated_count_data)


                if (db.total_count.find({'tag':Search}).count() > 0)== False:
                    Total_count.insert_one(count_data)
                else:
                    ttl_cnt = db.total_count.find_one({'tag':Search})
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
                    Sentiment_ = sentiment_analysis(top_level_comment.body)
                    if Sentiment_ == "Positive":
                        pos_ = 1
                        neg_ = 0
                        neu_ = 0
                    elif Sentiment_ == "Negative":
                        pos_ = 0
                        neg_ = 1
                        neu_ = 0
                    else:
                        pos_ = 0
                        neg_ = 0
                        neu_ = 1
                    comment_data = {
                    "postId":submission.id,
                    "comments":1,
                    "positive":pos_,
                    "negative":neg_,
                    "neutral":neu_ ,
                    "createdAt": datetime.datetime.now(),
                    "updatedAt": datetime.datetime.now()
                    }
                    if (db.reddit_comment.find({'postId':submission.id}).count() > 0)== False:
                        Reddit_comments.insert_one(comment_data)
                    else :
                        red_cmt = db.reddit_comment.find_one({'postId':submission.id})
                        reviews = red_cmt["comments"]
                        positive_ = red_cmt["positive"]
                        negative_ = red_cmt["negative"]
                        neutral_ = red_cmt["neutral"]
                        updated_count_data_ ={
                        "$set":
                        {"comments": reviews+1,
                        "positive":positive_+pos_,
                        "negative":negative_+neg_,
                        "neutral":neutral_+neu_,
                        "updatedAt": datetime.datetime.now()
                        }
                        }
                        Reddit_comments.update_one(red_cmt,updated_count_data_)
            #     print(comment)
                topics_dict["comments"].append(comments)

                Sentiment = sentiment_analysis(submission.title)
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
                count_data = {
                "tag" : Search,
                "Total_reviews": 1,
                "positive":pos,
                "negative":neg,
                "neutral":neu,
                "createdAt": datetime.datetime.now(),
                "updatedAt": datetime.datetime.now()
                }
                if (db.reddit_count.find({'tag':Search}).count() > 0)== False:
                    Reddit_count.insert_one(count_data)
                else :
                    tum_cnt = db.reddit_count.find_one({'tag':Search})
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
                    Reddit_count.update_one(tum_cnt,updated_count_data)


                if (db.total_count.find({'tag':Search}).count() > 0)== False:
                    Total_count.insert_one(count_data)
                else:
                    ttl_cnt = db.total_count.find_one({'tag':Search})
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
    except:
        pass

    topics_data = pd.DataFrame(topics_dict)
    json = topics_data.to_json(orient = "records")
    return json
