import praw
import pandas as pd
import datetime as dt
from praw.models import MoreComments
import sys
import json

def reddithot(Search,number=10):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).hot(limit=number)
    top_subreddit = subreddit
    topics_dict = { "Source":[],\
                    "title":[], \
                    "score":[], \
                    "id":[], "url":[],"comms_num": [], \
                    "created": [], \
                    "body":[],"comments":[]}
    for submission in top_subreddit:
        comment = []
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
            comment.append(top_level_comment.body)
    #     print(comment)
        topics_dict["comments"].append(comment)

    topics_data = pd.DataFrame(topics_dict)
    json = topics_data.to_json(orient = "records")
    return json

def reddittop(Search,number=10):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).top(limit=number)
    top_subreddit = subreddit
    topics_dict = { "Source":[],\
                    "title":[], \
                    "score":[], \
                    "id":[], "url":[],"comms_num": [], \
                    "created": [], \
                    "body":[],"comments":[]}
    for submission in top_subreddit:
        comment = []
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
            comment.append(top_level_comment.body)
    #     print(comment)
        topics_dict["comments"].append(comment)

    topics_data = pd.DataFrame(topics_dict)
    json = topics_data.to_json(orient = "records")
    return json

def redditnew(Search,number=10):
    reddit = praw.Reddit(client_id='qReU5pXkg46LcA', client_secret='HmxBqKB7ua_rbNVW3_8BUAg3kvlE4Q', user_agent='media monitoring')
    subreddit = reddit.subreddit(Search).new(limit=number)
    top_subreddit = subreddit
    topics_dict = { "Source":[],\
                    "title":[], \
                    "score":[], \
                    "id":[], "url":[],"comms_num": [], \
                    "created": [], \
                    "body":[],"comments":[]}
    for submission in top_subreddit:
        comment = []
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
            comment.append(top_level_comment.body)
    #     print(comment)
        topics_dict["comments"].append(comment)

    topics_data = pd.DataFrame(topics_dict)
    json = topics_data.to_json(orient = "records")
    return json
