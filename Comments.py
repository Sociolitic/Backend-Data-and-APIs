from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import sys
import json
import pandas as pd
import datetime
import pymongo
from pymongo import MongoClient
from sentiment import *

client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
Youtube_comments = db.youTube_comment

def comments_(videoId):
    DEVELOPER_KEY = "AIzaSyCLa0LoJiVAWWEX-BH4prLyldw13r0AbUI"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,developerKey=DEVELOPER_KEY)

    vid_stats = youtube.videos().list(part="statistics",id=videoId).execute()
    comment_count = vid_stats.get("items")[0].get("statistics").get("commentCount")

    comment = []
    Author_name = []
    date = []
    Author_channel_id = []
    likes = []
    totalReplyCount = []
    replies_=[]
    sentiment_ = []
    count=0

    if comment_count == "0":
        youtube_dict = {'Comment':comment,'Author_name':Author_name,'Date':date,'Author_channel_id':Author_channel_id,'Likes':likes,'totalReplyCount':totalReplyCount}
        topics_data = pd.DataFrame(youtube_dict)
        return topics_data
    else:
        try:
            response=youtube.commentThreads().list(
            part='snippet,replies',
            videoId=videoId
            ).execute()
        except HttpError as err:
          if err.resp.status in [403, 500, 503]:
              youtube_dict = {'Comment':comment,'Author_name':Author_name,'Date':date,'Author_channel_id':Author_channel_id,'Likes':likes,'totalReplyCount':totalReplyCount}
              topics_data = pd.DataFrame(youtube_dict)
              return topics_data
          else: raise

        while response:
            for item in response['items']:
                comment.append(item['snippet']['topLevelComment']['snippet']['textDisplay'])
                Author_name.append(item['snippet']['topLevelComment']['snippet']['authorDisplayName'])
                if 'authorChannelId' in item['snippet']['topLevelComment']['snippet'].keys():
                    Author_channel_id.append(item['snippet']['topLevelComment']['snippet']['authorChannelId']['value'])
                else:
                    Author_channel_id.append("NO_ID")
                date.append(datetime.datetime.strptime(item['snippet']['topLevelComment']['snippet']['updatedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'))
                likes.append(item['snippet']['topLevelComment']['snippet']['likeCount'])
                totalReplyCount.append(item['snippet']['totalReplyCount'])
                Sentiment_ = sentiment_analysis(item['snippet']['topLevelComment']['snippet']['textDisplay'])
                sentiment_.append(sentiment_analysis(item['snippet']['topLevelComment']['snippet']['textDisplay']))
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
                "videoId":videoId,
                "comments":1,
                "positive":pos_,
                "negative":neg_,
                "neutral":neu_ ,
                "createdAt": datetime.datetime.now(),
                "updatedAt": datetime.datetime.now()
                }
                if (db.youTube_comment.find({'videoId':videoId}).count() > 0)== False:
                    Youtube_comments.insert_one(comment_data)
                else :
                    red_cmt = db.youTube_comment.find_one({'videoId':videoId})
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
                    Youtube_comments.update_one(red_cmt,updated_count_data_)

                reply = item['snippet']['totalReplyCount']
                # reply_text =[]
                # reply_likes = []
                # reply_date = []
                # reply_author = []
                # reply_channel_id = []
                # sentiment = []
                # if reply>0:
                #     for reply in item['replies']['comments']:
                #         reply_text.append(reply['snippet']['textDisplay'])
                #         reply_author.append(reply['snippet']['authorDisplayName'])
                #         reply_channel_id.append(reply['snippet']['authorChannelId']['value'])
                #         reply_date.append(datetime.datetime.strptime(reply['snippet']['updatedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'))
                #         reply_likes.append(reply['snippet']['likeCount'])
                #         sentiment.append(sentiment_analysis(reply['snippet']['textDisplay']))
                #         replies_dict = {'Comment':reply_text,'Author_name':reply_author,'Date':reply_date,'Author_channel_id':reply_channel_id,'Likes':reply_likes,'Sentiment':sentiment}
                #         new_replies_dict = [{"Comment":a, "Author_name":b , "Date":c, "Author_channel_id":d , "Likes":e , "Sentiment":f} for a, b,c,d,e,f in zip(replies_dict["Comment"], replies_dict["Author_name"], replies_dict["Date"], replies_dict["Author_channel_id"], replies_dict["Likes"],replies_dict['Sentiment'])]
                #         replies_.append(new_replies_dict)
                # else:
                #     replies_.append({})
            if 'nextPageToken' in response and count<=1:
                response = youtube.commentThreads().list(
                        part = 'snippet,replies',
                        videoId = videoId
                    ).execute()
                count+=1
            else:
                youtube_dict ={'Comment':comment,'Author_name':Author_name,'Date':date,'Author_channel_id':Author_channel_id,'Likes':likes,'totalReplyCount':totalReplyCount,'Sentiment':sentiment_}
                break
        topics_data = pd.DataFrame(youtube_dict)
        return youtube_dict
