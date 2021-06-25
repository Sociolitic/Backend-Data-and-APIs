from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import pandas as pd
import sys
from Comments import *
import pymongo
from pymongo import MongoClient
import dns
import datetime
from sentiment import *


client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
YouTube = db.youTube
Youtube_count = db.youtube_count
Total_count = db.total_count

def Category(Id):
    switcher = {
        1  : "Film & Animation",
        2  : "Autos & Vehicles",
        10 : "Music",
        15 : "Pets & Animals",
        17 : "Sports",
        19 : "Travel & Events",
        20 : "Gaming",
        22 : "People & Blogs",
        23 : "Comedy",
        24 : "Entertainment",
        25 : "News & Politics",
        26 : "Howto & Style",
        27 : "Education",
        28 : "Science & Technology",
        29 : "Nonprofits & Activism"
    }
    return switcher.get(Id, "None")

def youtube_search(q, max_results=50,order="relevance", token=None, location=None, location_radius=None):
    DEVELOPER_KEY = "AIzaSyCLa0LoJiVAWWEX-BH4prLyldw13r0AbUI"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,developerKey=DEVELOPER_KEY)

    search_response = youtube.search().list(
    q=q,
    type="video",
    pageToken=token,
    order = order,
    part="id,snippet", # Part signifies the different types of data you want
    maxResults=max_results,
    location=location,
    locationRadius=location_radius).execute()

    title = []
    channelId = []
    channelTitle = []
    categoryId = []
    videoId = []
    viewCount = []
    likeCount = []
    dislikeCount = []
    commentCount = []
    favoriteCount = []
    category = []
    tags = []
    videos = []
    comments = []
    time = []
    for search_result in search_response.get("items", []):
        if (db.youTube.find({'videoId':search_result['id']['videoId']}).count() > 0)== False:
            if search_result["id"]["kind"] == "youtube#video":
                response = youtube.videos().list(
                part="statistics,snippet", # Part signifies the different types of data you want
                id = search_result['id']['videoId']).execute()
                title.append(search_result['snippet']['title'])
                videoId.append(search_result['id']['videoId'])
                time.append(response['items'][0]['snippet']['publishedAt'])
                channelId.append(response['items'][0]['snippet']['channelId'])
                channelTitle.append(response['items'][0]['snippet']['channelTitle'])
                categoryId.append(response['items'][0]['snippet']['categoryId'])
                favoriteCount.append(response['items'][0]['statistics']['favoriteCount'])
                viewCount.append(response['items'][0]['statistics']['viewCount'])
                Sentiment = sentiment_analysis(search_result['snippet']['title'])
                if 'likeCount' in response['items'][0]['statistics'].keys():
                    LikeCount = response["items"][0]["statistics"]["likeCount"]
                    likeCount.append(response["items"][0]["statistics"]["likeCount"])
                else:
                    LikeCount = -1
                    likeCount.append("")
                if 'dislikeCount' in response['items'][0]['statistics'].keys():
                    DislikeCount = response["items"][0]["statistics"]["dislikeCount"]
                    dislikeCount.append(response["items"][0]["statistics"]["dislikeCount"])
                else:
                    DislikeCount = -1
                    dislikeCount.append("")
                if 'commentCount' in response['items'][0]["statistics"].keys():
                    CommentCount = response["items"][0]["statistics"]["commentCount"]
                    commentCount.append(response["items"][0]["statistics"]["commentCount"])
                else:
                    CommentCount = 0
                    commentCount.append(0)
                print(search_result['id']['videoId'])
                COMMENTS = comments_(search_result['id']['videoId'])
                comments.append(COMMENTS)
                if len(COMMENTS) == 0:
                    COMMENTS = {}

            if 'tags' in response['items'][0]['snippet'].keys():
                Tags = response['items'][0]['snippet']['tags']
                tags.append(response['items'][0]['snippet']['tags'])
            else:
                Tags = []
                tags.append([])

            youtube_data = {
            'tags': Tags,
            'channelId': str(response['items'][0]['snippet']['channelId']),
            'channelTitle': response['items'][0]['snippet']['channelTitle'],
            'publishedTime': datetime.datetime.strptime(response['items'][0]['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'),
            'categoryId':int(response['items'][0]['snippet']['categoryId']),
            'category':Category(int(response['items'][0]['snippet']['categoryId'])),
            'title':search_result['snippet']['title'],
            'videoId':str(search_result['id']['videoId']),
            'viewCount':int(response['items'][0]['statistics']['viewCount']),
            'likeCount':int(LikeCount),
            'dislikeCount':int(DislikeCount),
            'commentCount':int(CommentCount),
            'favoriteCount':int(response['items'][0]['statistics']['favoriteCount']),
            'comments': COMMENTS,
            "tag" : q,
            "sentiment":Sentiment,
            "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
            }

            YouTube.insert_one(youtube_data)
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
            count_data = {
            "tag" : q,
            "Total_reviews": 1,
            "positive":pos,
            "negative":neg,
            "neutral":neu,
            "createdAt": datetime.datetime.now(),
            "updatedAt": datetime.datetime.now()
            }
            if (db.youtube_count.find({'tag':q}).count() > 0)== False:
                Youtube_count.insert_one(count_data)
            else :
                tum_cnt = db.youtube_count.find_one({'tag':q})
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
                Youtube_count.update_one(tum_cnt,updated_count_data)

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


    youtube_dict = {'tags':tags,'channelId': channelId,'channelTitle': channelTitle,'publishedTime':time,'categoryId':categoryId,'title':title,'videoId':videoId,'viewCount':viewCount,'likeCount':likeCount,'dislikeCount':dislikeCount,'commentCount':commentCount,'favoriteCount':favoriteCount,'comments':comments}

    return youtube_dict

def Video_Search(Search):
    test = youtube_search(Search)
    df=pd.DataFrame(test)
    json=df.to_json(orient="records")
    return json
