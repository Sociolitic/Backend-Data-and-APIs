from googleapiclient.discovery import build
import sys
import json
import pandas as pd


def Video_stats(videoId):
    DEVELOPER_KEY = "AIzaSyCLa0LoJiVAWWEX-BH4prLyldw13r0AbUI"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,developerKey=DEVELOPER_KEY)

    search_response = youtube.videos().list(
    part="statistics,snippet", # Part signifies the different types of data you want
    id = videoId).execute()


    published_date = search_response["items"][0]["snippet"]["publishedAt"]
    channelId = search_response["items"][0]["snippet"]["channelId"]
    title = search_response["items"][0]["snippet"]["title"]
    description = search_response["items"][0]["snippet"]["description"]
    channelTitle = search_response["items"][0]["snippet"]["channelTitle"]
    viewCount = search_response["items"][0]["statistics"]["viewCount"]
    if 'likeCount' in search_response['items'][0]['statistics'].keys():
        likeCount = search_response["items"][0]["statistics"]["likeCount"]
    else:
        likeCount = ""
    if 'dislikeCount' in search_response['items'][0]['statistics'].keys():
        dislikeCount = search_response["items"][0]["statistics"]["dislikeCount"]
    else:
        dislikeCount = ""
    if 'commentCount' in search_response['items'][0]['statistics'].keys():
        commentCount = search_response["items"][0]["statistics"]["commentCount"]
    else:
        commentCount = ""
    statistics_dict={"Date":published_date,"ChannelId":channelId,"Title":title,"Description":description,"Channel Title":channelTitle,"View count":viewCount,"Like count":likeCount,"Dislike Count":dislikeCount,"Comment count":commentCount}
    topics_data = pd.DataFrame(statistics_dict, index=[0])
    json = topics_data.to_json(orient = "records")
    return json
