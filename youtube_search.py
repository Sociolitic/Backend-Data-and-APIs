from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import pandas as pd
import pprint
import sys
from Comments import *

def youtube_search(q, max_results=50,order="relevance", token=None, location=None, location_radius=None):
    DEVELOPER_KEY = "API_KEY"
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

    for search_result in search_response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video":

            title.append(search_result['snippet']['title'])

            videoId.append(search_result['id']['videoId'])

            response = youtube.videos().list(
            part="statistics,snippet", # Part signifies the different types of data you want
            id = search_result['id']['videoId']).execute()

            channelId.append(response['items'][0]['snippet']['channelId'])
            channelTitle.append(response['items'][0]['snippet']['channelTitle'])
            categoryId.append(response['items'][0]['snippet']['categoryId'])
            favoriteCount.append(response['items'][0]['statistics']['favoriteCount'])
            viewCount.append(response['items'][0]['statistics']['viewCount'])
            if 'likeCount' in response['items'][0]['statistics'].keys():
                likeCount.append(response["items"][0]["statistics"]["likeCount"])
            else:
                likeCount.append("")
            if 'dislikeCount' in response['items'][0]['statistics'].keys():
                dislikeCount.append(response["items"][0]["statistics"]["dislikeCount"])
            else:
                dislikeCount.append("")
            if 'commentCount' in response['items'][0]["statistics"].keys():
                commentCount.append(response["items"][0]["statistics"]["commentCount"])
            else:
                commentCount.append("")
            print(search_result['id']['videoId'])
            comments.append(comments_(search_result['id']['videoId']))

        if 'tags' in response['items'][0]['snippet'].keys():
            tags.append(response['items'][0]['snippet']['tags'])
        else:
            tags.append([])

    youtube_dict = {'tags':tags,'channelId': channelId,'channelTitle': channelTitle,'categoryId':categoryId,'title':title,'videoId':videoId,'viewCount':viewCount,'likeCount':likeCount,'dislikeCount':dislikeCount,'commentCount':commentCount,'favoriteCount':favoriteCount,'comments':comments}

    return youtube_dict

def Video_Search(Search):
    test = youtube_search(Search)
    df=pd.DataFrame(test)
    json=df.to_json(orient="records")
    return json
