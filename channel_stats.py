from googleapiclient.discovery import build
import sys
import json
import pandas as pd


def Channel_stats(channelId):
	DEVELOPER_KEY = "AIzaSyCLa0LoJiVAWWEX-BH4prLyldw13r0AbUI"
	YOUTUBE_API_SERVICE_NAME = "youtube"
	YOUTUBE_API_VERSION = "v3"

	youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,developerKey=DEVELOPER_KEY)

	search_response = youtube.channels().list(
	part="statistics", # Part signifies the different types of data you want
	id = channelId
	).execute()

	viewCount = search_response["items"][0]["statistics"]["viewCount"]
	subscriberCount = search_response["items"][0]["statistics"]["subscriberCount"]
	hiddenSubscriberCount = search_response["items"][0]["statistics"]["hiddenSubscriberCount"]
	videoCount = search_response["items"][0]["statistics"]["videoCount"]

	statistics_dict={"viewCount":viewCount,"subscriberCount":subscriberCount,"hiddenSubscriberCount":hiddenSubscriberCount,"videoCount":videoCount}
	topics_data = pd.DataFrame(statistics_dict, index=[0])
	json = topics_data.to_json(orient = "records")
	return json
