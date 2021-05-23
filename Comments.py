from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import sys
import json
import pandas as pd
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
                date.append(item['snippet']['topLevelComment']['snippet']['updatedAt'])
                likes.append(item['snippet']['topLevelComment']['snippet']['likeCount'])
                totalReplyCount.append(item['snippet']['totalReplyCount'])
                reply = item['snippet']['totalReplyCount']
                reply_text =[]
                reply_likes = []
                reply_date = []
                reply_author = []
                reply_channel_id = []
                if reply>0:
                    for reply in item['replies']['comments']:
                        reply_text.append(reply['snippet']['textDisplay'])
                        reply_author.append(reply['snippet']['authorDisplayName'])
                        reply_channel_id.append(reply['snippet']['authorChannelId']['value'])
                        reply_date.append(reply['snippet']['updatedAt'])
                        reply_likes.append(reply['snippet']['likeCount'])
                        replies_dict = {'Comment':reply_text,'Author_name':reply_author,'Date':reply_date,'Author_channel_id':reply_channel_id,'Likes':reply_likes}
                        new_replies_dict = [{"Comment":a, "Author_name":b , "Date":c, "Author_channel_id":d , "Likes":e} for a, b,c,d,e in zip(replies_dict["Comment"], replies_dict["Author_name"], replies_dict["Date"], replies_dict["Author_channel_id"], replies_dict["Likes"])]
                        replies_.append(new_replies_dict)
                else:
                    replies_.append({})
            if 'nextPageToken' in response and count<=1:
                response = youtube.commentThreads().list(
                        part = 'snippet,replies',
                        videoId = videoId
                    ).execute()
                count+=1
            else:
                youtube_dict = {'Comment':comment,'Author_name':Author_name,'Date':date,'Author_channel_id':Author_channel_id,'Likes':likes,'totalReplyCount':totalReplyCount}
                break
        topics_data = pd.DataFrame(youtube_dict)
        return topics_data
