from instagrapi import Client
import pymongo
from pymongo import MongoClient
import dns
import time
import datetime
from sentiment import *


client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
Instagram = db.instagram
Insta_count = db.instagram_count
Total_count = db.total_count


cl = Client()
cl.login("socio_litic", "Sociolitic")

def insta(search,amount = 200):
    top_medias = cl.hashtag_medias_top(search, amount=100)
    for media in top_medias:
        result = media.dict()
        if (db.instagram.find({"shortcode":result['code']}).count() > 0)== False:
            shortcode = result['code']
            postid = result['pk']
            userid = result['user']['pk']
            url = result['thumbnail_url']
            created_at = result['taken_at']
            if (result['location']!= None):
                location = result['location']['name']
                lat = result['location']['lat']
                lng = result['location']['lng']
            else:
                location = None
                lat = None
                lng = None
            comment_count = result['comment_count']
            like_count = result['like_count']
            user_name = result['user']['full_name']
            caption_text = result['caption_text']
            Sentiment = sentiment_analysis(caption_text)
            insta_data = {
            "publishedTime": created_at ,
            "postId": postid,
            "userId": userid,
            "username": user_name,
            "location": location,
            "lat": lat,
            "lng": lng,
            "caption": caption_text,
            "image_url": url,
            "shortcode" : shortcode,
            "likes": int(like_count),
            "comment_count":int(comment_count),
            "tag" : search,
            "sentiment":Sentiment,
            "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
            }
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

            Instagram.insert_one(insta_data)
            count_data = {
            "tag" : search,
            "Total_reviews": 1,
            "positive":pos,
            "negative":neg,
            "neutral":neu,
            "createdAt": datetime.datetime.now(),
            "updatedAt": datetime.datetime.now()
            }
            if (db.instagram_count.find({'tag':search}).count() > 0)== False:
                Insta_count.insert_one(count_data)
            else :
                tum_cnt = db.instagram_count.find_one({'tag':search})
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
                Insta_count.update_one(tum_cnt,updated_count_data)

            if (db.total_count.find({'tag':search}).count() > 0)== False:
                Total_count.insert_one(count_data)
            else:
                ttl_cnt = db.total_count.find_one({'tag':search})
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

    recent_medias = cl.hashtag_medias_recent(search,amount)
    for media in recent_medias:
        result = media.dict()
        if (db.instagram.find({"shortcode":result['code']}).count() > 0)== False:
            shortcode = result['code']
            postid = result['pk']
            userid = result['user']['pk']
            url = result['thumbnail_url']
            created_at = result['taken_at']
            if (result['location']!= None):
                location = result['location']['name']
                lat = result['location']['lat']
                lng = result['location']['lng']
            else:
                location = None
                lat = None
                lng = None
            comment_count = result['comment_count']
            like_count = result['like_count']
            user_name = result['user']['full_name']
            caption_text = result['caption_text']
            Sentiment = sentiment_analysis(caption_text)
            insta_data = {
            "publishedTime": created_at ,
            "postId": postid,
            "userId": userid,
            "username": user_name,
            "location": location,
            "lat": lat,
            "lng": lng,
            "caption": caption_text,
            "image_url": url,
            "shortcode" : shortcode,
            "likes": int(like_count),
            "comment_count":int(comment_count),
            "tag" : search,
            "sentiment":Sentiment,
            "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
            }
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

            Instagram.insert_one(insta_data)
            count_data = {
            "tag" : search,
            "Total_reviews": 1,
            "positive":pos,
            "negative":neg,
            "neutral":neu,
            "createdAt": datetime.datetime.now(),
            "updatedAt": datetime.datetime.now()
            }
            if (db.instagram_count.find({'tag':search}).count() > 0)== False:
                Insta_count.insert_one(count_data)
            else :
                tum_cnt = db.instagram_count.find_one({'tag':search})
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
                Insta_count.update_one(tum_cnt,updated_count_data)

            if (db.total_count.find({'tag':search}).count() > 0)== False:
                Total_count.insert_one(count_data)
            else:
                ttl_cnt = db.total_count.find_one({'tag':search})
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

    return "Done"








# import requests
# import json
# import pandas as pd
# import pymongo
# from pymongo import MongoClient
# import dns
# import time
# from datetime import datetime
# from dateutil.parser import parse
#
# client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
# db = client.Social_media_data
# Insta = db.instagram
#
# def insta(q):
#     try:
#         url = "http://localhost:8000/search/"+q
#         response = requests.request("GET", url)
#         response = json.loads(response.text)
#         publishedTime= []
#         postId = []
#         userId = []
#         isvideo = []
#         url = []
#         caption = []
#         accessibility_caption = []
#         likes = []
#         views = []
#         comment_count = []
#         source = []
#         tag = []
#         shortcode = []
#
#         for item in response:
#             publishedTime.append(item['node']['taken_at_timestamp'])
#             postId.append(item['node']['id'])
#             caption.append(item['node']['edge_media_to_caption']['edges'][0]['node']['text'])
#             userId.append(item['node']['owner']['id'])
#             likes.append(item['node']['edge_liked_by']['count'])
#             isvideo.append(item['node']['is_video'])
#             accessibility_caption.append(item['node']['accessibility_caption'])
#             url.append(item['node']['display_url'])
#             shortcode.append(item['node']['shortcode'])
#             if 'video_view_count' in item['node']:
#                 views.append(item['node']['video_view_count'])
#                 Views= int(item['node']['video_view_count'])
#             else:
#                 views.append("null")
#                 Views = "NIL"
#             comment_count.append(item['node']['edge_media_to_comment']['count'])
#             tag.append(q)
#             insta_data = {
#             "created_date": datetime.datetime.utcnow(),
#             "publishedTime": datetime.fromtimestamp(item['node']['taken_at_timestamp']).strftime('%Y-%m-%d %H:%M:%S') ,
#             "postId": str(item['node']['id']),
#             "userId": str(item['node']['owner']['id']),
#             "caption": item['node']['edge_media_to_caption']['edges'][0]['node']['text'],
#             "url": str(item['node']['display_url']),
#             "shortcode" : item['node']['shortcode'],
#             "likes": int(item['node']['edge_liked_by']['count']),
#             "views" : Views,
#             "is_video" : item['node']['is_video'],
#             "accessibility_caption":item['node']['accessibility_caption'],
#             "comment_count":int(item['node']['edge_media_to_comment']['count']),
#             "tag" : q,
#             "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
#             }
#             if (db.instagram.find(insta_data).count() > 0)== False:
#                 Insta.insert_one(insta_data)
#
#         insta_dict = {'publishedTime':publishedTime,'postId':postId,'userId':userId,'caption':caption,'url':url,'shortcode':shortcode,'likes':likes,'views':views,'is_video':isvideo,'accessibility_caption':accessibility_caption,'comment_count':comment_count,'tag':tag}
#         df=pd.DataFrame(insta_dict)
#         json_=df.to_json(orient="records")
#         return json_
#
#     except:
#         return "error"
