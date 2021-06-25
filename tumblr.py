import pytumblr
import pandas as pd
import pymongo
from pymongo import MongoClient
import dns
import datetime
import time
from sentiment import *

client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
Tumblr = db.tumblr
Tumblr_count = db.tumblr_count
Total_count = db.total_count

def tumblrsearch(search):
    client = pytumblr.TumblrRestClient(
      'KD3YxdO7uigsFn9v8iWD8KHEJIhlJpj4WUfDYXLX0KulX540Bg',
      'oACAMUWjAilXiIglpGh2ibZLJwwGrqRu4y9dpNJE28VjR4FLgu',
      'prHg38NXZQigKYFgxxKxLGO0ePkwksqLkjF9NI653ZbIKB27tx',
      'pdwCtHPdCZO1J5MxSkoWr4lunFjGx4gxbcUn4Y5Axi2b1BJScN'
    )
    output=client.tagged(tag=search,filter="text")
    blog_dict = {"blog_name":[],"id":[],"post_url":[],"timestamp":[],"tags":[],"title":[],"body":[],"summary":[]}

    for blog in output:
        if (db.tumblr.find({"id":blog["id"]}).count() > 0)== False:
            if "caption" not in blog:
                if 'title' in blog:
                    Title = blog["title"]
                    blog_dict["title"].append(blog["title"])
                else:
                    Title = ""
                    blog_dict['title'].append("")
                if "body" in blog:
                    Body = blog["body"]
                    blog_dict["body"].append(blog["body"])
                    Sentiment = sentiment_analysis(blog["body"])
                else:
                    Body= ""
                    blog_dict["body"].append("")
                    Sentiment = sentiment_analysis(blog["summary"])

            else:
                Title = "image"
                Body = blog["caption"]
                blog_dict['title'].append("image")
                blog_dict["body"].append(blog["caption"])
                Sentiment = sentiment_analysis(blog["caption"])
            blog_dict["summary"].append(blog["summary"])
            blog_dict["blog_name"].append(blog["blog_name"])
            blog_dict["id"].append(blog["id"])
            blog_dict["post_url"].append(blog["post_url"])
            blog_dict["timestamp"].append(blog["timestamp"])
            blog_dict["tags"].append(blog["tags"])
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

            tumblr_data = {
            "title": Title,
            "body": Body,
            "id": str(blog["id"]),
            "url": blog["post_url"],
            "blog_name": blog["blog_name"],
            "summary": blog["summary"],
            "created_time" : datetime.datetime.fromtimestamp(blog["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'),
            "tags": blog["tags"],
            "tag" : search,
            "sentiment":Sentiment,
            "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
            }
            
            Tumblr.insert_one(tumblr_data)
            count_data = {
            "tag" : search,
            "Total_reviews": 1,
            "positive":pos,
            "negative":neg,
            "neutral":neu,
            "createdAt": datetime.datetime.now(),
            "updatedAt": datetime.datetime.now()
            }
            if (db.tumblr_count.find({'tag':search}).count() > 0)== False:
                Tumblr_count.insert_one(count_data)
            else :
                tum_cnt = db.tumblr_count.find_one({'tag':search})
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
                Tumblr_count.update_one(tum_cnt,updated_count_data)

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


    topics_data = pd.DataFrame(blog_dict)
    json = topics_data.to_json(orient="records")
    return json
