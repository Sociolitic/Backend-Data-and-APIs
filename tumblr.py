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

    topics_data = pd.DataFrame(blog_dict)
    json = topics_data.to_json(orient="records")
    return json
