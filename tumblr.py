import pytumblr
import pandas as pd
import pymongo
from pymongo import MongoClient
import dns
import datetime
import time
from sentiment import *
from ner import *
from dateutil.parser import parse


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

    for blog in output:
         if (db.tumblr.find({"id":blog["id"]}).count() > 0)== False:
            if "caption" not in blog:
                if 'title' in blog:
                    Title = blog["title"]
                else:
                    Title = ""
                if "body" in blog:
                    Body = blog["body"]
                    Sentiment = sentiment_analysis(blog["body"])
                else:
                    Body= ""
                    Sentiment = sentiment_analysis(blog["summary"])

            else:
                Title = "image"
                Body = blog["caption"]
                Sentiment = sentiment_analysis(blog["caption"])
            if Title == "" or Title == "image":
                if Body != "":
                    Title = Body
                else:
                    Title = blog["summary"]
            tumblr_data = {
            "source":"tumblr",
            "text": Title,
            "id": str(blog["id"]),
            "sentiment":Sentiment,
            "tag" : search,
            "created_time" : parse(datetime.datetime.fromtimestamp(blog["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')),
            "ner": tags(Title),
            "url": blog["post_url"],
            "spam":False,
            "misc":{"blog_name": blog["blog_name"],
            "body": Body,
            "tags": blog["tags"],
            "summary": blog["summary"],},
            "createdAt": datetime.datetime.now(), "updatedAt": datetime.datetime.now()
            }

            Tumblr.insert_one(tumblr_data)

    return "Done"
