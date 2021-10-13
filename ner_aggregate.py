import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from collections import Counter

client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
YouTube = db.youTube
Twitter = db.twitter
Tumblr = db.tumblr
Instagram = db.instagram
Reddit = db.reddit


def ner_mentions(tag):
    twitter = Twitter.find({'tag': tag},{"ner","created_time"})
    twitter_data=[ [] for _ in range(71) ]
    for data in twitter:
        i=0
        for j in range (24,0,-1):
            if (data["created_time"] < datetime.now()- timedelta(hours = j-1) and data["created_time"] > datetime.now()- timedelta(hours = j)):
                twitter_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1
        i=24
        for j in range (30,0,-1):
            if (data["created_time"] < datetime.now()- timedelta(days = j-1) and data["created_time"] > datetime.now()- timedelta(days = j)):
                twitter_data[i].extend(list(set((data['ner']['ORG']))))


            i+=1
        i=54
        for j in range (12,0,-1):
            if (data["created_time"] < datetime.now()- relativedelta(months = j-1) and data["created_time"] > datetime.now()- relativedelta(months = j)):
                twitter_data[i].extend(list(set((data['ner']['ORG']))))


            i+=1
        i=66
        for j in range (5,0,-1):
            if (data["created_time"] < datetime.now()-relativedelta(years = j-1) and data["created_time"] > datetime.now()- relativedelta(years = j)):
                twitter_data[i].extend(list(set((data['ner']['ORG']))))


            i+=1
    reddit = Reddit.find({'tag': tag},{"ner","created_time"})
    reddit_data=[ [] for _ in range(71) ]
    for data in reddit:
        i=0
        for j in range (24,0,-1):
            if ((data["created_time"] )< datetime.now()- timedelta(hours = j-1) and (data["created_time"] )> datetime.now()- timedelta(hours = j)):
                reddit_data[i].extend(list(set((data['ner']['ORG']))))

            i+=1
        i=24
        for j in range (30,0,-1):
            if ((data["created_time"] )< datetime.now()- timedelta(days = j-1) and (data["created_time"] )> datetime.now()- timedelta(days = j)):
                reddit_data[i].extend(list(set((data['ner']['ORG']))))

            i+=1
        i=54
        for j in range (12,0,-1):
            if ((data["created_time"] )< datetime.now()- relativedelta(months = j-1) and (data["created_time"] )> datetime.now()- relativedelta(months = j)):
                reddit_data[i].extend(list(set((data['ner']['ORG']))))

            i+=1
        i=66
        for j in range (5,0,-1):
            if ((data["created_time"] )< datetime.now()-relativedelta(years = j-1) and (data["created_time"] )> datetime.now()- relativedelta(years = j)):
                reddit_data[i].extend(list(set((data['ner']['ORG']))))

            i+=1


    tumblr = Tumblr.find({'tag': tag},{"ner","created_time"})
    tumblr_data=[ [] for _ in range(71) ]
    for data in tumblr:
        i=0
        for j in range (24,0,-1):
            if ((data["created_time"] )< datetime.now()- timedelta(hours = j-1) and (data["created_time"] )> datetime.now()- timedelta(hours = j)):
                tumblr_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1
        i=24
        for j in range (30,0,-1):
            if ((data["created_time"] )< datetime.now()- timedelta(days = j-1) and (data["created_time"] )> datetime.now()- timedelta(days = j)):
                tumblr_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1
        i=54
        for j in range (12,0,-1):
            if ((data["created_time"] )< datetime.now()- relativedelta(months = j-1) and (data["created_time"] )> datetime.now()- relativedelta(months = j)):
                tumblr_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1
        i=66
        for j in range (5,0,-1):
            if ((data["created_time"] )< datetime.now()-relativedelta(years = j-1) and (data["created_time"] )> datetime.now()- relativedelta(years = j)):
                tumblr_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1
    youtube = YouTube.find({'tag': tag},{"ner","created_time"})
    youtube_data=[ [] for _ in range(71) ]
    for data in youtube:
        i=0
        for j in range (24,0,-1):
            if ((data["created_time"] )< datetime.now()- timedelta(hours = j-1) and (data["created_time"] )> datetime.now()- timedelta(hours = j)):
                youtube_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1
        i=24
        for j in range (30,0,-1):
            if ((data["created_time"] )< datetime.now()- timedelta(days = j-1) and (data["created_time"] )> datetime.now()- timedelta(days = j)):
                youtube_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1
        i=54
        for j in range (12,0,-1):
            if ((data["created_time"] )< datetime.now()- relativedelta(months = j-1) and (data["created_time"] )> datetime.now()- relativedelta(months = j)):
                youtube_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1
        i=66
        for j in range (5,0,-1):
            if ((data["created_time"] )< datetime.now()-relativedelta(years = j-1) and (data["created_time"] )> datetime.now()- relativedelta(years = j)):
                youtube_data[i].extend(list(set((data['ner']['ORG']))))
            i+=1

    for i in range (71):
        twitter_data[i]=dict(Counter([ x for x in twitter_data[i] if tag.lower() not in x.lower()]))
        youtube_data[i]=dict(Counter([ x for x in youtube_data[i] if tag.lower() not in x.lower()]))
        tumblr_data[i]=dict(Counter([ x for x in tumblr_data[i] if tag.lower() not in x.lower()]))
        reddit_data[i]=dict(Counter([ x for x in reddit_data[i] if tag.lower() not in x.lower()]))


    counts={
        'tag':tag,
        'all_mentions':{
        'twitter':{'hourly': twitter_data[0:24],
                 'daily':twitter_data[24:54],
                 'monthly':twitter_data[54:66],
                 'yearly':twitter_data[66:72]},
        'youtube':{'hourly': youtube_data[0:24],
                 'daily':youtube_data[24:54],
                 'monthly':youtube_data[54:66],
                 'yearly':youtube_data[66:72]},
        'reddit':{'hourly': reddit_data[0:24],
                 'daily':reddit_data[24:54],
                 'monthly':reddit_data[54:66],
                 'yearly':reddit_data[66:72]},
        'tumblr':{'hourly': tumblr_data[0:24],
                 'daily':tumblr_data[24:54],
                 'monthly':tumblr_data[54:66],
                 'yearly':tumblr_data[66:72]},
        }}
    return counts
