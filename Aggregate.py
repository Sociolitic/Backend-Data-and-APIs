import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
YouTube = db.youTube
Twitter = db.twitter
Tumblr = db.tumblr
Instagram = db.instagram
Reddit = db.reddit


def mentions(tag):
    twitter = Twitter.find({'tag': tag},{"sentiment","created_time"})
    twitter_count=[0]*71
    twitter_pos=[0]*71
    twitter_neg=[0]*71
    twitter_neu=[0]*71
    for data in twitter:
        for i in range (1,25):
            if (data["created_time"] < datetime.now()- timedelta(hours = i-1) and data["created_time"] > datetime.now()- timedelta(hours = i)):
                twitter_count[i-1]+=1
                if data["sentiment"]=="Positive":
                    twitter_pos[i-1]+=1
                elif data["sentiment"]=="Negative":
                    twitter_neg[i-1]+=1
                else:
                    twitter_neu[i-1]+=1
        i=24
        for j in range (1,31):
            if (data["created_time"] < datetime.now()- timedelta(days = j-1) and data["created_time"] > datetime.now()- timedelta(days = j)):
                twitter_count[i]+=1
                if data["sentiment"]=="Positive":
                    twitter_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    twitter_neg[i]+=1
                else:
                    twitter_neu[i]+=1
            i+=1
        i=54
        for j in range (1,13):
            if (data["created_time"] < datetime.now()- relativedelta(months = j-1) and data["created_time"] > datetime.now()- relativedelta(months = j)):
                twitter_count[i]+=1
                if data["sentiment"]=="Positive":
                    twitter_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    twitter_neg[i]+=1
                else:
                    twitter_neu[i]+=1
            i+=1
        i=66
        for j in range (1,5):
            if (data["created_time"] < datetime.now()-relativedelta(years = j-1) and data["created_time"] > datetime.now()- relativedelta(years = j)):
                twitter_count[i]+=1
                if data["sentiment"]=="Positive":
                    twitter_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    twitter_neg[i]+=1
                else:
                    twitter_neu[i]+=1
            i+=1
    reddit = Reddit.find({'tag': tag},{"sentiment","created_time"})
    reddit_count=[0]*71
    reddit_pos=[0]*71
    reddit_neg=[0]*71
    reddit_neu=[0]*71
    for data in reddit:
        for i in range (1,25):
            if ((data["created_time"] )< datetime.now()- timedelta(hours = i-1) and (data["created_time"] )> datetime.now()- timedelta(hours = i)):
                reddit_count[i-1]+=1
                if data["sentiment"]=="Positive":
                    reddit_pos[i-1]+=1
                elif data["sentiment"]=="Negative":
                    reddit_neg[i-1]+=1
                else:
                    reddit_neu[i-1]+=1
        i=24
        for j in range (1,31):
            if ((data["created_time"] )< datetime.now()- timedelta(days = j-1) and (data["created_time"] )> datetime.now()- timedelta(days = j)):
                reddit_count[i]+=1
                if data["sentiment"]=="Positive":
                    reddit_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    reddit_neg[i]+=1
                else:
                    reddit_neu[i]+=1
            i+=1
        i=54
        for j in range (1,13):
            if ((data["created_time"] )< datetime.now()- relativedelta(months = j-1) and (data["created_time"] )> datetime.now()- relativedelta(months = j)):
                reddit_count[i]+=1
                if data["sentiment"]=="Positive":
                    reddit_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    reddit_neg[i]+=1
                else:
                    reddit_neu[i]+=1
            i+=1
        i=66
        for j in range (1,5):
            if ((data["created_time"] )< datetime.now()-relativedelta(years = j-1) and (data["created_time"] )> datetime.now()- relativedelta(years = j)):
                reddit_count[i]+=1
                if data["sentiment"]=="Positive":
                    reddit_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    reddit_neg[i]+=1
                else:
                    reddit_neu[i]+=1
            i+=1
    tumblr = Tumblr.find({'tag': tag},{"sentiment","created_time"})
    tumblr_count=[0]*71
    tumblr_pos=[0]*71
    tumblr_neg=[0]*71
    tumblr_neu=[0]*71
    for data in tumblr:
        for i in range (1,25):
            if ((data["created_time"] )< datetime.now()- timedelta(hours = i-1) and (data["created_time"] )> datetime.now()- timedelta(hours = i)):
                tumblr_count[i-1]+=1
                if data["sentiment"]=="Positive":
                    tumblr_pos[i-1]+=1
                elif data["sentiment"]=="Negative":
                    tumblr_neg[i-1]+=1
                else:
                    tumblr_neu[i-1]+=1
        i=24
        for j in range (1,31):
            if ((data["created_time"] )< datetime.now()- timedelta(days = j-1) and (data["created_time"] )> datetime.now()- timedelta(days = j)):
                tumblr_count[i]+=1
                if data["sentiment"]=="Positive":
                    tumblr_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    tumblr_neg[i]+=1
                else:
                    tumblr_neu[i]+=1
            i+=1
        i=54
        for j in range (1,13):
            if ((data["created_time"] )< datetime.now()- relativedelta(months = j-1) and (data["created_time"] )> datetime.now()- relativedelta(months = j)):
                tumblr_count[i]+=1
                if data["sentiment"]=="Positive":
                    tumblr_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    tumblr_neg[i]+=1
                else:
                    tumblr_neu[i]+=1
            i+=1
        i=66
        for j in range (1,5):
            if ((data["created_time"] )< datetime.now()-relativedelta(years = j-1) and (data["created_time"] )> datetime.now()- relativedelta(years = j)):
                tumblr_count[i]+=1
                if data["sentiment"]=="Positive":
                    tumblr_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    tumblr_neg[i]+=1
                else:
                    tumblr_neu[i]+=1
            i+=1
    youtube = YouTube.find({'tag': tag},{"sentiment","created_time"})
    youtube_count=[0]*71
    youtube_pos=[0]*71
    youtube_neg=[0]*71
    youtube_neu=[0]*71
    for data in youtube:
        for i in range (1,25):
            if ((data["created_time"] )< datetime.now()- timedelta(hours = i-1) and (data["created_time"] )> datetime.now()- timedelta(hours = i)):
                youtube_count[i-1]+=1
                if data["sentiment"]=="Positive":
                    youtube_pos[i-1]+=1
                elif data["sentiment"]=="Negative":
                    youtube_neg[i-1]+=1
                else:
                    youtube_neu[i-1]+=1
        i=24
        for j in range (1,31):
            if ((data["created_time"] )< datetime.now()- timedelta(days = j-1) and (data["created_time"] )> datetime.now()- timedelta(days = j)):
                youtube_count[i]+=1
                if data["sentiment"]=="Positive":
                    youtube_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    youtube_neg[i]+=1
                else:
                    youtube_neu[i]+=1
            i+=1
        i=54
        for j in range (1,13):
            if ((data["created_time"] )< datetime.now()- relativedelta(months = j-1) and (data["created_time"] )> datetime.now()- relativedelta(months = j)):
                youtube_count[i]+=1
                if data["sentiment"]=="Positive":
                    youtube_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    youtube_neg[i]+=1
                else:
                    youtube_neu[i]+=1
            i+=1
        i=66
        for j in range (1,5):
            if ((data["created_time"] )< datetime.now()-relativedelta(years = j-1) and (data["created_time"] )> datetime.now()- relativedelta(years = j)):
                youtube_count[i]+=1
                if data["sentiment"]=="Positive":
                    youtube_pos[i]+=1
                elif data["sentiment"]=="Negative":
                    youtube_neg[i]+=1
                else:
                    youtube_neu[i]+=1
            i+=1

    total_positive=[0]*71
    total_negative=[0]*71
    total_neutral=[0]*71
    total=[0]*71
    for i in range (len(twitter_count)):
        total_positive[i]=twitter_pos[i]+reddit_pos[i]+tumblr_pos[i]+youtube_pos[i]
        total_negative[i]=twitter_neg[i]+reddit_neg[i]+tumblr_neg[i]+youtube_neg[i]
        total_neutral[i]=twitter_neu[i]+reddit_neu[i]+tumblr_neu[i]+youtube_neu[i]
        total[i]=total_positive[i]+total_negative[i]+total_neutral[i]

    counts={
        'tag':tag,
        'all_mentions':{
            'total':{'hourly': total[0:24],
                 'daily':total[24:54],
                 'monthly':total[54:66],
                 'yearly':total[66:72]},
        'twitter':{'hourly': twitter_count[0:24],
                 'daily':twitter_count[24:54],
                 'monthly':twitter_count[54:66],
                 'yearly':twitter_count[66:72]},
        'youtube':{'hourly': youtube_count[0:24],
                 'daily':youtube_count[24:54],
                 'monthly':youtube_count[54:66],
                 'yearly':youtube_count[66:72]},
        'reddit':{'hourly': reddit_count[0:24],
                 'daily':reddit_count[24:54],
                 'monthly':reddit_count[54:66],
                 'yearly':reddit_count[66:72]},
        'tumblr':{'hourly': tumblr_count[0:24],
                 'daily':tumblr_count[24:54],
                 'monthly':tumblr_count[54:66],
                 'yearly':tumblr_count[66:72]},
        },
        'positive_mentions':{
            'total':{'hourly': total_positive[0:24],
                 'daily':total_positive[24:54],
                 'monthly':total_positive[54:66],
                 'yearly':total_positive[66:72]},
        'twitter':{'hourly': twitter_pos[0:24],
                 'daily':twitter_pos[24:54],
                 'monthly':twitter_pos[54:66],
                 'yearly':twitter_pos[66:72]},
        'youtube':{'hourly': youtube_pos[0:24],
                 'daily':youtube_pos[24:54],
                 'monthly':youtube_pos[54:66],
                 'yearly':youtube_pos[66:72]},
        'reddit':{'hourly': reddit_pos[0:24],
                 'daily':reddit_pos[24:54],
                 'monthly':reddit_pos[54:66],
                 'yearly':reddit_pos[66:72]},
        'tumblr':{'hourly': tumblr_pos[0:24],
                 'daily':tumblr_pos[24:54],
                 'monthly':tumblr_pos[54:66],
                 'yearly':tumblr_pos[66:72]},
        },
        'negative_mentions':{
            'total':{'hourly': total_negative[0:24],
                 'daily':total_negative[24:54],
                 'monthly':total_negative[54:66],
                 'yearly':total_negative[66:72]},
        'twitter':{'hourly': twitter_neg[0:24],
                 'daily':twitter_neg[24:54],
                 'monthly':twitter_neg[54:66],
                 'yearly':twitter_neg[66:72]},
        'youtube':{'hourly': youtube_neg[0:24],
                 'daily':youtube_neg[24:54],
                 'monthly':youtube_neg[54:66],
                 'yearly':youtube_neg[66:72]},
        'reddit':{'hourly': reddit_neg[0:24],
                 'daily':reddit_neg[24:54],
                 'monthly':reddit_neg[54:66],
                 'yearly':reddit_neg[66:72]},
        'tumblr':{'hourly': tumblr_neg[0:24],
                 'daily':tumblr_neg[24:54],
                 'monthly':tumblr_neg[54:66],
                 'yearly':tumblr_neg[66:72]},
        },
        'neutral_mentions':{
            'total':{'hourly': total_neutral[0:24],
                 'daily':total_neutral[24:54],
                 'monthly':total_neutral[54:66],
                 'yearly':total_neutral[66:72]},
        'twitter':{'hourly': twitter_neu[0:24],
                 'daily':twitter_neu[24:54],
                 'monthly':twitter_neu[54:66],
                 'yearly':twitter_neu[66:72]},
        'youtube':{'hourly': youtube_neg[0:24],
                 'daily':youtube_neg[24:54],
                 'monthly':youtube_neg[54:66],
                 'yearly':youtube_neg[66:72]},
        'reddit':{'hourly': reddit_neu[0:24],
                 'daily':reddit_neu[24:54],
                 'monthly':reddit_neu[54:66],
                 'yearly':reddit_neu[66:72]},
        'tumblr':{'hourly': tumblr_neu[0:24],
                 'daily':tumblr_neu[24:54],
                 'monthly':tumblr_neu[54:66],
                 'yearly':tumblr_neu[66:72]},
        }
    }
    return counts
