import pymongo
from pymongo import MongoClient
import random
from datetime import datetime
import warnings
import calendar
from pymongo import MongoClient
import time
warnings.filterwarnings("ignore", category=DeprecationWarning)

client = pymongo.MongoClient("mongodb+srv://KokilaReddy:KokilaReddy@cluster0.5nrpf.mongodb.net/Sociolitic?retryWrites=true&w=majority")
db = client.Social_media_data
YouTube = db.youTube
Twitter = db.twitter
Tumblr = db.tumblr
Reddit = db.reddit
Aggregate = db.aggregate

def years_(s,l,tag):
        todays_date = datetime.today()
        years = []
        for i in range (l,s,-1):
            start = datetime((todays_date.year-i), 1, 1)
            end = datetime((todays_date.year-i), 12, 31)
            years.insert(0, data_(start,end,tag))
        return(years)

def months_(s,l,tag):
    months= []
    todays_date = datetime.today()
    for i in range (s,todays_date.month):
        x,y = calendar.monthrange(todays_date.year, i)
        start = start = datetime((todays_date.year), i, 1)
        end = datetime((todays_date.year), i, y)
        months.append(data_(start,end,tag))
    months = (months + l * ['x'])[:l]
    return(months)

def days_(s,l,tag):
        days= []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.day):
            start = datetime(todays_date.year, todays_date.month,i,0,0,0)
            end =  datetime(todays_date.year, todays_date.month,i,23,59,59)
            days.append(data_(start,end,tag))
        days = (days + l * ['x'])[:l]
        return days

def hours_(s,l,tag):
        hours = []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.hour):
            start = datetime(todays_date.year, todays_date.month,todays_date.day,i,0,0)
            end =  datetime(todays_date.year, todays_date.month,todays_date.day,i,59,59)
            hours.append(data_(start,end,tag))
        hours = (hours + l * ['x'])[:l]
        return hours

def minutes_(s,l,tag):
        minutes = []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.minute):
            start = datetime(todays_date.year, todays_date.month,todays_date.day,todays_date.hour,i,0)
            end =  datetime(todays_date.year, todays_date.month,todays_date.day,todays_date.hour,i,59)
            minutes.append(data_(start,end,tag))
        minutes = (minutes + l * ['x'])[:l]
        return minutes

def data_(start,end,tag):
            reddit_pos = reddit_neg = reddit_neu = youtube_pos = youtube_neg = youtube_neu = tumblr_pos = tumblr_neg = tumblr_neu = twitter_pos = twitter_neg = twitter_neu = 0
            reddit  = Reddit.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment"})
            for data in reddit:
                if data["sentiment"]=="Positive":
                    reddit_pos+=1
                elif data["sentiment"]=="Negative":
                    reddit_neg+=1
                else:
                    reddit_neu+=1
            youtube  = YouTube.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment"})
            for data in youtube:
                if data["sentiment"]=="Positive":
                    youtube_pos+=1
                elif data["sentiment"]=="Negative":
                    youtube_neg+=1
                else:
                    youtube_neu+=1
            tumblr  = Tumblr.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment"})
            for data in tumblr:
                if data["sentiment"]=="Positive":
                    tumblr_pos+=1
                elif data["sentiment"]=="Negative":
                    tumblr_neg+=1
                else:
                    tumblr_neu+=1
            twitter  = Twitter.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment"})
            for data in twitter:
                if data["sentiment"]=="Positive":
                    twitter_pos+=1
                elif data["sentiment"]=="Negative":
                    twitter_neg+=1
                else:
                    twitter_neu+=1
            reddit = reddit_neu+reddit_neg+reddit_pos
            youtube = youtube_pos+youtube_neg+youtube_neu
            tumblr = tumblr_neu+tumblr_neg+tumblr_pos
            twitter = twitter_pos+twitter_neg+twitter_neu
            data = {
                        "start":start,
                        "end":end,
                        "total":twitter+tumblr+youtube+reddit,
                        "positive":reddit_pos+youtube_pos+tumblr_pos+twitter_pos,
                        "negative":reddit_neg+youtube_neg+tumblr_neg+twitter_neg,
                        "neutral":reddit_neu+youtube_neu+tumblr_neu+twitter_neu,
                        "sources":{
                        "reddit":
                        {
                            "total":reddit,
                            "positive":reddit_pos,
                            "negative":reddit_neg,
                            "neutral":reddit_neu
                        },
                        "youtube":
                        {
                            "total":youtube,
                            "positive":youtube_pos,
                            "negative":youtube_neg,
                            "neutral":youtube_neu
                        },
                        "tumblr":
                        {
                            "total":tumblr,
                            "positive":tumblr_pos,
                            "negative":tumblr_neg,
                            "neutral":tumblr_neu
                        },
                        "twitter":
                        {
                            "total":twitter,
                            "positive":twitter_pos,
                            "negative":twitter_neg,
                            "neutral":twitter_neu
                        }
                        }
                    }
            return data

def merge_data(c):
    start = c[0]["start"]
    end = c[-1]["end"]
    c = [{k: v for k, v in d.items() if k not in ['start','end']} for d in c]
    c = merge(c)
    c["start"]=start
    c["end"]=end
    return c
def merge(c):
    _keys = {i for b in c for i in b}
    return {i:[sum, merge][isinstance(c[0][i], dict)]([h[i] for h in c]) for i in _keys}
def get_data(tag):
    if (db["aggregate"].find({'tag':tag}).count() > 0)== False:
        profiles_data = db["profile"].find({"brand":tag},{"_id"})
        profiles=[]
        for ele in profiles_data:
            profiles.append(str(ele["_id"]))
        years = years_(0,5,tag)
        months= months_(1,12,tag)
        todays_date = datetime.today()
        x,y = calendar.monthrange(todays_date.year, todays_date.month)
        days= days_(1,y,tag)
        hours = hours_(0,24,tag)
        minutes = minutes_(0,24,tag)
        output = {
            "tag":tag,
            "profiles":profiles,
            "years":years,
            "months":months,
            "days":days,
            "hours":hours,
            "mins":minutes,
            "createdAt": datetime.now(), "updatedAt": datetime.now()
            }
        db["aggregate"].insert_one(output)
        while True:
            if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
                return "done"
            todays_date = datetime.today()
            if(todays_date.minute==0):
                minute = 59
                if(todays_date.hour==0):
                    hour = 23
                    if(todays_date.day==1):
                        day = len(days)
                        if(todays_date.month==1):
                            month = 12
                            year = todays_date.year-1
                        else:
                            month= todays_date.month-1
                    else:
                        day = todays_date.day-1
                else:
                    hour = todays_date.hour-1
            else:
                year = todays_date.year
                month = todays_date.month
                day = todays_date.day
                hour = todays_date.hour
                minute = todays_date.minute-1
            start = datetime(year,month,day,hour,minute,0)
            end =  datetime(year,month,day,hour,minute,59)
            minutes[todays_date.minute-1] = data_(start,end,tag)
            if(minutes[-1]!='x'):
                hours[todays_date.hour-1] = merge_data(minutes)
                minutes = ['x']*60
            if(hours[-1]!='x'):
                days[todays_date.day-1] = merge_data(hours)
                hours = ['x']*24
            if(days[-1]!='x'):
                months[todays_date.month-1] = merge_data(days)
                x,y = calendar.monthrange(todays_date.year, todays_date.month)
                days = ['x']*y
            if(months[-1]!='x'):
                years.append(merge_data(months))
                months = ["x"] * 12
            output1 = {
                "years":years,
                "months":months,
                "days":days,
                "hours":hours,
                "mins":minutes,
                "updatedAt": datetime.now()
                }
            db["aggregate"].update_one({"tag":tag},{"$set":output1})
            time.sleep(50)
        return "done"
    else:
        if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
            return "done"
        output = db["aggregate"].find({"tag":tag})[0]
        check_date = output["updatedAt"]
        todays_date = datetime.now()
        years = output["years"]
        months = output["months"]
        days=output["days"]
        hours=output["hours"]
        minutes=output["mins"]
        if (todays_date.year>check_date.year):
            y=years_(0,(todays_date.year-check_date.year),tag)
            years =(years+(y))
            months= months_(1,12,tag)
            todays_date = datetime.today()
            x,y = calendar.monthrange(todays_date.year, todays_date.month)
            days= days_(1,y,tag)
            hours = hours_(0,24,tag)
            minutes = minutes_(0,60,tag)
        elif (todays_date.month>check_date.month):
            y=months_((months.index("x")+1),(12-months.index("x")),tag)
            months = months[:months.index("x")]+(y)
            x,y = calendar.monthrange(todays_date.year, todays_date.month)
            days= days_(1,y,tag)
            hours = hours_(0,24,tag)
            minutes = minutes_(0,60,tag)
        elif(todays_date.day>check_date.day):
            a,b = calendar.monthrange(todays_date.year, todays_date.month)
            y=days_((days.index("x")),(b+1-days.index("x")),tag)
            days = days[:days.index("x")]+(y)
            hours = hours_(0,24,tag)
            minutes = minutes_(0,60,tag)
        elif(todays_date.hour>check_date.hour):
            y=hours_((hours.index("x")),(25-hours.index("x")),tag)
            hours = hours[:hours.index("x")]+(y)
            minutes = minutes_(0,60,tag)
        elif(todays_date.minute>check_date.minute):
            y=minutes_((minutes.index("x")),(61-minutes.index("x")),tag)
            minutes = minutes[:minutes.index("x")]+(y)
        output1 = {
                "years":years,
                "months":months,
                "days":days,
                "hours":hours,
                "mins":minutes,
                "updatedAt": datetime.now()
                }
        db["aggregate"].update_one({"tag":tag},{"$set":output1})
        while True:
            if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
                return "done"
            todays_date = datetime.today()
            if(todays_date.minute==0):
                minute = 59
                if(todays_date.hour==0):
                    hour = 23
                    if(todays_date.day==1):
                        day = len(days)
                        if(todays_date.month==1):
                            month = 12
                            year = todays_date.year-1
                        else:
                            month= todays_date.month-1
                    else:
                        day = todays_date.day-1
                else:
                    hour = todays_date.hour-1
            else:
                year = todays_date.year
                month = todays_date.month
                day = todays_date.day
                hour = todays_date.hour
                minute = todays_date.minute-1
            start = datetime(year,month,day,hour,minute,0)
            end =  datetime(year,month,day,hour,minute,59)
            minutes[todays_date.minute-1] = data_(start,end,tag)
            if(minutes[-1]!='x'):
                hours[todays_date.hour-1] = merge_data(minutes)
                minutes = ['x']*60
            if(hours[-1]!='x'):
                days[todays_date.day-1] = merge_data(hours)
                hours = ['x']*24
            if(days[-1]!='x'):
                months[todays_date.month-1] = merge_data(days)
                x,y = calendar.monthrange(todays_date.year, todays_date.month)
                days = ['x']*y
            if(months[-1]!='x'):
                years.append(merge_data(months))
                months = ["x"] * 12
            output1 = {
                "years":years,
                "months":months,
                "days":days,
                "hours":hours,
                "mins":minutes,
                "updatedAt": datetime.now()
                }
            db["aggregate"].update_one({"tag":tag},{"$set":output1})
            time.sleep(45)
        return "done"
