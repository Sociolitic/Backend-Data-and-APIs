import pymongo
from pymongo import MongoClient
import random
from datetime import datetime
import warnings
import calendar
from pymongo import MongoClient
import time
from collections import Counter
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
        years_ner = []
        for i in range (l,s,-1):
            start = datetime((todays_date.year-i), 1, 1)
            end = datetime((todays_date.year-i), 12, 31)
            data,ner = data_(start,end,tag)
            years.insert(0, data)
            years_ner.insert(0, ner)
        return(years,years_ner)

def months_(s,l,tag):
    months= []
    months_ner = []
    todays_date = datetime.today()
    for i in range (s,todays_date.month):
        x,y = calendar.monthrange(todays_date.year, i)
        start = start = datetime((todays_date.year), i, 1)
        end = datetime((todays_date.year), i, y)
        data,ner = data_(start,end,tag)
        months.append(data)
        months_ner.append(ner)
    months = (months + l * ['x'])[:l]
    months_ner = (months_ner + l * ['x'])[:l]
    return(months,months_ner)

def days_(s,l,tag):
        days = []
        days_ner = []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.day):
            start = datetime(todays_date.year, todays_date.month,i,0,0,0)
            end =  datetime(todays_date.year, todays_date.month,i,23,59,59)
            data,ner = data_(start,end,tag)
            days.append(data)
            days_ner.append(ner)
        days = (days + l * ['x'])[:l]
        days_ner = (days_ner + l * ['x'])[:l]
        return (days,days_ner)

def hours_(s,l,tag):
        hours = []
        hours_ner = []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.hour):
            start = datetime(todays_date.year, todays_date.month,todays_date.day,i,0,0)
            end =  datetime(todays_date.year, todays_date.month,todays_date.day,i,59,59)
            data,ner = data_(start,end,tag)
            hours.append(data)
            hours_ner.append(ner)
        hours = (hours + l * ['x'])[:l]
        hours_ner = (hours_ner + l * ['x'])[:l]
        return (hours,hours_ner)

def minutes_(s,l,tag):
        minutes = []
        minutes_ner = []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.minute):
            start = datetime(todays_date.year, todays_date.month,todays_date.day,todays_date.hour,i,0)
            end =  datetime(todays_date.year, todays_date.month,todays_date.day,todays_date.hour,i,59)
            data,ner = data_(start,end,tag)
            minutes.append(data)
            minutes_ner.append(ner)
        minutes = (minutes + l * ['x'])[:l]
        minutes_ner = (minutes_ner + l * ['x'])[:l]
        return (minutes,minutes_ner)

def data_(start,end,tag):
            reddit_pos = reddit_neg = reddit_neu = youtube_pos = youtube_neg = youtube_neu = tumblr_pos = tumblr_neg = tumblr_neu = twitter_pos = twitter_neg = twitter_neu = 0
            reddit_ner={}
            youtube_ner={}
            tumblr_ner = {}
            twitter_ner ={}
            reddit  = Reddit.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment","ner"})
            for data in reddit:
                reddit_ner = {key:reddit_ner.get(key,[])+data["ner"].get(key,[]) for key in set(list(reddit_ner.keys())+list(data["ner"].keys())) }
                if data["sentiment"]=="Positive":
                    reddit_pos+=1
                elif data["sentiment"]=="Negative":
                    reddit_neg+=1
                else:
                    reddit_neu+=1

            youtube  = YouTube.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment","ner"})
            for data in youtube:
                youtube_ner = {key:youtube_ner.get(key,[])+data["ner"].get(key,[]) for key in set(list(youtube_ner.keys())+list(data["ner"].keys())) }
                if data["sentiment"]=="Positive":
                    youtube_pos+=1
                elif data["sentiment"]=="Negative":
                    youtube_neg+=1
                else:
                    youtube_neu+=1

            tumblr  = Tumblr.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment","ner"})
            for data in tumblr:
                tumblr_ner = {key:tumblr_ner.get(key,[])+data["ner"].get(key,[]) for key in set(list(tumblr_ner.keys())+list(data["ner"].keys())) }
                if data["sentiment"]=="Positive":
                    tumblr_pos+=1
                elif data["sentiment"]=="Negative":
                    tumblr_neg+=1
                else:
                    tumblr_neu+=1

            twitter  = Twitter.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment","ner"})
            for data in twitter:
                twitter_ner = {key:twitter_ner.get(key,[])+data["ner"].get(key,[]) for key in set(list(twitter_ner.keys())+list(data["ner"].keys())) }
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
            ner = {
                "total": counter(merge_dicts([reddit_ner,twitter_ner,tumblr_ner,youtube_ner])),
                "reddit":counter(reddit_ner),
                "twitter":counter(twitter_ner),
                "tumblr":counter(tumblr_ner),
                "youtube":counter(youtube_ner)
            }
            data = {
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
            return data,ner

def correct_dict(d):
    new = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = correct_dict(v)
        new[k.replace('.', '_')] = v
    return new

def counter(dict_):
    for key in dict_.keys():
        dict_[key]=dict(Counter(dict_[key]))
    return correct_dict(dict_)

def merge_dicts(list_):
    x = {}
    for y in list_:
        x = { key:x.get(key,[])+y.get(key,[]) for key in set(list(x.keys())+list(y.keys())) }
    return x


def merge(c):
    _keys = {i for b in c for i in b}
    return {i:[sum, merge][isinstance(c[0][i], dict)]([h[i] for h in c]) for i in _keys}

def merge_ner(lst):
    final_dict = dict()
    for l in lst:
        sum_(final_dict,l)
    return final_dict

def sum_(final_dict,iter_dict):
    for k, v in iter_dict.items():
        if isinstance(v, dict):
            sum_(final_dict.setdefault(k, dict()), v)
        elif isinstance(v, int):
            final_dict[k] = final_dict.get(k, 0) + v

def insert_data(tag):
    if (db["aggregate"].find({'tag':tag}).count() > 0)== False:
        print("aggregate started")
        profiles_data = db["profile"].find({"brand":tag},{"_id"})
        profiles=[]
        for ele in profiles_data:
            profiles.append(str(ele["_id"]))
        years,years_ner = years_(0,5,tag)
        months,months_ner= months_(1,12,tag)
        todays_date = datetime.today()
        x,y = calendar.monthrange(todays_date.year, todays_date.month)
        days,days_ner= days_(1,y,tag)
        hours,hours_ner = hours_(0,24,tag)
        minutes,minutes_ner = minutes_(0,60,tag)
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
        output_ner = {
            "tag":tag,
            "profiles":profiles,
            "years":years_ner,
            "months":months_ner,
            "days":days_ner,
            "hours":hours_ner,
            "mins":minutes_ner,
            "createdAt": datetime.now(), "updatedAt": datetime.now()
            }
        db["aggregate"].insert_one(output)
        db["ner_aggregate"].insert_one(output_ner)
        while True:
            if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
                return "No profiles are monitoring"
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
            minutes[todays_date.minute-1] , minutes_ner[todays_date.minute-1] = data_(start,end,tag)
            if(minutes[-1]!='x'):
                hours[todays_date.hour-1] = merge(minutes)
                hours_ner[todays_date.hour-1] = merge_ner(minutes_ner)
                minutes = ['x']*60
                minutes_ner = ['x']*60
            if(hours[-1]!='x'):
                days[todays_date.day-1] = merge(hours)
                days_ner[todays_date.day-1] = merge_ner(hours_ner)
                hours = ['x']*24
                hours_ner = ['x']*24
            if(days[-1]!='x'):
                months[todays_date.month-1] = merge(days)
                months_ner[todays_date.month-1] = merge_ner(days_ner)
                x,y = calendar.monthrange(todays_date.year, todays_date.month)
                days = ['x']*y
                days_ner = ['x']*y
            if(months[-1]!='x'):
                years.append(merge(months))
                years_ner.append(merge_ner(months_ner))
                months = ["x"] * 12
                months_ner = ["x"] * 12
            output1 = {
                "years":years,
                "months":months,
                "days":days,
                "hours":hours,
                "mins":minutes,
                "updatedAt": datetime.now()
                }
            output_ner = {
                "years":years_ner,
                "months":months_ner,
                "days":days_ner,
                "hours":hours_ner,
                "mins":minutes_ner,
                "updatedAt": datetime.now()
                }
            db["aggregate"].update_one({"tag":tag},{"$set":output1})
            db["ner_aggregate"].update_one({"tag":tag},{"$set":output_ner})
        return "done"
    else:
        if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
            return "No profiles are monitoring"
        print("Aggregate is updating")
        output = db["aggregate"].find({"tag":tag})[0]
        output_ner = db["ner_aggregate"].find({"tag":tag})[0]
        check_date = output["updatedAt"]
        todays_date = datetime.now()
        years = output["years"]
        months = output["months"]
        days=output["days"]
        hours=output["hours"]
        minutes=output["mins"]
        years_ner = output_ner["years"]
        months_ner = output_ner["months"]
        days_ner = output_ner["days"]
        hours_ner = output_ner["hours"]
        minutes_ner = output_ner["mins"]
        if (todays_date.year>check_date.year):
            data,ner=years_(0,(todays_date.year-check_date.year),tag)
            years =(years+(data))
            years_ner = (years_ner+ner)
            months,months_ner= months_(1,12,tag)
            todays_date = datetime.today()
            x,y = calendar.monthrange(todays_date.year, todays_date.month)
            days,days_ner = days_(1,y,tag)
            hours,hours_ner = hours_(0,24,tag)
            minutes,minutes_ner = minutes_(0,60,tag)
        elif (todays_date.month>check_date.month):
            data,ner = months_((months.index("x")+1),(12-months.index("x")),tag)
            months = months[:months.index("x")]+(data)
            months_ner = months_ner[:months.index("x")]+(ner)
            x,y = calendar.monthrange(todays_date.year, todays_date.month)
            days,days_ner = days_(1,y,tag)
            hours,hours_ner = hours_(0,24,tag)
            minutes,minutes_ner = minutes_(0,60,tag)
        elif(todays_date.day>check_date.day):
            a,b = calendar.monthrange(todays_date.year, todays_date.month)
            data,ner = days_((days.index("x")),(b+1-days.index("x")),tag)
            days = days[:days.index("x")]+(data)
            days_ner = days_ner[:days.index("x")]+(ner)
            hours,hours_ner = hours_(0,24,tag)
            minutes,minutes_ner = minutes_(0,60,tag)
        elif(todays_date.hour>check_date.hour):
            data,ner = hours_((hours.index("x")),(25-hours.index("x")),tag)
            hours = hours[:hours.index("x")]+(data)
            hours_ner = hours_ner[:hours.index("x")]+(ner)
            minutes,minutes_ner = minutes_(0,60,tag)
        elif(todays_date.minute>check_date.minute):
            if "x" in minutes:
                data,ner = minutes_((minutes.index("x")),(61-minutes.index("x")),tag)
                minutes = minutes[:minutes.index("x")]+(data)
                minutes_ner = minutes_ner[:minutes.index("x")]+(ner)
            else:
                minutes,minutes_ner = minutes_(0,60,tag)
        output1 = {
                "years":years,
                "months":months,
                "days":days,
                "hours":hours,
                "mins":minutes,
                "updatedAt": datetime.now()
                }
        output_ner = {
            "years":years_ner,
            "months":months_ner,
            "days":days_ner,
            "hours":hours_ner,
            "mins":minutes_ner,
            "updatedAt": datetime.now()
            }
        db["aggregate"].update_one({"tag":tag},{"$set":output1})
        db["ner_aggregate"].update_one({"tag":tag},{"$set":output_ner})
        while True:
            if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
                return "No profiles are monitoring"
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
            minutes[todays_date.minute-1] , minutes_ner[todays_date.minute-1] = data_(start,end,tag)
            if(minutes[-1]!='x'):
                hours[todays_date.hour-1] = merge(minutes)
                hours_ner[todays_date.hour-1] = merge_ner(minutes_ner)
                minutes = ['x']*60
                minutes_ner = ['x']*60
            if(hours[-1]!='x'):
                days[todays_date.day-1] = merge(hours)
                days_ner[todays_date.day-1] = merge_ner(hours_ner)
                hours = ['x']*24
                hours_ner = ['x']*24
            if(days[-1]!='x'):
                months[todays_date.month-1] = merge(days)
                months_ner[todays_date.month-1] = merge_ner(days_ner)
                x,y = calendar.monthrange(todays_date.year, todays_date.month)
                days = ['x']*y
                days_ner = ['x']*y
            if(months[-1]!='x'):
                years.append(merge(months))
                years_ner.append(merge_ner(months_ner))
                months = ["x"] * 12
                months_ner = ["x"] * 12
            output1 = {
                "years":years,
                "months":months,
                "days":days,
                "hours":hours,
                "mins":minutes,
                "updatedAt": datetime.now()
                }
            output_ner = {
                "years":years_ner,
                "months":months_ner,
                "days":days_ner,
                "hours":hours_ner,
                "mins":minutes_ner,
                "updatedAt": datetime.now()
                }
            db["aggregate"].update_one({"tag":tag},{"$set":output1})
            db["ner_aggregate"].update_one({"tag":tag},{"$set":output_ner})
        return "done"

def dist_list(List_dict):
    if List_dict==[]:
        return {}
    dict_ = {k: [dic[k] for dic in List_dict] for k in List_dict[0]}
    for k,v in dict_.items():
        if isinstance(v[0],dict):
            dict_[k] = dist_list(v)
    return dict_

def get_data(tag):
    data = db["aggregate"].find({"tag":tag})
    if (data.count==0):
        return("Aggregate isn't started yet")
    else:
        data = data[0]
        Data = {
            "_id":str(data['_id']),
            "tag":data['tag'],
            "profiles":data["profiles"],
            "years":dist_list(data["years"]),
            "months":dist_list(data["months"][:data["months"].index("x")]),
            "days":dist_list(data["days"][:data["days"].index("x")]),
            "hours":dist_list(data["hours"][:data["hours"].index("x")]),
            "mins":dist_list(data["mins"][:data["mins"].index("x")]),
            "createdAt":data["createdAt"],
            "updatedAt":data["updatedAt"]
        }
        return Data
