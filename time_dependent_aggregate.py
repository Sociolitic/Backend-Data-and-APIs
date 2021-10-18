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

def counter(dict_):
    for key in dict_.keys():
        dict_[key]=dict(Counter(dict_[key]))
    return correct_dict(dict_)

def merge_dicts(list_):
    x = {}
    for y in list_:
        x = { key:x.get(key,[])+y.get(key,[]) for key in set(list(x.keys())+list(y.keys())) }
    return x

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
                        "sources_reddit_total":reddit,
                        "sources_reddit_positive":reddit_pos,
                        "sources_reddit_negative":reddit_neg,
                        "sources_reddit_neutral":reddit_neu,
                        "sources_youtube_total":youtube,
                        "sources_youtube_positive":youtube_pos,
                        "sources_youtube_negative":youtube_neg,
                        "sources_youtube_neutral":youtube_neu,
                        "sources_tumblr_total":tumblr,
                        "sources_tumblr_positive":tumblr_pos,
                        "sources_tumblr_negative":tumblr_neg,
                        "sources_tumblr_neutral":tumblr_neu,
                        "sources_twitter_total":twitter,
                        "sources_twitter_positive":twitter_pos,
                        "sources_twitter_negative":twitter_neg,
                        "sources_twitter_neutral":twitter_neu
                        }
            return data,ner

def years_(l,tag):
        todays_date = datetime.today()
        years = []
        years_ner = []
        for i in range (l,0,-1):
            start = datetime((todays_date.year-i), 1, 1)
            end = datetime((todays_date.year-i), 12, 31)
            data,ner = data_(start,end,tag)
            years.insert(0, data)
            years_ner.insert(0, ner)
        years = dist_list(years)
        return(years,years_ner)

def months_(s,tag):
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
    months = dist_list(months)
    return(months,months_ner)

def days_(s,tag):
        days = []
        days_ner = []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.day):
            start = datetime(todays_date.year, todays_date.month,i,0,0,0)
            end =  datetime(todays_date.year, todays_date.month,i,23,59,59)
            data,ner = data_(start,end,tag)
            days.append(data)
            days_ner.append(ner)
        days = dist_list(days)
        return (days,days_ner)

def hours_(s,tag):
        hours = []
        hours_ner = []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.hour):
            start = datetime(todays_date.year, todays_date.month,todays_date.day,i,0,0)
            end =  datetime(todays_date.year, todays_date.month,todays_date.day,i,59,59)
            data,ner = data_(start,end,tag)
            hours.append(data)
            hours_ner.append(ner)
        hours = dist_list(hours)
        return (hours,hours_ner)

def minutes_(s,tag):
        minutes = []
        minutes_ner = []
        todays_date = datetime.today()
        for  i in  range (s,todays_date.minute):
            start = datetime(todays_date.year, todays_date.month,todays_date.day,todays_date.hour,i,0)
            end =  datetime(todays_date.year, todays_date.month,todays_date.day,todays_date.hour,i,59)
            data,ner = data_(start,end,tag)
            minutes.append(data)
            minutes_ner.append(ner)
        minutes = dist_list(minutes)
        return (minutes,minutes_ner)

def nest_dict(dict_):
    result = {}
    for k, v in dict_.items():
        _nest_dict_rec(k, v, result)
    return result

def correct_dict(d):
    new = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = correct_dict(v)
        new[k.replace('.', '_')] = v
    return new

def _nest_dict_rec(k, v, out):
    k, *rest = k.split('_', 1)
    if rest:
        _nest_dict_rec(rest[0], v, out.setdefault(k, {}))
    else:
        out[k] = v

def dist_list(List_dict):
    if List_dict==[]:
        return {}
    dict_ = {k: [dic[k] for dic in List_dict] for k in List_dict[0]}
    return nest_dict(dict_)

def merge(dict_):
    x = {}
    for k,v in dict_.items():
        if isinstance(v, dict):
            x[k]=merge(v)
        if isinstance(v, list):
            x[k]=[sum(v)]
    return x
def merge_dict(a,b):
    if a=={}:
        return b
    de = {}
    for i, j in a.items():
        if isinstance(j,list):
            a[i].extend(b[i])
            de[i]=a[i]
        if isinstance(j,dict):
            de[i] = merge_dict(j,b[i])
    return dict(de)

def cal_sum(lst):
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

def get_data(tag):
    if (db["aggregate"].find({'tag':tag}).count() > 0)== False:
        profiles_data = db["profile"].find({"brand":tag},{"_id"})
        profiles=[]
        for ele in profiles_data:
            profiles.append(str(ele["_id"]))
        years,years_ner = years_(5,tag)
        months,months_ner= months_(1,tag)
        days,days_ner= days_(1,tag)
        hours,hours_ner = hours_(0,tag)
        minutes,minutes_ner = minutes_(0,tag)
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
        time.sleep(45)
        while True:
            if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
                return "done"
            todays_date = datetime.today()
            if(todays_date.minute==0):
                minute = 59
                if(todays_date.hour==0):
                    hour = 23
                    if(todays_date.day==1):
                        day = len(days["total"])
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
            data , ner = data_(start,end,tag)
            minutes_ner.append(ner)
            minutes = merge_dict(minutes,dist_list([data]))
            if(len(minutes["total"])==60):
                hours=merge_dict(hours,merge(minutes))
                hours_ner.append(cal_sum(minutes_ner))
                minutes = {}
                minutes_ner = []
            if(len(hours["total"])==24):
                days=merge_dict(hours,merge(hours))
                days_ner.append(cal_sum(hours_ner))
                hours = {}
                hours_ner = []
            x,y = calendar.monthrange(year,month)
            if(len(days["total"])== y):
                months=merge_dict(hours,merge(days))
                months_ner.append(cal_sum(days_ner))
                days = {}
                days_ner = []
            if(len(months["total"])==12):
                years=merge_dict(hours,merge(months))
                years_ner.append(cal_sum(months_ner))
                months = {}
                months_ner = []
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
            time.sleep(45)
        return "done"
    else:
        if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
            return "done"
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
            data,ner=years_((todays_date.year-check_date.year),tag)
            years =merge_dict(years,(data))
            years_ner = (years_ner+ner)
            months,months_ner= months_(1,tag)
            todays_date = datetime.today()
            x,y = calendar.monthrange(todays_date.year, todays_date.month)
            days,days_ner = days_(1,y,tag)
            hours,hours_ner = hours_(0,tag)
            minutes,minutes_ner = minutes_(0,tag)
        elif (todays_date.month>check_date.month):
            data,ner = months_(len(months["total"]),tag)
            months = merge_dict(months,data)
            months_ner = months_ner+(ner)
            days,days_ner = days_(1,tag)
            hours,hours_ner = hours_(0,tag)
            minutes,minutes_ner = minutes_(0,tag)
        elif(todays_date.day>check_date.day):
            data,ner = days_(len(days["total"]),tag)
            days = merge_dict(days,data)
            days_ner = days_ner+(ner)
            hours,hours_ner = hours_(0,tag)
            minutes,minutes_ner = minutes_(0,tag)
        elif(todays_date.hour>check_date.hour):
            data,ner = hours_(len(hours["total"]),tag)
            hours = merge_dict(hours,data)
            hours_ner = hours_ner+ner
            minutes,minutes_ner = minutes_(0,tag)
        elif(todays_date.minute>check_date.minute+1):
            data,ner = minutes_(len(minutes["total"]),tag)
            minutes = merge_dict(minutes,data)
            minutes_ner = minutes_ner+ner
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
        time.sleep(45)
        while(True):
            if (db["aggregate"].find({"tag":tag,"profiles":[]}).count==0):
                return "done"
            todays_date = datetime.today()
            if(todays_date.minute==0):
                minute = 59
                if(todays_date.hour==0):
                    hour = 23
                    if(todays_date.day==1):
                        day = len(days["total"])
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
            data , ner = data_(start,end,tag)
            minutes_ner.append(ner)
            minutes = merge_dict(minutes,dist_list([data]))
            if(len(minutes["total"])==60):
                hours=merge_dict(hours,merge(minutes))
                hours_ner.append(cal_sum(minutes_ner))
                minutes = {}
                minutes_ner = []
            if(len(hours["total"])==24):
                days=merge_dict(hours,merge(hours))
                days_ner.append(cal_sum(hours_ner))
                hours = {}
                hours_ner = []
            x,y = calendar.monthrange(year,month)
            if(len(days["total"])== y):
                months=merge_dict(hours,merge(days))
                months_ner.append(cal_sum(days_ner))
                days = {}
                days_ner = []
            if(len(months["total"])==12):
                years=merge_dict(hours,merge(months))
                years_ner.append(cal_sum(months_ner))
                months = {}
                months_ner = []
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
            time.sleep(45)
        return "done"
