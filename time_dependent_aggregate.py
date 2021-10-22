import pymongo
from pymongo import MongoClient
import random
from datetime import datetime,timedelta
import warnings
import calendar
from pymongo import MongoClient
import time
from collections import Counter
import requests
import json
warnings.filterwarnings("ignore", category=DeprecationWarning)
headers = {'content-type': 'application/json'}
url = 'http://master:8080/brand/alert'

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

def refine_list(lst):
    i=0
    while(i<len(lst)):
        j=i+1
        while(j<len(lst)):
            if (lst[i]["source"]==lst[j]["source"] and lst[i]['tag']==lst[j]['tag'] and lst[i]['phrase']==lst[j]['phrase'] and lst[i]['sentiment']==lst[j]['sentiment']):
                lst[i]['count']+=lst[j]['count']
                if lst[i]['created_date']>lst[j]['created_date']:
                    lst[i]['created_date']=lst[j]['created_date']
                lst.remove(lst[j])
            j+=1
        i+=1
    newlist = sorted(lst, key=lambda d: d['count'],reverse = True)
    return newlist[:20]

def ner_data(NER,source,sentiment,date):
    Data = []
    for k,v in NER.items():
        if (len(v)>0):
            for i in v:
                data = {"source":source,
                         "tag":k,
                         "phrase":i,
                         "sentiment":sentiment,
                         "created_date":date,
                         "count":1}
                Data.append(data)
    return Data

def data_(start,end,tag):
            reddit_pos = reddit_neg = reddit_neu = youtube_pos = youtube_neg = youtube_neu = tumblr_pos = tumblr_neg = tumblr_neu = twitter_pos = twitter_neg = twitter_neu = 0
            NER_data=[]
            reddit  = Reddit.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment","ner","created_time"})
            for data in reddit:
                NER_data.extend(ner_data(data["ner"],"reddit",data["sentiment"],data["created_time"]))
                if data["sentiment"]=="Positive":
                    reddit_pos+=1
                elif data["sentiment"]=="Negative":
                    reddit_neg+=1
                else:
                    reddit_neu+=1

            youtube  = YouTube.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment","ner",'created_time'})
            for data in youtube:
                NER_data.extend(ner_data(data["ner"],"youtube",data["sentiment"],data["created_time"]))
                if data["sentiment"]=="Positive":
                    youtube_pos+=1
                elif data["sentiment"]=="Negative":
                    youtube_neg+=1
                else:
                    youtube_neu+=1

            tumblr  = Tumblr.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment","ner",'created_time'})
            for data in tumblr:
                NER_data.extend(ner_data(data["ner"],"tumblr",data["sentiment"],data["created_time"]))
                if data["sentiment"]=="Positive":
                    tumblr_pos+=1
                elif data["sentiment"]=="Negative":
                    tumblr_neg+=1
                else:
                    tumblr_neu+=1

            twitter  = Twitter.find({'tag': tag,"created_time":{'$lt': end, '$gte': start}},{"sentiment","ner",'created_time'})
            for data in twitter:
                NER_data.extend(ner_data(data["ner"],"twitter",data["sentiment"],data["created_time"]))
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
            NER_data = refine_list(NER_data)
            return data,NER_data

def merge(c):
    _keys = {i for b in c for i in b}
    return {i:[sum, merge][isinstance(c[0][i], dict)]([h[i] for h in c]) for i in _keys}

def merge_ner(lst):
    final_list = []
    for l in lst:
        final_list.extend(l)
    final_list = refine_list(final_list)
    return final_list

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
        now = datetime.now()

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
        previous_day = now - timedelta(1)
        start = datetime(previous_day.year,previous_day.month,previous_day.day,previous_day.hour,0,)
        end = datetime(previous_day.year,previous_day.month,previous_day.day,previous_day.hour,59,59)
        data = data_(start,end,tag)
        previous_hour = data[0]["total"]
        db["aggregate"].insert_one(output)
        db["ner_aggregate"].insert_one(output_ner)
        while True:
            while ((datetime.now()-now).seconds>50):
                now = datetime.now()
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
                    this_hour=merge(minutes)["total"]
                    if (this_hour!=0 and previous_hour!=0):
                        if(previous_hour==0):
                            change = this_hour
                        else:
                            change = (previous_hour-this_hour)/previous_hour*100
                        if (abs(change)>=20):
                            previous_day = now - timedelta(25)
                            start = datetime(previous_day.year,previous_day.month,previous_day.day,previous_day.hour,0,)
                            end = datetime(previous_day.year,previous_day.month,previous_day.day,previous_day.hour,59,59)
                            data = data_(start,end,tag)
                            previous_day_hour = data[0]["total"]
                            data={
                                "tag":tag,
                                "profiles":profiles,
                                "change":change,
                                "this_hour":this_hour,
                                "previous_hour":previous_hour,
                                "previous_day_hour":previous_day_hour
                            }
                            requests.post(url, data=json.dumps(data), headers=headers)
                    previous_hour = this_hour
                    hours[todays_date.hour-1] = merge(minutes)
                    hours_ner[todays_date.hour-1] = merge_ner(minutes_ner)
                    minutes = ['x']*60
                    minutes_ner = ['x']*60
                if(hours[-1]!='x'):
                    days[todays_date.day-2] = merge(hours)
                    days_ner[todays_date.day-2] = merge_ner(hours_ner)
                    hours = ['x']*24
                    hours_ner = ['x']*24
                if(days[-1]!='x'):
                    months[todays_date.month-2] = merge(days)
                    months_ner[todays_date.month-2] = merge_ner(days_ner)
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
        time.sleep(60)
        output_x = db["aggregate"].find({"tag":tag})[0]
        if (output_x["updatedAt"]!=check_date):
            return ("other function is running")
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
            data,ner = months_((months_ner.index("x")+1),(12-months_ner.index("x")),tag)
            months = months[:months_ner.index("x")]+(data)
            months_ner = months_ner[:months_ner.index("x")]+ner
            x,y = calendar.monthrange(todays_date.year, todays_date.month)
            days,days_ner = days_(1,y,tag)
            hours,hours_ner = hours_(0,24,tag)
            minutes,minutes_ner = minutes_(0,60,tag)
        elif(todays_date.day>check_date.day):
            a,b = calendar.monthrange(todays_date.year, todays_date.month)
            data,ner = days_((days.index("x")),(b+1-days.index("x")),tag)
            days = days[:days_ner.index("x")]+(data)
            days_ner = days_ner[:days_ner.index("x")]+ner
            hours,hours_ner = hours_(0,24,tag)
            minutes,minutes_ner = minutes_(0,60,tag)
        elif(todays_date.hour>check_date.hour):
            data,ner = hours_((hours_ner.index("x")),(25-hours_ner.index("x")),tag)
            hours = hours[:hours_ner.index("x")]+(data)
            hours_ner = hours_ner[:hours_ner.index("x")]+ner
            minutes,minutes_ner = minutes_(0,60,tag)
        elif(todays_date.minute>check_date.minute):
            if "x" in minutes:
                data,ner = minutes_((minutes_ner.index("x")),(61-minutes_ner.index("x")),tag)
                minutes = minutes[:minutes_ner.index("x")]+(data)
                minutes_ner = minutes_ner[:minutes_ner.index("x")]+ner
            else:
                minutes,minutes_ner = minutes_(0,60,tag)
        now = datetime.now()
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
        previous_day = now - timedelta(1)
        start = datetime(previous_day.year,previous_day.month,previous_day.day,previous_day.hour,0,)
        end = datetime(previous_day.year,previous_day.month,previous_day.day,previous_day.hour,59,59)
        data = data_(start,end,tag)
        previous_hour = data[0]["total"]
        while True:
            while ((datetime.now()-now).seconds>50):
                now = datetime.now()
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
                    this_hour=merge(minutes)["total"]
                    if (this_hour!=0 and previous_hour!=0):
                        if(previous_hour==0):
                            change = this_hour
                        else:
                            change = (previous_hour-this_hour)/previous_hour*100
                        if (abs(change)>=20):
                            previous_day = now - timedelta(25)
                            start = datetime(previous_day.year,previous_day.month,previous_day.day,previous_day.hour,0,)
                            end = datetime(previous_day.year,previous_day.month,previous_day.day,previous_day.hour,59,59)
                            data = data_(start,end,tag)
                            previous_day_hour = data[0]["total"]
                            data={
                                "tag":tag,
                                "profiles":profiles,
                                "change":change,
                                "this_hour":this_hour,
                                "previous_hour":previous_hour,
                                "previous_day_hour":previous_day_hour
                            }
                            requests.post(url, data=json.dumps(data), headers=headers)
                    previous_hour = this_hour
                    hours[todays_date.hour-1] = merge(minutes)
                    hours_ner[todays_date.hour-1] = merge_ner(minutes_ner)
                    minutes = ['x']*60
                    minutes_ner = ['x']*60
                if(hours[-1]!='x'):
                    days[todays_date.day-2] = merge(hours)
                    days_ner[todays_date.day-2] = merge_ner(hours_ner)
                    hours = ['x']*24
                    hours_ner = ['x']*24
                if(days[-1]!='x'):
                    months[todays_date.month-2] = merge(days)
                    months_ner[todays_date.month-2] = merge_ner(days_ner)
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

def get_data_aggregate(tag):
    try:
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
    except:
        return("Aggregate isn't started yet")


def get_data_ner_aggregate(tag):
    try:
        data = db["ner_aggregate"].find({"tag":tag})
        if (data.count==0):
            return("Aggregate isn't started yet")
        else:
            data = data[0]
            Data = {
                "_id":str(data['_id']),
                "tag":data['tag'],
                "profiles":data["profiles"],
                "years":(data["years"]),
                "months":(data["months"][:data["months"].index("x")]),
                "days":(data["days"][:data["days"].index("x")]),
                "hours":(data["hours"][:data["hours"].index("x")]),
                "mins":(data["mins"][:data["mins"].index("x")]),
                "createdAt":data["createdAt"],
                "updatedAt":data["updatedAt"]
            }
        return Data
    except:
        return("Aggregate isn't started yet")
