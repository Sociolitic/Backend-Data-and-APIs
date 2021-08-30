from flask import Flask
from flask import request
from reddit_search import *
from channel_stats import *
from video_stats import *
from youtube_search import *
from tumblr import *
# from insta import *
from twitter import *
from sentiment import *

app = Flask(__name__)

@app.route('/')
def baseurl():
	return("working")

@app.route('/reddit/search/',methods=['GET'])
def redditapi():
	Sort = request.args.get('sort')
	Limit = request.args.get('limit')
	Search = request.args.get('q')
	if Limit is None:
		Limit=100
	if (Sort=="Top"):
		return reddittop(Search,number=int(Limit))
	elif (Sort=='New'):
		return redditnew(Search,number=int(Limit))
	else:
		return reddithot(Search,number=int(Limit))

@app.route('/youtube/stats/',methods=['GET'])
def youtubestats():
	 channelId = request.args.get('channelId')
	 videoId = request.args.get('videoId')
	 if channelId is not None:
		 return Channel_stats(channelId)
	 elif videoId is not None:
		 return Video_stats(videoId)
	 else:
		 return ("Provide channel Id or video Id")

@app.route('/youtube/search/',methods=['GET'])
def youtube_search():
	 Search = request.args.get('q')
	 Limit = request.args.get('limit')
	 if (Limit is None) or (int(Limit)>50):
		 return Video_Search(Search)
	 else:
		 return Video_Search(Search,max_results=int(Limit))


@app.route('/tumblr/search/',methods=['GET'])
def tumblr():
	Search = request.args.get('q')
	return tumblrsearch(Search)

@app.route('/sentiment/',methods=['GET'])
def sentiment_():
	Search = request.args.get('q')
	return sentiment_analysis(Search)
#
# @app.route('/insta/search/top/',methods=['GET'])
# def Insta_top():
# 	Search = request.args.get('q')
# 	Limit = request.args.get('limit')
# 	if (Limit is None):
# 		return insta_top(Search)
# 	else:
# 		return insta_top(Search,amount=int(Limit))
#
# @app.route('/insta/search/recent/',methods=['GET'])
# def Insta_recent():
# 	Search = request.args.get('q')
# 	Limit = request.args.get('limit')
# 	if (Limit is None):
# 		return insta_recent(Search)
# 	else:
# 		return insta_recent(Search,amount=int(Limit))

@app.route('/twitter/search/',methods=['GET'])
def Twitter():
	Search = request.args.get('q')
	Limit = request.args.get('limit')
	if (Limit is None) or (int(Limit)>3000):
		return twitter_past(Search)
	else:
		return twitter_past(Search,count=int(Limit))

@app.route('/twitter/stream/',methods=['GET'])
def Twitter_():
	Search = request.args.get('q')
	Time_Limit = request.args.get('time')
	if Time_Limit == None:
		return twitter_stream(Search)
	return twitter_stream(q=Search,t=float(Time_Limit)*60)

@app.route('/search/',methods=['GET'])
def All_data():
	Search = request.args.get('q')
	print("Searching started")
	tumblrsearch(Search)
	Video_Search(Search)
	reddithot(Search)
	twitter_past(Search)
	twitter_stream(q=Search)
	return True

@app.route('/mentions/',methods=['GET'])
def mention():
	Search = request.args.get('q')
	return mentions(Search)

if __name__ == '__main__':
	app.run(debug=False,host='0.0.0.0')
