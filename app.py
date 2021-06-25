from flask import Flask
from flask import request
from reddit_search import *
from channel_stats import *
from video_stats import *
from youtube_search import *
from tumblr import *
from insta import *
from twitter import *

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
	 return Video_Search(Search)

@app.route('/tumblr/search/',methods=['GET'])
def tumblr():
	Search = request.args.get('q')
	return tumblrsearch(Search)

@app.route('/insta/search/',methods=['GET'])
def Insta():
	Search = request.args.get('q')
	return insta(Search)

@app.route('/twitter/search/',methods=['GET'])
def Twitter():
	Search = request.args.get('q')
	return twitter_past(Search)

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
	insta(Search)
	reddithot(Search)
	twitter_past(Search)
	twitter_stream(q=Search)
	return True


if __name__ == '__main__':
	app.run(debug=False,host='0.0.0.0')
