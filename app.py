from flask import Flask
from flask import request
from reddit_search import *
from channel_stats import *
from video_stats import *
from youtube_search import *
from tumblr import *

app = Flask(__name__)

@app.route('/')
def baseurl():
	return("working")

@app.route('/reddit/',methods=['GET'])
def redditapi():
	Sort = request.args.get('sort')
	Limit = request.args.get('limit')
	Search = request.args.get('q')
	if Limit is None:
		Limit=10
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

if __name__ == '__main__':
	app.run(debug=False,host='0.0.0.0')
