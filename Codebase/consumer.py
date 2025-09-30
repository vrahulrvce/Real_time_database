"""Listens to a topic on a Kafka cluster and processes praw Submission objects"""

import pickle
import faust

import pymongo
import praw
import json
import spacy
import cassandra 
import nltk
import logging
from spacy_wordnet.wordnet_annotator import WordnetAnnotator
from spacytextblob.spacytextblob import SpacyTextBlob
from cassandra import ConsistencyLevel
from cassandra.policies import RoundRobinPolicy
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
my_nlp = spacy.load('en_core_web_sm')
my_nlp.add_pipe('spacytextblob')
my_nlp.add_pipe("spacy_wordnet", after='tagger', config={'lang': my_nlp.lang})
my_database_url = pymongo.MongoClient("mongodb://172.17.0.3:27017/")
my_database = my_database_url["reddit_data"]
my_post_collection = my_database["machine_learning_posts"]
my_comment_collection = my_database["machine_learning_comments"]

def load_post_mongodb(post):
    
    _submission = {
       "id": post.id,
        "subreddit": post.subreddit_name_prefixed,
        "title": post.title,
        "author": post.author_fullname,
        "url": post.url,
        "created_utc": post.created_utc,
        "num_comments": post.num_comments,
        "media": post.media, 
        "is_video": post.is_video, 
        "is_original_content": post.is_original_content,
        "is_self": post.is_self,
        "name": post.name,
        "over_18": post.over_18,
        "permalink": post.permalink
    }
    my_post_collection.insert_one(_submission)
    logger.info(f"[C] post id '"+str(post.id)+ "' was added to the MongoDB database")

def load_comment_mongodb(post,comment):
  
    _comment = {
        "id": comment.id,
        "submissionid": post.id,
        "parent_id": "", 
        "body": str(comment.body.encode('utf-8')),
        "created_utc": comment.created_utc,
        "permalink": comment.permalink,
        "subreddit": post.subreddit_name_prefixed
    }
    my_comment_collection.insert_one(_comment)
    logger.info(f"  [C] comment id '"+str(comment.id)+ "' was added to the MongoDB database")

app = faust.App("reddit-consumer", broker="kafka://localhost:9092", store="memory://")
machinelearning_subreddit = app.topic("inclasstopic", value_serializer="raw")
@app.agent(machinelearning_subreddit)
async def process(subreddit: faust.Stream[praw.Reddit.submission]):
    async for bytes_payload in subreddit:
        submission = pickle.loads(bytes_payload, fix_imports=False, encoding="bytes")
        logger.info(f"[C] receiving: {submission.id} - {submission.title}")
        if my_post_collection.find({ "id": submission.id }).count()==0:
            load_post_mongodb(submission)
        else:
            logger.info(f"[C] submission id "+str(submission.id)+ " already in the MongoDB database")
        submission.comments.replace_more(limit=0)
        comments = submission.comments.list()
        for comment in comments:
            if my_comment_collection.find({ "id": comment.id }).count()==0:
                load_comment_mongodb(submission,comment)
            else:
                logger.info(f"  [C] comment id "+str(comment.id)+ " already in the MongoDB database")
if __name__ == "__main__":
    app.main()
