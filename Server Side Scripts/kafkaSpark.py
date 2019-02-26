# Libraries to prepare the tweets
import json
import re
import string
import csv
import collections
from nltk.stem import PorterStemmer

# Spark libraries
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# ML libraries
from hdfs import InsecureClient
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
import pickle

# Library for HBase access
import happybase

# Servers
hbaseServer = 'hdp-fw2c.c.twitterml.internal'
hdfsServer = 'http://hdp-fw2c.c.twitterml.internal:50070'
zookeeperServer = 'hdp-h91v.c.twitterml.internal:2181,hdp-fw2c.c.twitterml.internal:2181'

# Creating an object of class PorterStemmer
porter = PorterStemmer()

# Regex to split contracted negative auxiliary verbs
auxiliaryVerbs = ['do','does','did','has','have','had','should','must','can','could']
splitNegativeWords = re.compile(r'('+r'|'.join(auxiliaryVerbs)+r')n?\'t')

# Clean the tweet text and add the cleaned text version to inside the field 'tokens' 
def prepare_tweet(tweet):
    # Removing Hashtags
    tweet_aux = re.sub('(^|\ )#[^ ]+', '', tweet['text'].lower())
    # Removing mentions
    tweet_aux = re.sub('(^|\ )@[^ ]+', '', tweet_aux)
    # Removing URLs
    tweet_aux =  re.sub('https?://[^ ]+', '', tweet_aux)
    tweet_aux =  re.sub('www.[^ ]+', '', tweet_aux)
    # Removing symbols and numbers
    tweet_aux = re.sub('[^A-Za-z \n]+', '', tweet_aux)
    # Splitting contracted negative auxiliary verbs
    tweet_aux = splitNegativeWords.sub("\\1 not", tweet_aux)
    # Stemming
    stem_tokens = []
    for token in tweet_aux.split():
        stem_tokens.append(porter.stem(token))
    tweet['tokens'] = ' '.join(stem_tokens)
    return tweet


# Analyse the tweet sentiment and returns 4 is the sentiment is positive and 0 if negative
def sentiment(tweet, logmodel, top_words):
    clean_tweet = prepare_tweet(tweet)
    counter = CountVectorizer(vocabulary=top_words, token_pattern=r"(?u)\b\w+\b")
    tweet_bag_of_words = counter.transform([tweet['tokens']])
    tweet_bow_df = pd.DataFrame(tweet_bag_of_words.astype(np.int8).toarray(), 
                                columns=top_words, dtype=np.int8)
    return logmodel.predict(tweet_bow_df)[0]


# Return the original tweet with an extra field 'sentiment' with 0 for negative and 4 for positive sentiment 
def tweet_sentiment(tweet, logmodel, top_words):
    sentimentId = sentiment(tweet, logmodel, top_words)
    if sentimentId == 4:
        tweet['sentiment'] = 'positive'
    else:
        tweet['sentiment'] = 'negative'
    return tweet

# Send the tweets to be stored in HBase
def sendToHbase(records):
    hbaseConnection = happybase.Connection(hbaseServer)
    hbaseTable = hbaseConnection.table(b'twitterSentiment')
    for record in records:
        print(str(record['id']).encode('UTF-8'))
        hbaseTable.put(str(record['id']).encode('UTF-8'), {
                                b'date:col1': record['date'].encode('UTF-8'),
                                b'text:col1': record['text'].encode('UTF-8'),
                                b'tokens:col1': record['tokens'].encode('UTF-8'),
                                b'sentiment:col1': record['sentiment'].encode('UTF-8')
                                })
    hbaseConnection.close()
    

def main():
    # Connecting to HDFS
    client = InsecureClient(hdfsServer, user='admin')
    
    # Downloading the list of most popular words
    with client.read('/tmp/word_count_100k.csv', encoding='UTF-8') as csvfile: 
        w = csv.DictReader(csvfile)
        word_count_aux = list(w)[0]
    
    # Selecting the 2000 most popular words
    word_count_dict = {key:int(value) for key,value in word_count_aux.items()}
    word_count = collections.Counter(word_count_dict)
    top_words = [word for (word, _) in word_count.most_common(2000)]
    
    # Downloading the trained Logistic Regression model
    with client.read('/tmp/twitterML.model') as modelfile:
        logmodel = pickle.load(modelfile)
    
    # Starting Spark context and streaming
    sc = SparkContext(appName="StreamingKafkaTweetProcessor")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/checkpoint")
    
    # Configuring Spark Streaming with a Kafka Consumer using a JSON deserializer 
    kafkaStream = KafkaUtils.createStream(ssc, zookeeperServer, 
                    'spark-group', {'twitter':1}, 
                    valueDecoder=lambda m: json.loads(m.decode('UTF-8')))
    
    # Extracting the data field
    tweets = kafkaStream.map(lambda v: v[1])
    
    # Analysing the sentiment of each Tweet
    sentiment_tweets = tweets.map(lambda tweet: tweet_sentiment(tweet, logmodel, top_words))
    # Printing 10 Tweets with the sentiment each second
    sentiment_tweets.pprint(10)
    
    # Sending blocks of Tweets to the function responsible to send the to HBase
    sentiment_tweets.foreachRDD(lambda rdd: rdd.foreachPartition(sendToHbase))
    
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
    