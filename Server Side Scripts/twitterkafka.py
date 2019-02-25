
import twitter
import json
import logging
import sys
import traceback
from kafka import KafkaProducer

# Importing Twitter Secrets and Keys
import twitterKeys


# Twitter search keywords
twitter_search = ['Trump']

# Kafka servers
kafka_servers = ['hdp-fw2c.c.twitterml.internal:6667']
kafka_topic = 'twitter'

def main():
    # Authenticating on Twitter
    api = twitter.api.Api(consumer_key=twitterKeys.consumer_key,
                      consumer_secret=twitterKeys.consumer_secret,
                      access_token_key=twitterKeys.access_token_key,
                      access_token_secret=twitterKeys.access_token_secret,
                      tweet_mode='extended')
    
    tweet = dict()
    
    # Configuring Kafka Producer with a JSON serializer
    producer = KafkaProducer(bootstrap_servers=kafka_servers, value_serializer=lambda m: json.dumps(m).encode('UTF-8'))

    # Iterating through the Tweets 
    for line in api.GetStreamFilter(track=twitter_search, languages=['en']):
        try:
            # Checking if this is an actual Tweet
            if line.get('limit') is None:
                tweet['date'] = line['created_at']
                tweet['id'] = line['id']
                
                # Finding where the full text of the Tweet is and copying it to tweet['text']
                if line.get('retweeted_status') is None:
                    if line['truncated'] == False:
                        tweet['text'] = line['text']
                    else:
                        tweet['text'] = line['extended_tweet']['full_text']
                else:
                    if line['retweeted_status']['truncated'] == False:
                        tweet['text'] = line['retweeted_status']['text']
                    else:
                        tweet['text'] = line['retweeted_status']['extended_tweet']['full_text']
        except:
            print("\n\n\n\n ERROR : %s \n\n\n\n" % (json.dumps(line, indent=4, separators=(',', ': '))))   
            print(traceback.format_exc()) 
            quit()
        
        # Sending the Tweet to Kafka servers using the topic 'kafka_topic'
        producer.send(kafka_topic,value=tweet)
        


if __name__ == '__main__':
    main()