# Sentiment Analysis on Twitter Streaming Data

This is a personal project, developed in order to practice the usage and integration of some technologies from the Hadoop environment.

The *twitterKafka.py* script is the one responsible to connect to Twitter and get in real time all tweets that include the words mentioned in the variable *twitter_search*. These tweets are them processed to extract the date, tweet ID and the full text of the tweet and send them to Kafka.

Then the *kafkaSpark.py* will read the data from Kafka, clean the tweets' text, run sentiment analysis on them and store the tweets and their sentiments in HBase.

Our sentiment analysis model is using only 2000 words in the bag-of-words due to memory constrains and has a 77.8% accuracy on the cross validation set.

## Solution Architecture
![](images/Architecture.png)

## Servers
![](images/Servers-Architecture.png)

### Server Configuration
All servers are VMs inside Google Cloud with the following configurations:

HDP Ambari: 1 vCPU, 3.75 GB

HDP Node 1: 2 vCPUs, 13 GB

HDP Node 2: 2 vCPUs, 13 GB

Python Server: 1 vCPU, 3.75 GB

## Configuration
### Twitter Connection
In order to get Tweets from Twitter it's necessary to create a Developer account to have your own Access Tokens. 
You can do it following the instructions in the following link:

https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens.html

After that you can add your Access Token information to the file twitterKeys.py

### Kafka
The library kafka-python version 1.4.2 was used because both versions 1.4.3 and 1.4.4 seems to have some issues when using group IDs.

Creating the new Kafka Topic
```
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper hdp-h91v.c.twitterml.internal:2181,hdp-fw2c.c.twitterml.internal:2181 --replication-factor 1 --partitions 1 --topic twitter
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper hdp-fw2c.c.twitterml.internal:2181
```
### HBase
Starting Thrift server so we can use the HappyBase library:
```
/usr/hdp/current/hbase-master/bin/hbase-daemon.sh start thrift -p 9090 --infoport 9095
```

Creating a new table called "twitterSentiment" inside HBase shell:
```
create 'twitterSentiment', 'date', 'text', 'tokens', 'sentiment'
list
scan 'twitterSentiment'
```
## Notes
The Dataset used for the Logistic Regression model training can be found at: http://help.sentiment140.com/for-students
