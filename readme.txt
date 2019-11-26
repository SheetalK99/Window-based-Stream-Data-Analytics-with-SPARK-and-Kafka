Big Data and Analytics 	Assignment 4  CS 6350 
Name: Sheetal Kadam      Netid: sak170006


stream_producer.py : modified to generate offline data for training


train.py : training the dateset on RandomForest Classifier and exporting pipeline

test.py : fetching news from kafka in intervals of and using saved model. Pushed prediction onto the Kafka server to be consumed by logstash

logstash-lafka.conf: Configuation file of logstash to pick predcited labels 



1. Open the kafka folder and start zookeeper and kafka server.
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

2.Run the stream_producer file to generate the lables.json and data.json file.
python3 stream_producer.py 4cb1f7e4-a51e-4c58-8df1-511feed23555 2019-05-01 2019-10-30

3.For training from spark folder:
./bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 train.py

4.Run the stream_producer.py  to generate the streaming data for testing.
python3 stream_producer.py 4cb1f7e4-a51e-4c58-8df1-511feed23555 2019-11-01 2019-11-16

5.For predicting streaming data from spark folder
./bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 test.py

6.Start elasticsearch, logstash and kibana:

sudo systemctl start logstash
sudo systemctl start elasticsearch
sudo systemctl start kibana

7.Configure logstash to get predicted labels from kafka

 cd /etc/logstash/conf.d
 touch logstash-kafka.conf
 
8. Open http://localhost:9200/, http://localhost:5601

9. From elasticsearch search for required pattern

10. Create report in Kibana from Visualiza/Create -> Tag Cloud. Select aggregation and metrics