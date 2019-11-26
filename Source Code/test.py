from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml import PipelineModel
from pyspark import *
from kafka import KafkaProducer
from time import sleep
import json, sys
import requests
import time
from pyspark.sql import SparkSession
import json
import time

f = open('labels.json', 'r')
label_data = json.loads(f.read())

def connect_kafka_producer():
    _producer = None
    try:
        #_producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'),bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
         _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
    
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


prod=connect_kafka_producer();
def modelExc(stream):
    data = stream.map(lambda x: x.split("||"))
    news_data = data.toDF(["label","text"])
    news_data = news_data.withColumn("label", news_data["label"].cast("int"))
   
    modelDt = PipelineModel.load('model_rf1')
   
    observedDt = modelDt.transform(news_data)
    evaluatorDt = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="accuracy")
    
    records=observedDt.select("label").rdd.map(lambda r: r[0]).collect()

    for record in records:
	
        publish_message(prod,'counts',label_data[str(record)])

    
    
    print("Accuracy of streaming using Random Forest: ")
    print(evaluatorDt.evaluate(observedDt))


appName = "Streming data"
master = 'local'

sparkConf = SparkConf().setAppName(appName).setMaster(master).set('spark.executor.memory', '4G').set('spark.driver.memory', '45G').set('spark.driver.maxResultSize', '10G')
sc = SparkContext(conf=sparkConf)

ssc = StreamingContext(sc, 60)


spark = SparkSession.builder.appName("Streaming data").getOrCreate()
spark.sparkContext.setLogLevel("Error")

print("Waiting for the data")

stream = KafkaUtils.createDirectStream(ssc=ssc, topics=["guardian2"], kafkaParams={'bootstrap.servers' : 'localhost:9092'})

stream = stream.map(lambda x:x[1])

stream.foreachRDD(lambda k: modelExc(k))

ssc.start()
ssc.awaitTermination()



