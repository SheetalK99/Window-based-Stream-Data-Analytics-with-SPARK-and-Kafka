# -*- coding: utf-8 -*-

from kafka import KafkaProducer
from time import sleep
import json, sys
import requests
import time
labels = {}
labels_inv = {}
def getData(url):
    jsonData = requests.get(url).json()
    data = []
    
    index = 0

    for i in range(len(jsonData["response"]['results'])):
        headline = jsonData["response"]['results'][i]['fields']['headline']
        bodyText = jsonData["response"]['results'][i]['fields']['bodyText']
        headline += bodyText
        label = jsonData["response"]['results'][i]['sectionName']
        if label not in labels:
            labels[label] = index
            labels_inv[index] = label
            index += 1  
        #data.append({'label':labels[label],'Descript':headline})
        toAdd=str(labels[label])+'||'+headline
        data.append(toAdd)
    return(data)

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

if __name__== "__main__":
    
    if len(sys.argv) != 4: 
        print ('Number of arguments is not correct')
        exit()
    
    key = sys.argv[1]
    fromDate = sys.argv[2]
    toDate = sys.argv[3]
    news = []
    url = 'http://content.guardianapis.com/search?from-date='+ fromDate +'&to-date='+ toDate +'&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key='+key        
    all_news=getData(url)
    if len(all_news)>0:
        prod=connect_kafka_producer();
        for story in all_news:
            print(json.dumps(story))
            news.append(story)
            publish_message(prod, 'guardian2', story)
            time.sleep(1)
        with open('data.json', 'w') as train_data:
            json.dump(news, train_data)
            print("done")
            '''
            for i in news:
                train_data.write(i)
            '''
        if prod is not None:
            prod.close()
    json.dump( labels_inv, open( "labels.json", 'w' ) )

