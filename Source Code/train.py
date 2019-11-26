import json
from pyspark import *
from pyspark.ml import Pipeline
from pyspark.conf import *
from pyspark.sql import *
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, NaiveBayes, RandomForestClassifier
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import IDF
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
import pprint

with open("data.json") as json_file:
    newsData = json.load(json_file)

indexNewsList = []
for news in newsData:
    indexNews = news.split("||")
    indexNewsList.append([int(indexNews[0]), indexNews[1]])

appName = "News label prediction"
master = 'local'

spark = SparkConf().setAppName(appName).setMaster(master).set('spark.executor.memory', '4G').set('spark.driver.memory', '45G').set('spark.driver.maxResultSize', '10G')
sc = SparkContext(conf=spark)
sqlContext = SQLContext(sc)

df = sqlContext.createDataFrame(indexNewsList, ["label","text"])

tokenizer = Tokenizer(inputCol="text", outputCol="words")

remover = StopWordsRemover(inputCol="words", outputCol="filtered")

hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)

idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)

nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

rf = RandomForestClassifier(labelCol="label",featuresCol="features", numTrees = 100, maxDepth = 15, maxBins = 32)

pipeline = Pipeline(stages = [tokenizer, remover, hashingTF, idf])
pipeline1 = Pipeline(stages = [tokenizer, remover, hashingTF, idf,rf])
pipeline_nb = pipeline1.fit(df)
pipelineFit = pipeline.fit(df)
pipeline_nb.write().overwrite().save("model_rf1")
dataset = pipelineFit.transform(df)

(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)

modelNb = nb.fit(trainingData)
modelRf = rf.fit(trainingData)

modelNb.write().overwrite().save("model_lr")
modelRf.write().overwrite().save("model_rf")

predictionNb = modelNb.transform(testData)
predictionRf = modelRf.transform(testData)

evaluatorNb = MulticlassClassificationEvaluator(predictionCol="prediction",  metricName="accuracy")
evaluatorRf = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="accuracy")


print("Accuracy of Naive Bayes: ")
print(evaluatorNb.evaluate(predictionNb))

print("Accuracy of Decision tree: ")
print(evaluatorRf.evaluate(predictionRf))


