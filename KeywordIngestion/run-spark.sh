#!/bin/bash

#for i in {1..5}
#do
/home/rely/spark/bin/spark-submit\
	  --class "main.scala.keywordIngestion"\
	  --master yarn\
	  --deploy-mode cluster\
	  --driver-memory 8g\
	  --executor-memory 10g\
	  --num-executors 12\
	  --executor-cores 3\
	  --queue spark\
	  --conf spark.yarn.maxAppAttempts=1\
	  /home/rely/spark-projects/KeywordIngestion/target/scala-2.11/keyword-ingestion_2.11-1.0.jar 10 1 1
#done

