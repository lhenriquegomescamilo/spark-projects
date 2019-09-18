#!/bin/bash

/home/rely/spark/bin/spark-submit\
	  --class "main.scala.keywordIngestion"\
	  --master yarn\
	  --deploy-mode cluster\
	  --driver-memory 8g\
	  --executor-memory 8g\
	  --num-executors 15\
	  --executor-cores 4\
	  --queue spark\
	  --conf spark.yarn.maxAppAttempts=1\
      --conf spark.sql.shuffle.partitions=300\
	  /home/rely/spark-projects/KeywordIngestion/target/scala-2.11/keyword-ingestion_2.11-1.0.jar 5 1 1 

