#!/bin/bash

/home/rely/spark/bin/spark-submit\
	  --class "main.scala.keywordIngestion"\
	  --master yarn\
	  --deploy-mode cluster\
	  --driver-memory 12g\
	  --executor-memory 8g\
	  --num-executors 15\
	  --executor-cores 4\
	  --queue default\
	  --conf spark.yarn.maxAppAttempts=1\
      --conf spark.sql.shuffle.partitions=500\
	  /home/rely/spark-projects/KeywordIngestion/target/scala-2.11/keyword-ingestion_2.11-1.0.jar 10 1 1

