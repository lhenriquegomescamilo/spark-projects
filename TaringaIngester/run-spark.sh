#!/bin/bash

/home/rely/spark/bin/spark-submit\
  --class "main.scala.TaringaIngester"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 12\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/TaringaIngester/target/scala-2.11/taringa-ingester_2.11-1.0.jar
