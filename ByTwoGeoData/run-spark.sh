#!/bin/bash

source /home/rely/.bashrc

/home/rely/spark/bin/spark-submit\
  --class "main.scala.ByTwoGeoData"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 18\
  --executor-cores 4\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/ByTwoGeoData/target/scala-2.11/bytwo-data_2.11-1.0.jar 9 1
