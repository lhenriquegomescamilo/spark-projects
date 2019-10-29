#!/bin/bash

/home/rely/spark/bin/spark-submit\
  --class "main.scala.dataGCBA"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 12\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/DataGcba/target/scala-2.11/data-gcba_2.11-1.0.jar 0 1
