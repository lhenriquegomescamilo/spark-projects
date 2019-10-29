#!/bin/bash

source /home/rely/.bashrc

/home/rely/spark/bin/spark-submit\
  --master yarn\
  --class "main.scala.GetPii"\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 9g\
  --num-executors 15\
  --executor-cores 3\
  --queue spark\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/GetPII/target/scala-2.11/pii-table_2.11-1.0.jar
