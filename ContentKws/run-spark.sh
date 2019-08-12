#!/bin/bash

source /home/rely/.bashrc

/home/rely/spark/bin/spark-submit\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/DataCustomAudiences/target/scala-2.11/data-for-custom-audiences_2.11-1.0.jar --nDays 1 --from 1
