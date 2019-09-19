#!/bin/bash

source /home/rely/.bashrc

/home/rely/spark/bin/spark-submit\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 4\
  --queue spark\
  --jars /home/rely/spark-projects/DataUserAgent/uap-scala_2.11-0.3.0.jar\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/DataUserAgent/target/scala-2.11/user-agent-data-downloader-and-parser_2.11-1.0.jar --nDays 1 --from 1
