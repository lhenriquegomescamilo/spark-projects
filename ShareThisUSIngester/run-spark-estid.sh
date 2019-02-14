#/bin/bash

/home/rely/spark/bin/spark-submit \
  --class "main.scala.EstidMapper"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue default\
  --conf spark.sql.shuffle.partitions=400\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/ShareThisUSIngester/target/scala-2.11/sharethis-us-ingester_2.11-1.0.jar