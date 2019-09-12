#/bin/bash

/home/rely/spark/bin/spark-submit \
  --class "main.scala.ShareThisInput"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 9g\
  --num-executors 15\
  --executor-cores 3\
  --queue spark\
  --conf spark.yarn.maxAppAttempts=1\
  --conf spark.locality.wait=0s \
  /home/rely/spark-projects/ShareThisUSIngester/target/scala-2.11/sharethis-us-ingester_2.11-1.0.jar --nDays 1 --from 1
