spark-submit\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 16\
  --executor-cores 4\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/ShareThisUSIngester/target/scala-2.11/sharethis-us-ingester_2.11-1.0.jar
