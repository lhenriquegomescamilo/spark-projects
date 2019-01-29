spark-submit\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 15\
  --executor-cores 5\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/GetPII/target/scala-2.11/random_2.11-1.0.jar