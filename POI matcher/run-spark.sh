spark-submit\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 16\
  --executor-cores 4\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/target/scala-2.12/spark-projects_2.12-0.1.0-SNAPSHOT.jar
