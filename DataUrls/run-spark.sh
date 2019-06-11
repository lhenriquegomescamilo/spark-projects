spark-submit\
  --class "main.scala.GenerateFeaturesUrls"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 15\
  --executor-cores 4\
  --queue default\
  --conf spark.sql.shuffle.partitions=500\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/DataUrls/target/scala-2.11/generate-features-for-url-classifier_2.11-1.0.jar
