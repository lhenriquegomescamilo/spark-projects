spark-submit\
  --class "main.scala.monthly.GenerateMonthlyData"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 15\
  --executor-cores 4\
  --queue default\
  --conf spark.sql.shuffle.partitions=500\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/DataDemo/target/scala-2.11/generate-triplets_2.11-1.0.jar
