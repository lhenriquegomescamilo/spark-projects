spark-submit\
  --class "main.scala.pvAlert"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 12\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/Reports/target/scala-2.11/reports_2.11-1.0.jar




