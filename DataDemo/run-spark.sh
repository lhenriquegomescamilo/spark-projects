spark-submit\
  --class "main.scala.TrainModel"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 12\
  --executor-cores 3\
  --queue spark\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/DataDemo/target/scala-2.11/generate-triplets_2.11-1.0.jar
