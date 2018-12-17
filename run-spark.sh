spark-submit\
  --class "AudienceCrossDevicer"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue spark\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/AudienceCrossDevicer/target/scala-2.11/audience-cross-devicer_2.11-1.0.jar
