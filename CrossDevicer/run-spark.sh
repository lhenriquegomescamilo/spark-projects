spark-submit\
  --class "CrossDevicer"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 15\
  --executor-cores 4\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/CrossDevicer/target/scala-2.11/cross-devicer_2.11-1.0.jar --from 1 --nDays 48