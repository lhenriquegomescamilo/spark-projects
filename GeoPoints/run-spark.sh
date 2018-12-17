spark-submit\
  --class GetPoints\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue spark\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/GeoPoints/target/scala-2.11/geo-points_2.11-1.0.jar danone_35936
