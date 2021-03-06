spark-submit\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 12\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  --packages com.databricks:spark-csv_2.11:1.5.0\
  /home/rely/spark-projects/Profiler/target/scala-2.11/profiler_2.11-1.0.jar
