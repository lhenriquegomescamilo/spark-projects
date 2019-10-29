spark-submit\
  --class "main.scala.Item2Item"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 10g\
  --num-executors 20\
  --executor-cores 2\
  --queue default\
  --conf spark.locality.wait=0s\
  --conf spark.yarn.maxAppAttempts=1\
  --conf spark.sql.shuffle.partitions=400\
 /home/rely/spark-projects/LookAlike/target/scala-2.11/lookalike_2.11-1.0.jar --filePath "/datascience/data_lookalike/input/input_PE_demo.json" --simHits "binary" --predHits "binary" --nDays 5
