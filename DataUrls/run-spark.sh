spark-submit\
  --class "main.scala.datasets.DatasetSegmentsBranded"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 12\
  --executor-cores 3\
  --queue spark\
  --conf spark.sql.shuffle.partitions=500\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/DataUrls/target/scala-2.11/url-classifier_2.11-1.0.jar --ndays 10 --since 1 --country "AR" --train 1 --expansion 0 --ndaysDataset 30 
