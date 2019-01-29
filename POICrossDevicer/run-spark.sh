spark-submit\
  --class "main.scala.POICrossDevicer"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  "/home/rely/spark-projects/POICrossDevicer/target/scala-2.11/poi-crossdevicer_2.11-1.0.jar"  --nDays 30 --poi_file  "hdfs://rely-hdfs/datascience/geo/AR/naranja.csv" --country argentina --output /datascience/geo/AR/naranja_30_29-01 
