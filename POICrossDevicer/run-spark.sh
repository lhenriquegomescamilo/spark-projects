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
  "/home/rely/spark-projects/POICrossDevicer/target/scala-2.11/poi-crossdevicer_2.11-1.0.jar"  --nDays 2 --poi_file  "hdfs://rely-hdfs/datascience/geo/AR/publicidad_sarmiento.csv" --country argentina --output /datascience/geo/AR/publicidad_sarmientito_test 
