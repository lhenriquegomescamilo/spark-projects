spark-submit\
  --class "main.scala.POIGeoSparkMatcher"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  "/home/rely/spark-projects/POI matcher/target/scala-2.11/poi-matcher_2.11-1.0.jar"  --nDays 10 --poi_file "hdfs://rely-hdfs/datascience/geo/MX/airports_mx.csv" --country mexico --output /datascience/geo/MX/specific_POIs2
   --country mexico --output /datascience/geo/MX/airports_mx_geospark_10