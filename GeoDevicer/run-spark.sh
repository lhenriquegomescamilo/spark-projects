spark-submit\
  --class "main.scala.GeoSparkMatcher"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue spark\
  --conf spark.yarn.maxAppAttempts=1\
  "/home/rely/spark-projects/GeoDevicer/target/scala-2.11/geodevicer_2.11-1.0.jar" --path_geo_json /datascience/geo/geo_json/ecobici_poi_60d_argentina_14-3-2019-13h.json