spark-submit\
  --class "main.scala.GeoSparkMatcher"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue spark\
  --jars geospark-1.1.3.jar,geospark-sql_2.3-1.1.3.jar,geospark-viz-1.1.3.jar\
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer\
  --conf spark.kryo.registrator=org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator\
  --conf spark.yarn.maxAppAttempts=1\
  --conf spark.sql.broadcastTimeout=3000000\
  "/home/rely/spark-projects/GeoDevicer/target/scala-2.11/geodevicer_2.11-1.0.jar" --path_geo_json sarmiento_pois_5d_argentina_2-5-2019-8h --geospark
