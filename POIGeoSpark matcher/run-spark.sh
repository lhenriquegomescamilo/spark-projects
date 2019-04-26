spark-submit\
  --class "main.scala.POIGeoSparkMatcher"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue default\
  --jars geospark-1.1.3.jar,geospark-sql_2.3-1.1.3.jar,geospark-viz-1.1.3.jar\
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer\
  --conf spark.kryo.registrator=org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator\
  --conf spark.yarn.maxAppAttempts=1\
  --conf spark.sql.broadcastTimeout=3000000\
  "/home/rely/spark-projects/POIGeoSpark matcher/target/scala-2.11/poi-geo-sparkmatcher_2.11-1.0.jar"  --nDays 2 --poi_file hdfs://rely-hdfs/datascience/geo/GCBAPoisSeguridad.csv --country argentina --output /datascience/geo/MX/test_geospark
