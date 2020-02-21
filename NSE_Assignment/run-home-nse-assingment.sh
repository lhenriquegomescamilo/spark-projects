/home/rely/spark/bin/spark-submit\
  --class "main.scala.NSEFromHomes"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g \
  --executor-memory 8g \
  --num-executors 10 \
  --executor-cores 3 \
  --queue default \
  --jars geospark-1.2.0.jar,geospark-sql_2.3-1.2.0.jar\
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer\
  --conf spark.kryo.registrator=org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.sql.broadcastTimeout=3000000  \
  "/home/rely/spark-projects/NSE_Assignment/target/scala-2.11/geodevicer_2.11-1.0.jar" --path_geo_json /datascience/geo/NSEHomes/monthly/to_process/$1.json