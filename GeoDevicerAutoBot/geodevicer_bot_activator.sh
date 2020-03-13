#!/bin/bash
# Este script lee los jsons que hay para procesar en to_process
# Debería activarse cada pocos minutos

for json in $(hdfs dfs -ls -C /datascience/geo/geodevicer_bot/to_process); do
/home/rely/spark/bin/spark-submit\
  --class 'main.scala.Geodevicer'\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g \
  --executor-memory 8g \
  --num-executors 8 \
  --executor-cores 2 \
  --queue spark \
  --jars geospark-1.1.3.jar,geospark-sql_2.3-1.1.3.jar,geospark-viz-1.1.3.jar\
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer\
  --conf spark.kryo.registrator=org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.sql.broadcastTimeout=3000000  \
  "/home/rely/spark-projects/GeoDevicerAutoBot/target/scala-2.11/geodevicer_2.11-1.0.jar" --path_geo_json $json;

fecha=$(date +"%Y-%m")
echo  "$json has been read on $fecha"
done

