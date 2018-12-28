spark-submit\
  --class "main.scala.AudienceCrossDevicer"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 15\
  --executor-cores 4\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/AudienceCrossDevicer/target/scala-2.11/audience-cross-devicer_2.11-1.0.jar "index_type IN ('a', 'i')" /datascience/geo/MX/specific_POIs