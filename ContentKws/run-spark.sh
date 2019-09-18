spark-submit\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  /home/rely/spark-projects/ContentKws/target/scala-2.11/content-keywords_2.11-1.0.jar --json "/datascience/custom/CL_new_taxo.json"




