spark-submit\
  --class "main.scala.HomeJobs"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 8g\
  --num-executors 10\
  --executor-cores 3\
  --queue spark\
  --conf spark.yarn.maxAppAttempts=1\
  "/home/rely/spark-projects/HomeJobs/target/scala-2.11/homejobs-variable_2.11-1.0.jar"  --HourFrom 19 --HourTo 7 --UseType home --country mexico --nDays 2 --output  /datascience/geo/MX/test

