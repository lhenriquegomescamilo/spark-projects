 spark-submit  --class "main.scala.HomeJobs"  --master yarn  --deploy-mode cluster  --driver-memory 8g  --executor-memory 8g  --num-executors 10  --executor-cores 3  --queue spark  --conf spark.yarn.maxAppAttempts=1  "/home/rely/spark-projects/HomeJobs/target/scala-2.11/homejobs-variable_2.11-1.0.jar"  --nDays 2   --country mexico --HourFrom 20 --HourTo 8 --output /datascience/geo/MX/homes_test

