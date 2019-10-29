
for i in  $( seq 5 31); do
    spark-submit\
      --class "main.scala.pvAlertData"\
      --master yarn\
      --deploy-mode cluster\
      --driver-memory 8g\
      --executor-memory 8g\
      --num-executors 12\
      --executor-cores 3\
      --queue default\
      --conf spark.yarn.maxAppAttempts=1\
      /home/rely/spark-projects/Reports/target/scala-2.11/reports_2.11-1.0.jar --nDays 1 --since $i
done



