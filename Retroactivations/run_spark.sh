: '
spark-submit\
 --class "main.scala.GetDataPartnerID"\
 --master yarn\
 --deploy-mode cluster\
 --driver-memory 8g\
 --executor-memory 8g\
 --num-executors 10\
 --executor-cores 3\
 --queue spark\
 --conf spark.yarn.maxAppAttempts=1\
 /home/rely/spark-projects/Retroactivations/target/scala-2.11/retroactivations-by-partner-id_2.11-1.0.jar --from 1 --nDays 25
'


spark-submit\
 --class "main.scala.devicer.GetAudience"\
 --master yarn\
 --deploy-mode cluster\
 --driver-memory 8g\
 --executor-memory 8g\
 --num-executors 40\
 --executor-cores 4\
 --queue default\
 --conf spark.yarn.maxAppAttempts=1\
 /home/rely/spark-projects/Retroactivations/target/scala-2.11/retroactivations-by-partner-id_2.11-1.0.jar
