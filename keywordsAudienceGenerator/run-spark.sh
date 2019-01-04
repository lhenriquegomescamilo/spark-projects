spark-submit\
   --class "main.scala.keywordsAudienceGenerator"\
   --master yarn\
   --deploy-mode cluster\
   --driver-memory 8g\
   --executor-memory 8g\
   --num-executors 10\
   --executor-cores 3\
   --queue spark\
   --conf spark.yarn.maxAppAttempts=1\
   /home/rely/spark-projects/keywordsAudienceGenerator/target/scala-2.11/keyword-audience-generator_2.11-1.0.jar 30 entel_cl "country = 'CL' AND array_contains(all_segments, '1336')"