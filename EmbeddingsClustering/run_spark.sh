spark-submit\
  --class "main.scala.EmbeddingsClustering"\
  --master yarn\
  --deploy-mode cluster\
  --driver-memory 8g\
  --executor-memory 10g\
  --num-executors 10\
  --executor-cores 3\
  --queue default\
  --conf spark.yarn.maxAppAttempts=1\
  --conf spark.sql.shuffle.partitions=400\
 /home/rely/spark-projects/EmbeddingsClustering/target/scala-2.11/cluster_2.11-1.0.jar --filePath "/datascience/data_clustering/cf_user_embeddings_AR_201909.csv" --numClusters 10000 --normalize 1 --runs 5
