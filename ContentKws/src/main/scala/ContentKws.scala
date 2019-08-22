package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.classification.{
  RandomForestClassificationModel,
  RandomForestClassifier
}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.{
  GBTClassificationModel,
  GBTClassifier
}

//import org.apache.spark.mllib.feature.Stemmer

import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import java.time.DateTimeException
import java.sql.Savepoint

/**
  * The idea of this script is to run processes using content keywords from urls.
  */
object ContentKws {
 
  /**
    *
    *
    *
    *                Get Users pipeline 3 
    *
    *
    *
   

  // This function reads from data_keywords
  //Input = country, nDays and since.
  //Output = DataFrame with "content_keys"| "device_id"

  def read_data_kws(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer) : DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read
      .option("basePath", path).parquet(hdfs_files: _*)
      .select("content_keys","device_id")
      .na.drop() 

    df
  }


  // This function joins data from "data_keywords" with all keywords from queries ("content_keys")
  //Input = df from read_data_kws() and df from desired keys.
  //Output = DataFrame with "device_type"|"device_id"|"kws", grouped by kws list for each user

  def get_joint_keys(
      df_keys: DataFrame,
      df_data_keywords: DataFrame) : DataFrame = {

    val df_joint = df_data_keywords.join(broadcast(df_keys), Seq("content_keys"))
    df_joint
      .select("content_keys","device_id")
      .dropDuplicates()
      .groupBy("device_id")
      .agg(collect_list("content_keys").as("kws"))
      .withColumn("device_type", lit("web"))            
      .select("device_type", "device_id", "kws")
  
  }
    
  // This function appends a file per query (for each segment), containing users that matched the query
  // then it groups segments by device_id, obtaining a list of segments for each device.   
  //Input = df with queries |"seg_id"|"query"| and joint df from get_joint_keys().
  //Output = DataFrame with "device_type"|"device_id"|"seg_id"
  // if populate True (1), it creates a file for ingester.
  
  def save_query_results(
      spark: SparkSession,
      df_queries: DataFrame,
      df_joint: DataFrame,
      populate: Int,
      job_name: String) = {
  
    df_joint.cache()

    val fileName = "/datascience/devicer/processed/" + job_name
    val fileNameFinal = fileName + "_grouped"

    val tuples = df_queries.select("seg_id", "query")
      .collect()
      .map(r => (r(0).toString, r(1).toString))
    for (t <- tuples) {
      df_joint
        .filter(t._2)
        .withColumn("seg_id", lit(t._1))
        .select("device_type", "device_id", "seg_id")
        .write
        .format("csv")
        .option("sep", "\t")
        .mode("append")
        .save(fileName)
    }
  
    val done = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(fileName)
      .distinct()
    done
      .groupBy("_c0", "_c1")
      .agg(collect_list("_c2") as "segments")
      .withColumn("segments", concat_ws(",", col("segments")))
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
  
    if(populate == 1) {
      val conf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(conf)
      val os = fs.create(new Path("/datascience/ingester/ready/%s".format(job_name)))
      val content =
        """{"filePath":"%s", "pipeline": 3, "priority": 20, "partnerId": 0, "queue":"datascience", "jobid": 0, "description":"%s"}"""
          .format(fileNameFinal,job_name)
      println(content)
      os.write(content.getBytes)
      os.close()
    }
    
  }
  
  //main method:

  // This function saves a file per query (for each segment), containing users that matched the query
  // and sends file to ingester if populate == 1.  
  //Input = country,nDays,since,keys_path,queries,path,populate,job_name
  //Output = DataFrame with "device_type"|"device_id"|"seg_id"
  // if populate True (1), it creates a file for ingester.


  def get_users_pipeline_3(
      spark: SparkSession,
      nDays: Integer,
      since: Integer,
      json_path: String,
      populate: Int) = {


    //reads json with queries, kws and seg_ids
    val df_queries = spark.read
      .format("json")
      .load(json_path)
    
    //selects "content_keys" (every keyword that appears in the queries) to match with df_kws
    val df_keys = df_queries.select("kws")
      .withColumn("kws", split(col("kws"), ","))
      .withColumn("kws", explode(col("kws")))
      .dropDuplicates("kws")
      .withColumnRenamed("kws", "content_keys")
    
    val country = df_queries.select("country").first.getString(0)

    // reads from "data_keywords"
    val df_data_keywords = read_data_kws(spark = spark,
                                         country = country,
                                         nDays = nDays,
                                         since = since)
        
    // matches content_keys with data_keywords
    val df_joint = get_joint_keys(df_keys = df_keys,
                                  df_data_keywords = df_data_keywords)

    val job_name = df_queries.select("job_name").first.getString(0)

    save_query_results(spark = spark,
                       df_queries = df_queries,
                       df_joint = df_joint,
                       populate = populate,
                       job_name = job_name)

  }
  */

   /**
    *
    *
    *
    *                Get Users pipeline 3 
    *
    *
    *
    */

  // This function reads from data_keywords
  //Input = country, nDays and since.
  //Output = DataFrame with "content_keys"| "device_id"

  def read_data_kws(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer) : DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read
      .option("basePath", path).parquet(hdfs_files: _*)
      .select("content_keys","device_id")
      .na.drop() 

    df
  }


  // This function joins data from "data_keywords" with all keywords from queries ("content_keys")
  //Input = df from read_data_kws() and df from desired keys.
  //Output = DataFrame with "device_type"|"device_id"|"kws", grouped by kws list for each user

  def get_joint_keys(
      df_keys: DataFrame,
      df_data_keywords: DataFrame) : DataFrame = {

    val df_joint = df_data_keywords.join(broadcast(df_keys), Seq("content_keys")).select("content_keys","device_id").dropDuplicates()
    println("count del join con duplicados: %s".format(df_joint.select("device_id").distinct().count())) 
    val df_grouped = df_joint
      .groupBy("device_id")
      .agg(collect_list("content_keys").as("kws"))
      .withColumn("device_type", lit("web"))            
      .select("device_type", "device_id", "kws")
    df_grouped
  }
    
  // This function appends a file per query (for each segment), containing users that matched the query
  // then it groups segments by device_id, obtaining a list of segments for each device.   
  //Input = df with queries |"seg_id"|"query"| and joint df from get_joint_keys().
  //Output = DataFrame with "device_type"|"device_id"|"seg_id"
  // if populate True (1), it creates a file for ingester.
  
  def save_query_results(
      spark: SparkSession,
      df_queries: DataFrame,
      df_joint: DataFrame,
      populate: Int,
      job_name: String) = {
  
    df_joint.cache()

    val fileName = "/datascience/devicer/processed/" + job_name
    val fileNameFinal = fileName + "_grouped"

    val tuples = df_queries.select("seg_id", "query")
      .collect()
      .map(r => (r(0).toString, r(1).toString))
    for (t <- tuples) {
      df_joint
        .filter(t._2)
        .withColumn("seg_id", lit(t._1))
        .select("device_type", "device_id", "seg_id")
        .write
        .format("csv")
        .option("sep", "\t")
        .mode("append")
        .save(fileName)
    }
  
    //var done, if (parametro) --> distinct()

    val done = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(fileName)
      .distinct()
    done
      .groupBy("_c0", "_c1")
      .agg(collect_list("_c2") as "segments")
      .withColumn("segments", concat_ws(",", col("segments")))
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
  
    if(populate == 1) {
      val conf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(conf)
      val os = fs.create(new Path("/datascience/ingester/ready/%s".format(job_name)))
      val content =
        """{"filePath":"%s", "pipeline": 3, "priority": 20, "partnerId": 0, "queue":"datascience", "jobid": 0, "description":"%s"}"""
          .format(fileNameFinal,job_name)
      println(content)
      os.write(content.getBytes)
      os.close()
    }
    
  }
  


  //main method:

  // This function saves a file per query (for each segment), containing users that matched the query
  // and sends file to ingester if populate == 1.  
  //Input = country,nDays,since,keys_path,queries,path,populate,job_name
  //Output = DataFrame with "device_type"|"device_id"|"seg_id"
  // if populate True (1), it creates a file for ingester.


  def get_users_pipeline_3(
      spark: SparkSession,
      nDays: Integer,
      since: Integer,
      json_path: String,
      populate: Int) = {


    //reads json with queries, kws and seg_ids
    val df_queries = spark.read
      .format("json")
      .load(json_path)
    
    //selects "content_keys" (every keyword that appears in the queries) to match with df_kws
    val df_keys = df_queries.select("kws")
      .withColumn("kws", split(col("kws"), ","))
      .withColumn("kws", explode(col("kws")))
      .dropDuplicates("kws")
      .withColumnRenamed("kws", "content_keys")
    
    val country = df_queries.select("country").first.getString(0)

    // reads from "data_keywords"
    val df_data_keywords = read_data_kws(spark = spark,
                                         country = country,
                                         nDays = nDays,
                                         since = since)

    println("count de data_keywords para %sD: %s".format(nDays,df_data_keywords.select("device_id").distinct().count())) 
        
    // matches content_keys with data_keywords
    val df_joint = get_joint_keys(df_keys = df_keys,
                                  df_data_keywords = df_data_keywords)

    println("count del join after groupby: %s".format(df_joint.select("device_id").distinct().count())) 

    val job_name = df_queries.select("job_name").first.getString(0)

    save_query_results(spark = spark,
                       df_queries = df_queries,
                       df_joint = df_joint,
                       populate = populate,
                       job_name = job_name)

  }

   /**
    *
    *
    *
    *
    *
    *
    *                    Pedido GBA
    *
    *
    *
    *
    *
   */

  def read_data_kws_gba(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer) : DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read
      .option("basePath", path).parquet(hdfs_files: _*)
      .select("content_keys","device_id")
      .withColumnRenamed("content_keys", "kws")
      .na.drop() 

    df
  }   

  def get_joint_keys_gba(
      df_keys: DataFrame,
      df_data_keywords: DataFrame) : DataFrame = {

  val df_joint = df_data_keywords.join(broadcast(df_keys), Seq("kws"))
  df_joint
    .select("kws","device_id")
    .groupBy("device_id")
    .agg(collect_list("kws").as("kws"))        
    .select("device_id", "kws")


  }
  
  def save_query_results_gba(
      spark: SparkSession,
      df_queries: DataFrame,
      df_joint: DataFrame,
      populate: Int,
      job_name: String) = {
  
    df_joint.cache()

    val fileName = "/datascience/devicer/processed/" + job_name
    
    val tuples = df_queries.select("seg_id", "query")
      .collect()
      .map(r => (r(0).toString, r(1).toString))
    for (t <- tuples) {
      df_joint
        .filter(t._2)
        .withColumn("seg_id", lit(t._1))
        .select("device_id", "seg_id")
        .write
        .format("csv")
        .option("sep", "\t")
        .mode("append")
        .save(fileName)
    }

  }

  def get_users_pipeline_3_gba(
      spark: SparkSession,
      nDays: Integer,
      since: Integer,
      json_path: String,
      populate: Int) = {

    //reads json with queries, kws and seg_ids
    val df_queries = spark.read
      .format("json")
      .load(json_path)
    
    //selects "content_keys" (every keyword that appears in the queries) to match with df_kws
    val df_keys = df_queries.select("kws")
      .withColumn("kws", split(col("kws"), ","))
      .withColumn("kws", explode(col("kws")))
      .dropDuplicates("kws")
    
    val country = df_queries.select("country").first.getString(0)

    // reads from "data_keywords"
    val df_data_keywords = read_data_kws_gba(spark = spark,
                                         country = country,
                                         nDays = nDays,
                                         since = since)
        
    // matches content_keys with data_keywords
    val df_joint = get_joint_keys_gba(df_keys = df_keys,
                                  df_data_keywords = df_data_keywords)

    val job_name = df_queries.select("job_name").first.getString(0)

    save_query_results_gba(spark = spark,
                       df_queries = df_queries,
                       df_joint = df_joint,
                       populate = populate,
                       job_name = job_name)

  }
        
  /**
    *
    *
    *
    *
    *
    *
    *                    TEST STEMMING
    *
    *
    *
    *
    *
  
  //country se deberá leer del json
  def test_no_stemming(spark: SparkSession,
                       nDays: Integer,
                       since: Integer) = {    

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format)) //lista con formato de format
    val path = "/datascience/data_keywords"

    //country se deberá leer del json
    val country = "AR"
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*) //lee todo de una
    val content_kws = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/content_keys_danone.csv")
    val joint = df.join(broadcast(content_kws), Seq("content_keys"))
    joint.write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/test_joint_keys_no_stemming")
  }


  def test_stemming(spark: SparkSession,
                    nDays: Integer,
                    since: Integer) = {          

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val toArray = udf[Array[String], String]( _.split(" "))

    //reading data_keywords
    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format)) //lista con formato de format
    val path = "/datascience/data_keywords"

    //country se deberá leer del json
    val country = "AR"
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read
      .option("basePath", path).parquet(hdfs_files: _*) //lee todo de una
      .withColumn("content_keys", toArray(df("content_keys")))

    //stemming df
    val df_stemmed = new Stemmer()
      .setInputCol("content_keys")
      .setOutputCol("stemmed")
      .setLanguage("Spanish")
      .transform(df)
      .withColumn("stemmed" ,concat_ws(" ", col("stemmed")))
      .withColumn("content_keys" ,concat_ws(" ", col("content_keys")))
      .select("device_id","stemmed","content_keys")

    //reading query
    val content_kws = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/content_keys_danone.csv")
      .withColumn("content_keys", toArray(content_kws("content_keys")))

    val content_kws_st = new Stemmer()
      .setInputCol("content_keys")
      .setOutputCol("stemmed")
      .setLanguage("Spanish")
      .transform(content_kws)
      .withColumn("stemmed" ,concat_ws(" ", col("stemmed")))
      .dropDuplicates("stemmed").select("seg_id","stemmed")

    val joint = df_stemmed.join(broadcast(content_kws_st), Seq("stemmed"))
    joint.write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/test_joint_keys_stemmed")
  }
    
  
  */
  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    
    get_users_pipeline_3_gba(spark = spark,
                         nDays = 30,
                         since = 1,
                         json_path = "/datascience/custom/gba_freq.json",
                         populate = 0) 
  
     
  }

}
