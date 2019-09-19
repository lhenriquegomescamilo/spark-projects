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
  * The idea of this script is to generate audiences based on keywords obtained from url content. 
  */
object Keywiser {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * This method returns a DataFrame with the data from the "data_keywords" pipeline, for the interval
    * of days specified. Basically, it loads every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned. Full or stemmed keywords can be chosen to read.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    * @param stemming: if 1, stemmed keywords are read, if 0, full keywords are read.    
    *
    * @return a DataFrame with the information coming from the read data. Columns: "content_keywords" and "device_id"
   **/

  def getDataKeywords(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer,
      stemming: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val to_select =
      if (stemming == 1) List("stemmed_keys", "device_id")
      else List("content_keys", "device_id")

    val columnName = to_select(0).toString

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select(to_select.head, to_select.tail: _*)
      .withColumnRenamed(columnName, "content_keywords")
      .na
      .drop()

    df
  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR MERGING DATA     //////////////////////
    *
    */
   /**
    * This method joins "content_keywords" from the "data_keywords" pipeline,
    * with "content_keywords" from the queries in the json file, obtaining corresponding device_ids for each keyword.
    * Then it drops duplicates and after that it groups by "device_id", obtaining a list of keywords for each device.
    * Also adds "device_type" as "web". 
    *
    * @param df_keys: DataFrame obtained from json queries.
    * @param df_data_keywords: DataFrame obtained from getDataKeywords().
    * @param verbose: if true prints are logged.
    *
    * @return a DataFrame with "device_type", "device_id", "kws", being "kws" a list of keywords.
   **/

  def getJointKeys(
      df_keys: DataFrame,
      df_data_keywords: DataFrame,
      verbose: Boolean
  ): DataFrame = {

    val df_joint = df_data_keywords
      .join(broadcast(df_keys), Seq("content_keywords"))
      .select("content_keywords", "device_id")
      .dropDuplicates()

    /**
    if verbose {
      println(
        "count del join con duplicados: %s"
          .format(df_joint.select("device_id").distinct().count())
      )
    }
    */   
    val df_grouped = df_joint
      .groupBy("device_id")
      .agg(collect_list("content_keywords").as("kws"))
      .withColumn("device_type", lit("web"))
      .select("device_type", "device_id", "kws")
    df_grouped
  }


  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR PARSING JSON FILES     //////////////////////
    *
    */
  /**
    * This method gets all the files to be processed from the folder /datascience/devicer/to_process/
    * and also it removes from the list all the files that have been already processed (which are
    * located in /datascience/devicer/done/).
    *
    * @param spark: Spark session that will be used to access HDFS.
    * @param pathToProcess: Default: "/datascience/devicer/to_process/".
    *
    * @return a list of strings, where every element is the complete path to the files to be processed.
  **/
  def getQueryFiles(
      spark: SparkSession,
      pathToProcess: String) = {

    // First we get the list of files to be processed
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Now we order the files according to their date (filename, timestamp).
    val filesReady = fs
      .listStatus(new Path(pathToProcess))
      .map(
        f =>
          (f.getPath.toString.split("/").last.toString, f.getModificationTime)
      )
      .toList

    // Now we sort the list by the second component (timestamp) ????
    val filesReadyOrdered = scala.util.Sorting.stableSort(
      filesReady,
      (e1: (String, Long), e2: (String, Long)) => e1._2 < e2._2
    )

    // Now we get the list of files that have been processed already
    val pathDone = "/datascience/devicer/done/"
    val filesDone = fs
      .listStatus(new Path(pathDone))
      .map(x => x.getPath.toString.split("/").last)
      .toList
    // Finally we return the ones that have not been processed yet
    //filesReady diff filesDone
    filesReadyOrdered.map(x => x._1).filterNot(filesDone.contains(_))
  }

  /**
    * This method obtains all the data from a single file, iterating through each row.
    * Every row has to have a filter (or query) and the segment to which the audience is going to be pushed,
    * (or a ficticious segment if push = 0). Rows will also have other parameters. In sum:
    * "country" and "job_name" will be the same for all rows (REVISE!). 
    * - "seg_id"
    * - "kws" or "stem_kws"
    * - "query" or "stem_query"
    *
    * @param spark: Spark session that will be used to access HDFS.
    * @param file: The file that is going to be read.
    *
    * @return a list of Map's of query and parameters.
  **/

  def getQueriesFromFile(
      spark: SparkSession,
      file: String
  ): List[Map[String, Any]] = {
    // First of all we obtain all the data from the file
    val df = spark.sqlContext.read.json(file)
    val columns = df.columns
    val data = df
      .collect()
      .map(fields => fields.getValuesMap[Any](fields.schema.fieldNames))

    // Now we extract the different values from each row. Every row has to have a filter and the segment to which the
    // audience is going to be pushed. Then it might have the partnerId, the number of days to be skipped, and the
    // number of days to be loaded from the pipeline
    var queries = List[Map[String, Any]]()

    for (query <- data) {
      val filter = query("query")
      val segmentId = query("segmentId")
      val partnerId =
        if (query.contains("partnerId") && Option(query("partnerId"))
              .getOrElse("")
              .toString
              .length > 0) query("partnerId")
        else ""
      val since =
        if (query.contains("from") && Option(query("from"))
              .getOrElse("")
              .toString
              .length > 0) query("from")
        else 1
      val nDays =
        if (query.contains("ndays") && Option(query("ndays"))
              .getOrElse("")
              .toString
              .length > 0) query("ndays")
        else 30
      val push =
        if (query.contains("push") && Option(query("push"))
              .getOrElse("")
              .toString
              .length > 0) query("push")
        else false
      val priority =
        if (query.contains("priority") && Option(query("priority"))
              .getOrElse("")
              .toString
              .length > 0) query("priority")
        else 14
      val as_view =
        if (query.contains("as_view") && Option(query("as_view"))
              .getOrElse("")
              .toString
              .length > 0) query("as_view")
        else ""
      val queue =
        if (query.contains("queue") && Option(query("queue"))
              .getOrElse("")
              .toString
              .length > 0) query("queue")
        else "datascience"
      val pipeline =
        if (query.contains("pipeline") && Option(query("pipeline"))
              .getOrElse("")
              .toString
              .length > 0) query("pipeline")
        else 0
      val description =
        if (query.contains("description") && Option(query("description"))
              .getOrElse("")
              .toString
              .length > 0) query("description")
        else ""
      val jobid =
        if (query.contains("jobId") && Option(query("jobId"))
              .getOrElse("")
              .toString
              .length > 0) query("jobId")
        else ""
      val xd =
        if (query.contains("xd") && Option(query("xd"))
              .getOrElse("")
              .toString
              .length > 0) query("xd")
        else false
      val commonFilter =
        if (query.contains("common") && Option(query("common"))
              .getOrElse("")
              .toString
              .length > 0) query("common")
        else ""
      val xdFilter =
        if (query.contains("xdFilter") && Option(query("xdFilter"))
              .getOrElse("")
              .toString
              .length > 0) query("xdFilter")
        else "device_type IN ('coo', 'and', 'ios')"
      val limit =
        if (query.contains("limit") && Option(query("limit"))
              .getOrElse("")
              .toString
              .length > 0) query("limit")
        else "30000000"
      val country =
        if (query.contains("country") && Option(query("country"))
              .getOrElse("")
              .toString
              .length > 0) query("country")
        else ""
      val revenue =
        if (query.contains("revenue") && Option(query("revenue"))
              .getOrElse("")
              .toString
              .length > 0) query("revenue")
        else 0
      val unique =
        if (query.contains("unique") && Option(query("unique"))
              .getOrElse("")
              .toString
              .length > 0) query("unique")
        else 1

      val actual_map: Map[String, Any] = Map(
        "filter" -> filter,
        "segment_id" -> segmentId,
        "partner_id" -> partnerId,
        "since" -> since,
        "ndays" -> nDays,
        "push" -> push,
        "priority" -> priority,
        "as_view" -> as_view,
        "queue" -> queue,
        "pipeline" -> pipeline,
        "xdFilter" -> xdFilter,
        "description" -> description,
        "jobid" -> jobid,
        "xd" -> xd,
        "common" -> commonFilter,
        "limit" -> limit,
        "country" -> country,
        "revenue" -> revenue,
        "unique" -> unique
      )

      queries = queries ::: List(actual_map)
    }
    queries
  }

 



  /**
  This function appends a file per query (for each segment), containing users that matched the query
  then it groups segments by device_id, obtaining a list of segments for each device.
  Input = df with queries |"seg_id"|"query"| and joint df from get_joint_keys().
  Output = DataFrame with "device_type"|"device_id"|"seg_id"
  if populate True (1), it creates a file for ingester.
  */
  def save_query_results(
      spark: SparkSession,
      df_queries: DataFrame,
      df_joint: DataFrame,
      stemming: Int,
      push: Int,
      job_name: String
  ) = {

    df_joint.cache()

    val fileName = "/datascience/devicer/processed/" + job_name
    val fileNameFinal = fileName + "_grouped"

    val to_select =
      if (stemming == 1) List("seg_id", "stem_query")
      else List("seg_id", "query")

    val tuples = df_queries
      .select(to_select.head, to_select.tail: _*)
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

    if (push == 1) {
      val conf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(conf)
      val os =
        fs.create(new Path("/datascience/ingester/ready/%s.meta".format(job_name)))
      val content =
        """{"filePath":"%s", "pipeline": 3, "priority": 20, "partnerId": 0, "queue":"highload", "jobid": 0, "description":"%s"}"""
          .format(fileNameFinal, job_name)
      println(content)
      os.write(content.getBytes)
      os.close()
    }

  }

  /**

  MAIN METHOD
  This function saves a file per query (for each segment), containing users that matched the query
  and sends file to ingester if populate == 1.
  Input = country,nDays,since,keys_path,queries,path,populate,job_name
  Output = DataFrame with "device_type"|"device_id"|"seg_id"
  if populate True (1), it creates a file for ingester.

  */

  def get_users_pipeline_3(
      spark: SparkSession,
      json_path: String,
      verbose: Boolean
  ) = {

    /** Read json with queries, keywordss and seg_ids */
    val df_queries = spark.read
      .format("json")
      .load(json_path)

    /** Load parameters */
    val country = df_queries.select("country").first.getString(0)
    val nDays = df_queries.select("ndays").selectExpr("cast(cast(ndays as int ) as String)").first.getString(0).toInt
    val since = df_queries.select("since").selectExpr("cast(cast(since as int ) as String)").first.getString(0).toInt
    val stemming = df_queries.select("stemming").selectExpr("cast(cast(stemming as int ) as String)").first.getString(0).toInt
    val push = df_queries.select("push").selectExpr("cast(cast(push as int ) as String)").first.getString(0).toInt
    val job_name = df_queries.select("job_name").first.getString(0)

    /**
    Select "content_keywords" (every keyword that appears in the queries) to match with df_kws
    depending on stemming parameter selects stemmed keywords or not stemmed.
    */
    val to_select = if (stemming == 1) List("stem_kws") else List("kws")

    val columnName = to_select(0).toString

    val df_keys = df_queries
      .select(to_select.head, to_select.tail: _*)
      .withColumnRenamed(columnName, "content_keywords")
      .withColumn("content_keywords", split(col("content_keywords"), ","))
      .withColumn("content_keywords", explode(col("content_keywords")))
      .dropDuplicates("content_keywords")

    /** Read from "data_keywords" folder */
    val df_data_keywords = getDataKeywords(
      spark = spark,
      country = country,
      nDays = nDays,
      since = since,
      stemming = stemming
    )

    /**
    if verbose {
      println(
        "count de data_keywords para %sD: %s"
          .format(nDays, df_data_keywords.select("device_id").distinct().count())
      )
    }
    */

    /**  Match content_keywords with data_keywords */
    val df_joint = getJointKeys(
      df_keys = df_keys,
      df_data_keywords = df_data_keywords,
      verbose = verbose)

    /**
    if verbose {
      println(
        "count del join after groupby: %s"
          .format(df_joint.select("device_id").distinct().count())
      )
    }
    */
    

    save_query_results(
      spark = spark,
      df_queries = df_queries,
      df_joint = df_joint,
      push = push,
      stemming = stemming,
      job_name = job_name
    )

  }

  type OptionMap = Map[Symbol, String]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--json" :: value :: tail =>
        nextOption(map ++ Map('json -> value), tail)
      case "--verbose" :: tail =>
        nextOption(map ++ Map('verbose -> "true"), tail)
    }
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    // Reading the parameters
    val options = nextOption(Map(), args.toList)
    val verbose = if (options.contains('verbose)) true else false
    // If there is no json specified, it is going to fail
    val json = if (options.contains('json)) options('json) else "" 

    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    get_users_pipeline_3(
      spark = spark,
      json_path = json,
      verbose = verbose
    )

  }

}
