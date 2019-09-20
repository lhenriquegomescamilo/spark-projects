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
    *         \\\\\\\\\\\\\\\\\\\\\     AUXILIARY METHODS    //////////////////////
    *
    */
  /**
    * This method parses all the information given in the original json files, so that it
    * can generate a new json file that will be used by the Ingester to push the recently
    * downloaded audiences into the corresponding DSPs.
    *
    * @param file_name: File where we store all the audiences.
    * @param queries: list of parsed queries with all the information. Only the first
    * query is used to extract the properties.
    */
  def generateMetaFile(
      file_name: String,
      queries: List[Map[String, Any]]
  ) {
    println("KEYWISER LOG:\n\tPushing the audience to the ingester")

    // First we obtain the variables that will be stored in the meta data file.
    val priority = queries(0)("priority")
    val as_view =
      if (queries(0)("as_view").toString.length > 0)
        queries(0)("as_view").toString.toInt
      else 0
    val queue = queries(0)("queue").toString    
    val jobid =
      if (queries(0)("jobid").toString.length > 0)
        queries(0)("jobid").toString.toInt
      else 0
    val pipeline =
      if (queries(0)("pipeline").toString.length > 0)
        queries(0)("pipeline").toString.toInt
      else 3

    val description = queries(0)("description")
    var file_name_final =
      if (queries.length > 1) file_name + "_grouped" else file_name

    // Now we calculate the path of the file according to the properties.
    val file_path = "/datascience/keywiser/processed/"
    

    // Then we generate the content for the json file.
    val json_content = """{"filePath":"%s%s", "priority":%s, "as_view":%s,
                           "pipeline":"%s", "queue":"%s", "jobId":%s, "description":"%s"}"""
      .format(
        file_path,
        priority,
        as_view,
        pipeline,
        queue,
        jobid,
        description
      )
      .replace("\n", "")
    println("KEYWISER LOG:\n\t%s".format(json_content))

    // Finally we store the json.
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path("/datascience/ingester/ready/%s.meta".format(file_name))
    )
    os.write(json_content.getBytes)
    os.close()
    // fs.close()
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
    * This method gets all the files to be processed from the folder /datascience/keywiser/to_process/
    * and also it removes from the list all the files that have been already processed (which are
    * located in /datascience/keywiser/done/).
    *
    * @param spark: Spark session that will be used to access HDFS.
    * @param pathToProcess: Default: "/datascience/keywiser/to_process/".
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
    val pathDone = "/datascience/keywiser/done/"
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
    * (or a ficticious segment if push = 0). Rows will also have other parameters.
    * Each parameter will be the same for every row, except for "country", "seg_id", "query" and "kws" (REVISE!).
    * - "country".
    * - "seg_id".
    * - "query". (stemmed if stemming == 1).
    * - "kws". (stemmed if stemming == 1).
    * - "ndays" and "since".  
    * - "stemming": 1 or 0.  .
    * - "push": 1 or 0. 
    * - "description".  
    *
    * @param spark: Spark session that will be used to access HDFS.
    * @param file: The file that is going to be read.
    *
    * @return a list of Map's of query and parameters.????
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
    // audience is going to be pushed (or not). Then it has other parameters (specified above). 
    // the values of the parameter Map are appended to the list "queries".
    var queries = List[Map[String, Any]]()

    for (query <- data) {
      val filter = query("query")
      val segmentId = query("seg_id")
      val keywords =
        if (query.contains("kws") && Option(query("kws"))
              .getOrElse("")
              .toString
              .length > 0) query("kws")
        else ""
      val country =
        if (query.contains("country") && Option(query("country"))
              .getOrElse("")
              .toString
              .length > 0) query("country")
        else ""
      val since =
        if (query.contains("since") && Option(query("since"))
              .getOrElse("")
              .toString
              .length > 0) query("since")
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
        else 0
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
        else "highload"
      val pipeline =
        if (query.contains("pipeline") && Option(query("pipeline"))
              .getOrElse("")
              .toString
              .length > 0) query("pipeline")
        else 0                                                       // REVISE !!!
      val description =
        if (query.contains("description") && Option(query("description"))
              .getOrElse("")
              .toString
              .length > 0) query("description")
        else ""
      /**  
      val jobid =
        if (query.contains("jobId") && Option(query("jobId"))
              .getOrElse("")
              .toString
              .length > 0) query("jobId")
        else ""
       **/

      val actual_map: Map[String, Any] = Map(
        "filter" -> filter,
        "segment_id" -> segmentId,
        "keywords" -> keywords,
        "since" -> since,
        "ndays" -> nDays,
        "push" -> push,
        "priority" -> priority,
        "as_view" -> as_view,
        "queue" -> queue,
        "pipeline" -> pipeline,
        "description" -> description,
        //"jobid" -> jobid,
        "country" -> country
      )

      queries = queries ::: List(actual_map)
    }
    queries
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR QUERYING DATA     //////////////////////
    *
    */
  /**
    * This method takes a list of queries and their corresponding segment ids, and generates a file where the first
    * column is the device_type, the second column is the device_id, and the last column is the list of segment ids
    * for that user separated by comma. Every column is separated by a space. The file is stored in the folder
    * /datascience/keywiser/processed/file_name. The file_name value is extracted from the file path given by parameter.
    * In other words, this method appends a file per query (for each segment), containing users that matched the query
    * then it groups segments by device_id, obtaining a list of segments for each device.
    *
    * @param spark: Spark session that will be used to write results to HDFS.
    * @param queries: List of Maps, where the key is the parameter and the values are the values.
    * @param df_joint: DataFrame that will be used to extract the audience from, applying the corresponding filters.
    * @param file_name: File where we will store all the audiences.
    *
    * As a result this method stores the audience in the file /datascience/keywiser/processed/file_name, where
    * the file_name is extracted from the file path.
  **/

  def getAudiences(
      spark: SparkSession,
      queries: List[Map[String, Any]],
      df_joint: DataFrame,
      file_name: String
  ) = {

    df_joint.cache()

    val fileName = "/datascience/keywiser/processed/" + file_name
    val fileNameFinal = fileName + "_grouped"

    val tuples = queries
      .map(r => (r("seg_id").toString, r("query").toString))


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
  }
  

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD     //////////////////////
    *
    */
  /**
    * Given a file path, this method takes all the information from it (query, days to be read, etc)
    * and gets the audience.
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param file: file path String.
    * @param path: Default: "/datascience/keywiser/to_process/".
    * @param verbose: if true, prints are logged.
    *
    * As a result this method stores the audience in the file /datascience/keywiser/processed/file_name, where
    * the file_name is extracted from the file path.
  **/
  def processFile(
      spark: SparkSession,
      file: String,
      path: String,
      verbose: Boolean) = {

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    //var actual_path = "/datascience/keywiser/to_process/%s".format(file)
    var actual_path = path + file
    var srcPath = new Path("/datascience")
    var destPath = new Path("/datascience")
    var queries: List[Map[String, Any]] = List()
    var errorMessage = ""

    println(
      "KEYWISER LOG: actual path is: %s".format(actual_path)
    )

    try {
      queries = getQueriesFromFile(spark, actual_path)
    } catch {
      case e: Throwable => {
        errorMessage = e.toString()
      }
    }
    if (queries.length == 0) {
      // If there is an error in the file, move file from the folder /datascience/keywiser/to_process/ to /datascience/keywiser/errors/
      println(
        "KEYWISER LOG: The keywiser process failed on " + file + "\nThe error was: " + errorMessage
      )
      srcPath = new Path(actual_path)
      destPath = new Path("/datascience/keywiser/errors/")
      hdfs.rename(srcPath, destPath)
    } else {
      // Move file from the folder /datascience/keywiser/to_process/ to /datascience/keywiser/in_progress/
      srcPath = new Path(actual_path)
      destPath = new Path("/datascience/keywiser/in_progress/")
      hdfs.rename(srcPath, destPath)
      actual_path = "/datascience/keywiser/in_progress/%s".format(file)

      // Here we obtain parameters that are supposed to be equal for every query in the file
      val country = queries(0)("country").toString
      val keywords = queries(0)("keywords").toString
      val since = queries(0)("since").toString.toInt
      val nDays = queries(0)("ndays").toString.toInt
      val pipeline = queries(0)("pipeline").toString.toInt
      val push = queries(0)("push").toString
      val stemming = queries(0)("stemming").toString.toInt
      val description = queries(0)("description").toString

      println(
        "KEYWISER LOG: Parameters obtained for file %s:\n\tcountry: %s\n\tsince: %d\n\tnDays: %d\n\tPipeline: %d\n\tNumber of queries: %d\n\tPush: %s\n\tStemming: %s\n\tDescription: %s"
        //"KEYWISER LOG: Parameters obtained for file %s:\n\tpartner_id: %s\n\tsince: %d\n\tnDays: %d\n\tCommon filter: %s\n\tPipeline: %d\n\tNumber of queries: %d\n\tPush: %s\n\tXD: %s"
          .format(
            file,
            country,
            since,
            nDays,
            pipeline,
            queries.length,
            push,
            stemming,
            description
          )
      )
    
      println("KEYWISER LOG: \n\t%s".format(queries(0)("filter").toString))
      
      /**
        * Here we read data_keywords, format the keywords list from the json file.
        * Then we call getJointKeys() to merge them and group a list of keywords for each device_id.
      **/      
      /** Read from "data_keywords" database */
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
      **/

      /** Format all keywords from queries to join */
      val trimmedList: List[String] = keywords.split(",").map(_.trim).toList
      val df_keys = trimmedList.toDF().withColumnRenamed("value", "content_keywords")

      /**  Match all keywords with data_keywords */
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
      **/      
     
      // Lastly we store the audience applying the filters
      var file_name = file.replace(".json", "")
      // Flag to indicate if execution failed
      var failed = false

      try {
        getAudiences(
          spark = spark,
          queries = queries,
          df_joint = df_joint,
          file_name = file_name
        )
      } catch {
        case e: Exception => {
          e.printStackTrace()
          failed = true
        }
      }
      

      // If everything worked out ok, then move file from the folder /datascience/keywiser/in_progress/ to /datascience/keywiser/done/
      srcPath = new Path(actual_path)
      val destFolder =
        if (failed) "/datascience/keywiser/errors/"
        else "/datascience/keywiser/done/"
      destPath = new Path(destFolder)
      hdfs.rename(srcPath, destPath)

      // If push parameter is true, we generate a file with the metadata.
      if (!failed && Set("1", "true", "True").contains(push)) {
        generateMetaFile(file_name, queries)
      }
    }
    //hdfs.close()
  }



  type OptionMap = Map[Symbol, String]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
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

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Spark keywiser")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    val path =  "/datascience/keywiser/to_process/"

    println("LOGGER: Path: %s".format(path))
  
    val files = getQueryFiles(spark, path)

    files.foreach(file => processFile(spark, file, path, verbose))  

  }

}