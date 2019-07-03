package main.scala.devicer
import main.scala.crossdevicer.AudienceCrossDevicer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  lit,
  length,
  split,
  col,
  concat_ws,
  collect_list,
  udf
}
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

/*
 * This object receives an audience and cross-device it using a cross-deviced index.
 * The result is stored in a new folder.
 */
object GetAudience {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
  /**
    * This method returns a DataFrame with the data from the given path, and for the interval
    * of days specified. Basically, this method loads the given path as a base path, then it
    * also loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param path: base path that will be used to load the data.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getDataPipeline(
      spark: SparkSession,
      basePath: String,
      path: String,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", basePath).parquet(hdfs_files: _*)

    df
  }

  /**
    * This method returns a DataFrame with the data from the Keywords data pipeline, for the interval
    * of days specified. Basically, it loads every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
    */
  def getDataKeywords(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    df
  }

  /**
    * This method returns a DataFrame with the data from the US data pipeline, for the interval
    * of days specified. Basically, it loads every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
    */
  def getDataUS(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_us_p"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    df
  }

  /**
    * This method returns a DataFrame with the data from the audiences data pipeline, for the interval
    * of days specified. Basically, it loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getDataAudiences(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .withColumn("category", lit(""))
      .withColumn("title", lit(""))

    println("DEVICER LOG: list of files to be loaded.")
    hdfs_files.foreach(println)

    df
  }

  /**
    * This method returns a Sequence of DataFrames, where every DataFrame represents the data
    * for only one day. For every day, this function checks whether or not the file exists.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
    */
  def getDataAudiencesDays(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ): Seq[DataFrame] = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val dfs = hdfs_files.map(spark.read.option("basePath", path).parquet(_))

    println("DEVICER LOG: list of files to be loaded.")
    hdfs_files.foreach(println)

    dfs
  }

  /**
    * This method returns a DataFrame with the data from the partner data pipeline, for the interval
    * of days specified. Basically, this method loads the given path as a base path, then it
    * also loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param partnerIds: List of id partners from which we are going to load the data.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getDataIdPartners(
      spark: SparkSession,
      partnerIds: List[String],
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = partnerIds
      .flatMap(
        partner =>
          days
            .map(day => path + "id_partner=" + partner + "/day=%s".format(day))
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
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
    *
    * @return a list of strings, where every element is the complete path to the files to be processed.
  **/
  def getQueryFiles(spark: SparkSession,pathToProcess:String) = {
    // First we get the list of files to be processed
    //val pathToProcess = "/datascience/devicer/to_process/"
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    //val filesReady = fs
    //  .listStatus(new Path(pathToProcess))
    //  .map(x => x.getPath.toString.split("/").last)
    //  .toList

    // Now we order the files according to their date (filename, timestamp).
    val filesReady = fs
      .listStatus(new Path(pathToProcess))
      .map(f => 
            ( f.getPath.toString.split("/").last.toString, 
              f.getModificationTime
            )
          )
      .toList

    // Now we sort the list by the second component (timestamp)
    scala.util.Sorting.stableSort(filesReady, 
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
    filesReady.map(x => x._1).filterNot(filesDone.contains(_))
  }

  /***
  Posible pipeline values:
  - 0: automatico
  - 1: data_partner
  - 2: data_audiences_p
  - 3: data_keywords_p
  - 4: data_us_p
  ***/
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
        else "index_type = 'coo'"
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
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR QUERYING DATA     //////////////////////
    *
    */
  /**
    * This method takes a list of queries and their corresponding segment ids, and generates a file where the first
    * column is the device_type, the second column is the device_id, and the last column is the list of segment ids
    * for that user separated by comma. Every column is separated by a space. The file is stored in the folder
    * /datascience/devicer/processed/file_name. The file_name value is extracted from the file path given by parameter.
    *
    * @param data: DataFrame that will be used to extract the audience from, applying the corresponding filters.
    * @param queries: List of Maps, where the key is the parameter and the values are the values.
    * @param fileName: File where we will store all the audiences.
    * @param commonFilter: filter that will be used prior to the querying process. Especially useful when all the queries come
    * from the same country.
    *
    * As a result this method stores the audience in the file /datascience/devicer/processed/file_name, where
    * the file_name is extracted from the file path.
  **/
  def getAudience(
      spark: SparkSession,
      data: DataFrame,
      queries: List[Map[String, Any]],
      fileName: String,
      commonFilter: String = "",
      limit: Int,
      unique: Int
  ) = {
    println(
      "DEVICER LOG:\n\tCommon filter: %s\n\tCommon filter length: %d"
        .format(commonFilter, commonFilter.length)
    )

    // First we filter the data using the common filter, if given.
    val filtered: DataFrame =
      if (commonFilter.length > 0 && queries.length > 5)
        data.filter(commonFilter)
      else data

    // Now we print the execution plan
    println("\n\n\n\n")
    filtered.explain()

    // If the number of queries is big enough, we persist the data so that it is faster to query.
    // [OUTDATED] THIS SECTION OF THE CODE IS NOT BEING USED, SINCE PERSISTING THE DATA TAKES
    // MUCH MORE TIME THAN JUST LOADING AND QUERYING. REVISE!
    if (queries.length > 5000) {
      println("DEVICER LOG:\n\tPersisting data!")
      filtered.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // For every query we apply the filter and get only the distinct ids along with the
    // device type and segment id.
    
    val results = queries.map(
      query => 
        query("revenue") match {
            case 0 =>
                  filtered
                  .filter(query("filter").toString)
                  .select("device_type", "device_id")
                  .withColumn("segmentIds", lit(query("segment_id").toString))
            case 1 =>
                filtered
                .filter(query("filter").toString)
                .select("device_type", "device_id","id_partner")
                .withColumn("segmentIds", lit(query("segment_id").toString))
        }
    )

    // Here we select distinct users if needed
    val results_distinct = if(unique > 0) results.map(
      df => df.distinct()) else results

    // If there is a limit on the number of rows, we also apply it
    val results_limited = if (limit>0) results_distinct.map(
      singleDf => singleDf.limit(limit)
    ) else results_distinct
    // Now we store every single audience separately
    results_limited.foreach(
      dataframe =>
        dataframe.write
          .format("csv")
          .option("sep", "\t")
          .mode("append")
          .save("/datascience/devicer/processed/" + fileName)
    )

    // If we previously persisted the data, now we unpersist it back.
    if (queries.length > 5000) {
      filtered.unpersist()
    }

    // If the number of queries is greater than one, then we merge all the audiences,
    // into one single DataFrame where every device id now contains a list of segments
    // separated by commas.
    if (results_limited.length > 1) {
      val fileNameFinal = "/datascience/devicer/processed/" + fileName + "_grouped"
      val done = spark.read
        .format("csv")
        .option("sep", "\t")
        .load("/datascience/devicer/processed/" + fileName)
        .distinct()
      done
        .groupBy("_c0", "_c1")
        .agg(collect_list("_c2") as "segments")
        .withColumn("segments", concat_ws(",", col("segments")))
        .write
        .format("csv")
        .option("sep", "\t")
        .mode("append")
        .save(fileNameFinal)
    }
  }

  /**
    * This method reads the dataset given, and coalesces it into files of 2MM so that it is easier for the dev
    * team to ingest these files.
    *
    * @param fileName: file name where the data is stored now with complete path.
    *
    * As a result it stores the dataset coalesced in files of 2MM.
    */
  def coalesceDataset(spark: SparkSession, fileName: String) = {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(fileName)

    val count = data.count()
    val n_files = count / 2000000

    if (count > 2000000) {
      data
        .coalesce(n_files.toInt)
        .write
        .format("csv")
        .option("sep", "\t")
        .save(fileName + "_coalesced")
    }

    count > 2000000
  }

  /**
    * This method takes a list of queries and their corresponding segment ids, and generates a file where the first
    * column is the device_type, the second column is the device_id, and the last column is the list of segment ids
    * for that user separated by comma. Every column is separated by a space. The file is stored in the folder
    * /datascience/devicer/processed/file_name. The file_name value is extracted from the file path given by parameter.
    *
    * @param data: DataFrame that will be used to extract the audience from, applying the corresponding filters.
    * @param queries: List of Maps, where the key is the parameter and the values are the values.
    *
    * As a result this method stores the audience in the file /datascience/devicer/processed/file_name, where
    * the file_name is extracted from the file path.
  **/
  def getMultipleAudience(
      spark: SparkSession,
      data: DataFrame,
      queries: List[Map[String, Any]],
      fileName: String,
      commonFilter: String = ""
  ) = {
    println(
      "DEVICER LOG:\n\tCommon filter: %s\n\tCommon filter length: %d"
        .format(commonFilter, commonFilter.length)
    )
    val filtered: DataFrame =
      if (commonFilter.length > 0) data.filter(commonFilter) else data
    println("\n\n\n\n")
    filtered.explain()

    // First we register the table
    filtered.createOrReplaceTempView("data")
    // filtered.persist(StorageLevel.MEMORY_AND_DISK)

    // Now we set all the filters
    val columns = queries.map(query => col("c_" + query("segment_id").toString))
    val filters = queries
      .map(
        query =>
          "IF(%s, %s, '') as c_%s".format(
            query("filter").toString,
            query("segment_id").toString,
            query("segment_id").toString
          )
      )
      .mkString(", ")

    // We use all the filters to create a unique query
    val final_query =
      "SELECT device_id, device_type, %s FROM data".format(filters)

    // Here we define a useful function that concatenates two columns
    val concatUDF = udf(
      (c1: String, c2: String) =>
        if (c1.length > 0 && c2.length > 0) "%s,%s".format(c1, c2)
        else if (c1.length > 0) c1
        else c2
    )

    // Finally we store the results
    val results = spark.sql(final_query)

    results
      .withColumn("segments", columns.reduce(concatUDF(_, _)))
      .filter(length(col("segments")) > 0)
      .select("device_type", "device_id", "segments")
      .write
      .format("csv")
      .option("sep", "\t")
      .mode("append")
      .save("/datascience/devicer/processed/" + fileName)
    // filtered.unpersist()
  }

  /**
    * [OUTDATED] THIS METHOD IS TAKING TOO LONG TO RUN, AND HAS TO BE REVISED.
    * POSSIBLE IMPROVEMENTS:
    *    - PROCESS MORE THAN ONE DAY AT THE TIME, INSTEAD OF ONE BY ONE IT WOULD BE K BY K DAYS.
    *    - STORE INTERMEDIATE RESULTS. THE RESULTS FROM EVERY BATCH OF K DAYS SHOULD BE STORED
    *    IN A TEMPORARY FOLDER AND THEN THEY SHOULD BE REMOVED.
    *
    * This method downloads the audiences day by day and then it reconstructs the whole
    * DataFrame in the end. The idea is that given the fact that every day's data is small
    * enough to be cached, we can cache every day and look for all the audiences in that day.
    */
  def getAudienceDay(
      spark: SparkSession,
      data: DataFrame,
      queries: List[Map[String, Any]],
      commonFilter: String = ""
  ) = {
    val filtered: DataFrame =
      if (commonFilter.length > 0 && queries.length > 5)
        data.filter(commonFilter)
      else data

    if (queries.length > 5) {
      println("DEVICER LOG:\n\tPersisting data!")
      filtered.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val results = queries.map(
      query =>
        filtered
          .filter(query("filter").toString)
          .select("device_type", "device_id")
          .withColumn("segmentIds", lit(query("segment_id").toString))
    )
    val df = results.reduce((df1, df2) => df1.unionAll(df2))
    if (queries.length > 5) {
      filtered.unpersist()
    }

    df
  }

  /**
    * Given a list of DataFrames, this method loads them one by one and applies all the given queries.
    * Once the new DataFrames are obtained, it reduces them into a single DataFrame with all the downloaded
    * audiences.
    *
    * @param data: List of DataFrames that will be used to extract the audience from, applying the corresponding filters.
    * @param queries: List of Maps, where the key is the parameter and the values are the values.
    * @param fileName: File where we will store all the audiences.
    * @param commonFilter: filter that will be used prior to the querying process. Especially useful when all the queries come
    * from the same country.
    */
  def getAudienceDays(
      spark: SparkSession,
      data: Seq[DataFrame],
      queries: List[Map[String, Any]],
      fileName: String,
      commonFilter: String = ""
  ) = {
    data
      .map(getAudienceDay(spark, _, queries, commonFilter))
      .reduce((df1, df2) => df1.unionAll(df2))
      .distinct()
      .groupBy("device_id", "device_type")
      .agg(collect_list("SegmentIds") as "segments")
      .withColumn("segments", concat_ws(",", col("segments")))
      .write
      .format("csv")
      .option("sep", "\t")
      .mode("append")
      .save("/datascience/devicer/processed/" + fileName + "_grouped")
  }

  /**
    * This method parses all the information given in the original json files, so that it
    * can generate a new json file that will be used by the Ingester to push the recently
    * downloaded audiences into the corresponding DSPs.
    *
    * @param file_name: File where we store all the audiences.
    * @param queries: list of parsed queries with all the information. Only the first
    * query is used to extract the properties.
    * @param xd: whether or not the audience has been cross-deviced.
    */
  def generateMetaFile(
      file_name: String,
      queries: List[Map[String, Any]],
      xd: String
  ) {
    println("DEVICER LOG:\n\tPushing the audience to the ingester")

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
    val description = queries(0)("description")
    var file_name_final =
      if (queries.length > 1) file_name + "_grouped" else file_name

    // Now we calculate the path of the file according to the properties.
    var file_path = ""
    if (Set("1", "true", "True").contains(xd)) {
      file_path = "/datascience/audiences/crossdeviced/"
      file_name_final = file_name_final + "_xd"
    } else {
      file_path = "/datascience/devicer/processed/"
    }

    // Then we generate the content for the json file.
    val json_content = """{"filePath":"%s%s", "priority":%s, "partnerId":%s,
                                 "queue":"%s", "jobId":%s, "description":"%s"}"""
      .format(
        file_path,
        file_name_final,
        priority,
        as_view,
        queue,
        jobid,
        description
      )
      .replace("\n", "")
    println("DEVICER LOG:\n\t%s".format(json_content))

    // Finally we store the json.
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path("/datascience/ingester/ready/%s.meta".format(file_name))
    )
    os.write(json_content.getBytes)
    fs.close()
  }

  /**
    * Given a file path, this method takes all the information from it (query, days to be read, and the partner
    * id) and gets the audience.
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param file: file path String.
    *
    * As a result this method stores the audience in the file /datascience/devicer/processed/file_name, where
    * the file_name is extracted from the file path.
  **/
  def processFile(spark: SparkSession, file: String, path: String) {
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    //var actual_path = "/datascience/devicer/to_process/%s".format(file)
    var actual_path = path+file
    var srcPath = new Path("/datascience")
    var destPath = new Path("/datascience")
    var queries: List[Map[String, Any]] = List()
    var errorMessage = ""

    println(
        "DEVICER LOG: actual path is: %s".format(actual_path)
    )

    // Here we define a function that might be used when asking for an IN in a multivalue column
    spark.udf.register(
      "array_intersect",
      (xs: Seq[String], ys: Seq[String]) => xs.intersect(ys).size > 0
    )

    try {
      queries = getQueriesFromFile(spark, actual_path)
    } catch {
      case e: Throwable => {
        errorMessage = e.toString()
      }
    }
    if (queries.length == 0) {
      // If there is an error in the file, move file from the folder /datascience/devicer/to_process/ to /datascience/devicer/errors/
      println(
        "DEVICER LOG: The devicer process failed on " + file + "\nThe error was: " + errorMessage
      )
      srcPath = new Path(actual_path)
      destPath = new Path("/datascience/devicer/errors/")
      hdfs.rename(srcPath, destPath)
    } else {
      // Move file from the folder /datascience/devicer/to_process/ to /datascience/devicer/in_progress/
      srcPath = new Path(actual_path)
      destPath = new Path("/datascience/devicer/in_progress/")
      hdfs.rename(srcPath, destPath)
      actual_path = "/datascience/devicer/in_progress/%s".format(file)

      // Here we obtain three parameters that are supposed to be equal for every query in the file
      val partner_ids = queries(0)("partner_id")
      val since = queries(0)("since")
      val nDays = queries(0)("ndays")
      val pipeline = queries(0)("pipeline").toString.toInt
      val commonFilter = queries(0)("common").toString
      val push = queries(0)("push").toString
      val xd = queries(0)("xd").toString
      val limit = queries(0)("limit").toString.toInt
      val country = queries(0)("country").toString
      val unique = queries(0)("unique").toString.toInt

      println(
        "DEVICER LOG: Parameters obtained for file %s:\n\tpartner_id: %s\n\tsince: %d\n\tnDays: %d\n\tCommon filter: %s\n\tPipeline: %d\n\tNumber of queries: %d\n\tPush: %s\n\tXD: %s"
          .format(
            file,
            partner_ids,
            since,
            nDays,
            commonFilter,
            pipeline,
            queries.length,
            push,
            xd
          )
      )
      println("DEVICER LOG: \n\t%s".format(queries(0)("filter").toString))

      // If the partner id is set, then we will use the data_partner pipeline, otherwise it is going to be data_audiences_p
      // Now we finally get the data that will be used
      val ids = partner_ids.toString.split(",", -1).toList

      // Here we select the pipeline where we will gather the data
      val data = pipeline match {
        case 0 =>
          if (
              (partner_ids.toString.length > 0 && country == "")  || 
              (partner_ids.toString.length > 0 && !(ids.contains("1")))
              )
            getDataIdPartners(
              spark,
              ids,
              nDays.toString.toInt,
              since.toString.toInt
            )
          else
            getDataAudiences(spark, nDays.toString.toInt, since.toString.toInt)
        case 1 =>
          getDataIdPartners(
            spark,
            ids,
            nDays.toString.toInt,
            since.toString.toInt
          )
        case 2 =>
          getDataAudiences(spark, nDays.toString.toInt, since.toString.toInt)
        case 3 =>
          getDataKeywords(spark, nDays.toString.toInt, since.toString.toInt)
        case 4 =>
          getDataUS(spark, nDays.toString.toInt, since.toString.toInt)
      }
    
      // Lastly we store the audience applying the filters
      var file_name = file.replace(".json", "")
      // Flag to indicate if execution failed
      var failed = false
      
      if (queries.length > 10000) {
        // getMultipleAudience(spark, data, queries, file_name, commonFilter)
        val dataDays = getDataAudiencesDays(
          spark,
          nDays.toString.toInt,
          since.toString.toInt
        )
        getAudienceDays(
          spark,
          dataDays,
          queries,
          file_name,
          commonFilter
        )
      } else {   
          // try {
            getAudience(spark, data, queries, file_name, commonFilter, limit, unique)
          // }
          // catch {
          //   case e: Exception => {failed = true}
          // }
        }
      
      // We cross device the audience if the parameter is set.
      if (!failed && Set("1", "true", "True").contains(xd)) {
        println(
          "LOGGER: the audience will be cross-deviced. XD parameter value: %s"
            .format(xd)
        )
        val object_xd = AudienceCrossDevicer.cross_device(
          spark,
          "/datascience/devicer/processed/" + file_name,
          queries(0)("xdFilter").toString,
          "\t",
          "_c1"
        )
      }
      


      // If everything worked out ok, then move file from the folder /datascience/devicer/in_progress/ to /datascience/devicer/done/
      srcPath = new Path(actual_path)
      val destFolder = if (failed) "/datascience/devicer/errors/" else "/datascience/devicer/done/" 
      destPath = new Path(destFolder) 
      hdfs.rename(srcPath, destPath)

      // If push parameter is true, we generate a file with the metadata.
      if (!failed && Set("1", "true", "True").contains(push)) {
        generateMetaFile(file_name, queries, xd)
      }
    }
  }

    type OptionMap = Map[Symbol, Int]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--priority" :: tail =>
        nextOption(map ++ Map('exclusion -> 0), tail)
    }
  }

  def main(args: Array[String]) {
    
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)
    
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Spark devicer").getOrCreate()

    val p = if (args.length > 0) args(0).toInt else 0

    Logger.getRootLogger.setLevel(Level.WARN)

    val path = p match {
        case 0 => "/datascience/devicer/to_process/"
          
        case 1 => "/datascience/devicer/priority/"

      }

     println("LOGGER: Path: %s".format(path))

    val files = getQueryFiles(spark,path)

    files.foreach(file => processFile(spark, file, path))
  }

}
