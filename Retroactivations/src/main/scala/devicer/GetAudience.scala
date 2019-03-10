package main.scala.devicer
import main.scala.crossdevicer.AudienceCrossDevicer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, length, split, col, concat_ws, collect_list, udf}
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ SaveMode, DataFrame }
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf.Configuration




/*
 * This object receives an audience and cross-device it using a cross-deviced index.
 * The result is stored in a new folder.
 */
object GetAudience {
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
  def getDataPipeline(spark: SparkSession, basePath: String, path: String,
                      nDays: Int = 30, since: Int = 1): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days.map(day => path+"/day=%s".format(day))
                          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", basePath).parquet(hdfs_files:_*)

    df
  }

  def getDataKeywords(spark: SparkSession,nDays: Int = 30, since: Int = 1): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"
    
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days.map(day => path+"/day=%s".format(day))
                          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files:_*)
    df
  } 

  /**
  * This method returns a DataFrame with the data from the audiences data pipeline, for the interval
  * of days specified. Basically, this method loads the given path as a base path, then it
  * also loads the every DataFrame for the days specified, and merges them as a single
  * DataFrame that will be returned.
  *
  * @param spark: Spark Session that will be used to load the data from HDFS.
  * @param nDays: number of days that will be read.
  * @param since: number of days ago from where the data is going to be read.
  * 
  * @return a DataFrame with the information coming from the data read.
  **/
  def getDataAudiences(spark: SparkSession,
                       nDays: Int = 30, since: Int = 1): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences_p"
    
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days.map(day => path+"/day=%s".format(day))
                          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files:_*)

    df
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
  def getDataIdPartners(spark: SparkSession, partnerIds: List[String],
                         nDays: Int = 30, since: Int = 1): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner/"
    
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = partnerIds.flatMap(partner => days.map(day => path+"id_partner="+partner+"/day=%s".format(day)))
                          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files:_*)

    df
  }

  /**
  * This method gets all the files to be processed from the folder /datascience/devicer/to_process/
  * and also it removes from the list all the files that have been already processed (which are 
  * located in /datascience/devicer/done/).
  * 
  * @param spark: Spark session that will be used to access HDFS.
  *
  * @return a list of strings, where every element is the complete path to the files to be processed.
  **/
  def getQueryFiles(spark: SparkSession) = {
    // First we get the list of files to be processed
    val pathToProcess = "/datascience/devicer/to_process/"
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val filesReady  = fs.listStatus(new Path(pathToProcess))
                        .map(x => x.getPath.toString.split("/").last)
                        .toList
    
    // Now we get the list of files that have been processed already
    val pathDone = "/datascience/devicer/done/"
    val filesDone   = fs.listStatus(new Path(pathDone))
                        .map(x => x.getPath.toString.split("/").last)
                        .toList

    // Finally we return the ones that have not been processed yet
    filesReady diff filesDone
  }
  
  /***
  Posible pipeline values:
  - 0: automatico
  - 1: data_partner
  - 2: data_audiences_p
  - 3: data_keywords_p
  ***/
  def getQueriesFromFile(spark: SparkSession, file: String): List[Map[String, Any]] = {
    // First of all we obtain all the data from the file
    val df = spark.sqlContext.read.json(file)
    val columns = df.columns
    val data = df.collect().map(fields => fields.getValuesMap[Any](fields.schema.fieldNames))
    
    // Now we extract the different values from each row. Every row has to have a filter and the segment to which the
    // audience is going to be pushed. Then it might have the partnerId, the number of days to be skipped, and the 
    // number of days to be loaded from the pipeline
    var queries = List[Map[String, Any]]()

    for (query <- data) {
        val filter = query("query")
        val segmentId = query("segmentId")
        val partnerId = if (query.contains("partnerId") && Option(query("partnerId")).getOrElse("").toString.length>0) query("partnerId") else ""
        val since = if (query.contains("since") && Option(query("since")).getOrElse("").toString.length>0) query("since") else 1
        val nDays = if (query.contains("ndays") && Option(query("ndays")).getOrElse("").toString.length>0) query("ndays") else 30
        val push = if (query.contains("push") && Option(query("push")).getOrElse("").toString.length>0) query("push") else false
        val priority = if (query.contains("priority") && Option(query("priority")).getOrElse("").toString.length>0) query("priority") else 14
        val as_view = if (query.contains("as_view") && Option(query("as_view")).getOrElse("").toString.length>0) query("as_view") else ""
        val queue = if (query.contains("queue") && Option(query("queue")).getOrElse("").toString.length>0) query("queue") else "datascience"
        val pipeline = if (query.contains("pipeline") && Option(query("pipeline")).getOrElse("").toString.length>0) query("pipeline") else 0
        val description = if (query.contains("description") && Option(query("description")).getOrElse("").toString.length>0) query("description") else ""
        val jobid = if (query.contains("jobid") && Option(query("jobid")).getOrElse("").toString.length>0) query("jobid") else ""
        val xd = if (query.contains("xd") && Option(query("xd")).getOrElse("").toString.length>0) query("xd") else false
        val commonFilter = if (query.contains("common") && Option(query("common")).getOrElse("").toString.length>0) query("common") else ""
    
        val actual_map: Map[String,Any] = Map("filter" -> filter, "segment_id" -> segmentId, "partner_id" -> partnerId,
                                               "since" -> since, "ndays" -> nDays, "push" -> push, "priority" -> priority, 
                                               "as_view" -> as_view, "queue" -> queue, "pipeline" -> pipeline,
                                                "description" -> description, "jobid" -> jobid, "xd" -> xd, "common" -> commonFilter)
        
        queries = queries ::: List(actual_map)
      }
      queries
  }

  /** 
  * This method takes a list of queries and their corresponding segment ids, and generates a file where the first
  * column is the device_type, the second column is the device_id, and the last column is the list of segment ids
  * for that user separated by comma. Every column is separated by a space. The file is stored in the folder
  * /datascience/devicer/processed/file_name. The file_name value is extracted from the file path given by parameter.
  *
  * @param data: DataFrame that will be used to extract the audience from, applying the corresponding filters.
  * @param queries: List of Maps, where the key is the parameter and the values are the values.
  * @param fileName: File where we will store all the audiences.
  * @param dropDuplicates: whether or not we will remove duplicates from the audience.
  *
  * As a result this method stores the audience in the file /datascience/devicer/processed/file_name, where
  * the file_name is extracted from the file path.
  **/
  def getAudience(spark: SparkSession, 
                  data: DataFrame, 
                  queries: List[Map[String, Any]], 
                  fileName: String, 
                  dropDuplicates: Boolean = false) = { 
    val results = queries.map(query => data.filter(query("filter").toString)
                                        .select("device_type", "device_id")
                                        .withColumn("segmentIds", lit(query("segment_id").toString))
                                        .distinct())
    results.foreach(dataframe => dataframe.write.format("csv")
                                            .option("sep", "\t")
                                            .mode("append")
                                            .save("/datascience/devicer/processed/"+fileName))
    if (results.length > 1) {
      val done = spark.read.format("csv")
                        .option("sep", "\t")
                        .load("/datascience/devicer/processed/"+fileName)
                        .distinct()
      done.groupBy("_c0", "_c1")
          .agg(collect_list("_c2") as "segments")
          .withColumn("segments", concat_ws(",", col("segments")))
          .write.format("csv")
            .option("sep", "\t")
            .mode("append")
            .save("/datascience/devicer/processed/"+fileName+"_grouped")
    }
  
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
  def getMultipleAudience(spark: SparkSession, data: DataFrame, queries: List[Map[String, Any]], fileName: String, commonFilter: String = "") = {
    val filtered = if (commonFilter.length>0) data.filter(commonFilter) else data

    // First we register the table
    filtered.createOrReplaceTempView("data")

    // Now we set all the filters
    val columns = queries.map(query => col("c_"+query("segment_id").toString))
    val filters = queries.map(query => "IF(%s, %s, '') as c_%s".format(query("filter").toString, query("segment_id").toString, query("segment_id").toString)).mkString(", ")

    // We use all the filters to create a unique query
    val final_query = "SELECT device_id, device_type, %s FROM data".format(filters)

    // Here we define a useful function that concatenates two columns
    val concatUDF = udf( (c1:String, c2:String) => if (c1.length>0 && c2.length>0) "%s,%s".format(c1, c2) else if (c1.length>0) c1 else c2 )

    // Finally we store the results
    val results = spark.sql(final_query)
    println("\n\n\n\n")
    results.explain()
    results.withColumn("segments", columns.reduce(concatUDF(_, _)))
           .filter(length(col("segments"))>0)
           .select("device_type", "device_id", "segments")
           .write.format("csv")
           .option("sep", "\t")
           .mode("append")
           .save("/datascience/devicer/processed/"+fileName)
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

   def processFile(spark: SparkSession, file: String) {
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    var actual_path = "/datascience/devicer/to_process/%s".format(file)
    var srcPath = new Path("/datascience")
    var destPath = new Path("/datascience")
    var queries: List[Map[String, Any]] = List()
    var errorMessage = ""

    // Here we define a function that might be used when asking for an IN in a multivalue column
    spark.udf.register("array_intersect", (xs: Seq[String], ys: Seq[String]) => xs.intersect(ys).size>0)

    try{
      queries = getQueriesFromFile(spark, actual_path)
    } catch {
      case e: Throwable => {
        errorMessage = e.toString()
      }
    }
    if (queries.length==0) {
      // If there is an error in the file, move file from the folder /datascience/devicer/to_process/ to /datascience/devicer/errors/
      println("DEVICER LOG: The devicer process failed on "+file+"\nThe error was: "+errorMessage)
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
      val pipeline = queries(0)("pipeline")
      val commonFilter = queries(0)("common").toString
      println("DEVICER LOG: Parameters obtained for file %s:\n\tpartner_id: %s\n\tsince: %d\n\tnDays: %d".format(file, partner_ids, since, nDays))

      // If the partner id is set, then we will use the data_partner pipeline, otherwise it is going to be data_audiences_p
      // Now we finally get the data that will be used
      val ids = partner_ids.toString.split(",", -1).toList
      
      // Here we select the pipeline where we will gather the data
      val data = pipeline match {
                case 0 => if (partner_ids.toString.length>0) getDataIdPartners(spark, ids, nDays.toString.toInt, since.toString.toInt) else getDataAudiences(spark, nDays.toString.toInt,
                                                                                                                                                     since.toString.toInt)
                case 1 => getDataIdPartners(spark, ids, nDays.toString.toInt, since.toString.toInt)
                case 2 => getDataAudiences(spark, nDays.toString.toInt, since.toString.toInt)
                case 3 => getDataKeywords(spark, nDays.toString.toInt, since.toString.toInt)
                }

      // Lastly we store the audience applying the filters
      var file_name = file.replace(".json", "")
      if (queries.length > 10){
        getMultipleAudience(spark, data, queries, file_name, commonFilter)
      } else {
        getAudience(spark, data, queries, file_name)
      }
      

      // We cross device the audience if the parameter is set.
      val xd = queries(0)("xd")
      if (xd.toString.toBoolean){
        val object_xd = AudienceCrossDevicer.cross_device(spark,
                                  "/datascience/devicer/processed/"+file_name,
                                            "index_type IN ('coo')",
                                            "\t", "_c1")
      }

      // If everything worked out ok, then move file from the folder /datascience/devicer/in_progress/ to /datascience/devicer/done/
      srcPath = new Path(actual_path)
      destPath = new Path("/datascience/devicer/done/")
      hdfs.rename(srcPath, destPath)
      
      // If push parameter is true, we generate a file with the metadata.
      val push = queries(0)("push")
      if (push.toString.toBoolean){
          val priority = queries(0)("priority")
          val as_view = queries(0)("as_view")
          val queue = queries(0)("queue")
          val jobid = queries(0)("jobid")
          val description = queries(0)("description")
          file_name = if(queries.length > 1) file_name+"_grouped" else file_name
          
          
          val conf = new Configuration()
          conf.set("fs.defaultFS", "hdfs://rely-hdfs")
          val fs= FileSystem.get(conf)
          val os = fs.create(new Path("/datascience/ingester/ready/%s.meta".format(file_name)))
          val json_content = """{"filePath":"/datascience/devicer/processed/%s", "priority":%s, "partnerId":%s,
                                 "queue":"%s", "jobid":%s, "description":"%s"}""".format(file_name,priority,as_view,
                                                                                        queue,jobid,description)
          os.write(json_content.getBytes)
          fs.close()
      }
    }
  }


  def main(args: Array[String]) {    
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Spark devicer").getOrCreate()

    val files = getQueryFiles(spark)
    files.foreach( file => processFile(spark, file) )    
  }
  
}