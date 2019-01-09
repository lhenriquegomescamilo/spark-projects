package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, length, split, col}
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ SaveMode, DataFrame }
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
  
  def getQueriesFromFile(spark: SparkSession, file: String): List[(Any, Any, Any, Any, Any)] = {
    // First of all we obtain all the data from the file
    val df = spark.sqlContext.read.json(file)
    val columns = df.columns
    val data = df.collect().map(fields => fields.getValuesMap[Any](fields.schema.fieldNames))

    // Now we extract the different values from each row. Every row has to have a filter and the segment to which the
    // audience is going to be pushed. Then it might have the partnerId, the number of days to be skipped, and the 
    // number of days to be loaded from the pipeline
    var queries: List[(Any, Any, Any, Any, Any)] = List()
    for (query <- data) {
        val filter = query("query")
        val segmentId = query("segmentId")
        val partnerId = if (query.contains("partnerId") && Option(query("partnerId")).getOrElse("").toString.length>0) query("partnerId") else ""
        val from = if (query.contains("from") && Option(query("from")).getOrElse("").toString.length>0) query("from") else 1
        val nDays = if (query.contains("ndays") && Option(query("ndays")).getOrElse("").toString.length>0) query("ndays") else 30
        
        queries = queries ::: List((filter, segmentId, partnerId, from, nDays))
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
  * @param queries: List of tuples, where every tuple has two elements: (query, segmentId).
  *
  * As a result this method stores the audience in the file /datascience/devicer/processed/file_name, where
  * the file_name is extracted from the file path.
  **/
  def getAudience(data: DataFrame, queries: List[(String, String)], fileName: String) = {
    data.cache()
    val results = queries.map(query => data.filter(query._1)
                                           .select("device_type", "device_id")
                                           .withColumn("segmentIds", lit(query._2)))
    results.foreach(dataframe => dataframe.write.format("csv")
                                                .option("sep", " ")
                                                .mode("append")
                                                .save("/datascience/devicer/processed/"+fileName))
    data.unpersist()
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
    var queries = List()

    try{
      val queries = getQueriesFromFile(spark, actual_path)
    } catch {
      case e: Exception => {
        println(e.toString())
      }
    }
    if (queries.length()==0) {
      // If there is an error in the file, move file from the folder /datascience/devicer/to_process/ to /datascience/devicer/errors/
      srcPath = new Path(actual_path)
      destPath = new Path("/datascience/devicer/error/%s".format(file))
      hdfs.rename(srcPath, destPath)
    } else {
      // Move file from the folder /datascience/devicer/to_process/ to /datascience/devicer/in_progress/
      srcPath = new Path(actual_path)
      destPath = new Path("/datascience/devicer/in_progress/%s".format(file))
      hdfs.rename(srcPath, destPath)
      actual_path = "/datascience/devicer/in_progress/%s".format(file)
      
      // Here we obtain three parameters that are supposed to be equal for every query in the file
      val partner_id = queries(0)._3.toString
      val since = queries(0)._4.toString.toInt
      val nDays = queries(0)._5.toString.toInt

      // If the partner id is set, then we will use the data_partner_p pipeline, otherwise it is going to be data_audiences_p
      val path = if (partner_id.length > 0) "/datascience/data_partner_p/id_partner=%s".format(partner_id) else "/datascience/data_audiences_p/"
      val basePath = if (partner_id.length > 0) "/datascience/data_partner_p/" else "/datascience/data_audiences_p/"
      // Now we finally get the data that will be used
      val data = getDataPipeline(spark, basePath, path, nDays, since)

      // Lastly we store the audience applying the filters
      val file_name = file.split(".")(0)
      getAudience(data, queries.map(tuple => (tuple._1.toString, tuple._2.toString)), file_name)

      // If everything worked out ok, then move file from the folder /datascience/devicer/in_progress/ to /datascience/devicer/done/
      srcPath = new Path(actual_path)
      destPath = new Path("/datascience/devicer/done/%s".format(file))
      hdfs.rename(srcPath, destPath)
    }
  }


  def main(args: Array[String]) {    
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Spark devicer").getOrCreate()

    val files = getQueryFiles(spark)
    files.foreach( file => processFile(spark, file) )    
  }
  
}