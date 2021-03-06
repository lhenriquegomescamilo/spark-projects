package main.scala

import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Utils {
  /**
   * Given a file, it returns a Map with all the fields that the json has, and 
   * if a field is not present, it gives a default value.
  */
  def getQueriesFromFile(
      spark: SparkSession,
      file: String
  ): List[Map[String, String]] = {
    // First of all we obtain all the data from the file
    val df = spark.sqlContext.read.json(file)
    val columns = df.columns
    val data = df
      .collect()
      .map(fields => fields.getValuesMap[Any](fields.schema.fieldNames))

    // Now we extract the different values from each row.
    var queries = List[Map[String, String]]()

    // Default values
    val defaultValues: Map[String, String] = Map(
      "query" -> "",
      "partnerId" -> "",
      "push" -> "true",
      "priority" -> "14",
      "queue" -> "report",
      "description" -> "",
      "jobId" -> "0",
      "datasource" -> "",
      "interval" -> "",
      "type" -> "insights",
      "split" -> "false",
      "segments" -> "",
      "segmentsFilter" -> "",
      "userEmail" -> "salvador@retargetly.com",
      "reportId" -> "0",
      "report_subtype" -> "",
      "userId" -> ""
    )

    for (query <- data) {
      val actual_map = defaultValues.keys
        .map(key => (key, query.getOrElse(key, defaultValues(key)).toString))
        .toMap

      queries = queries ::: List(actual_map)
    }
    queries
  }

  /**
   * This function returns the list of json files to be processed.
  */
  def getQueryFiles(spark: SparkSession, pathToProcess: String): List[String] = {
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

    // Now we sort the list by the second component (timestamp)
    val filesReadyOrdered = scala.util.Sorting.stableSort(
      filesReady,
      (e1: (String, Long), e2: (String, Long)) => e1._2 < e2._2
    )

    // Finally we return the json paths
    filesReadyOrdered.map(x => x._1).toList
  }

  /**
   * This function is used to move a file from a folder to another one.
  */
  def moveFile(actual_path: String, dest_path: String, fileName: String) {
    // HDFS configuration to be able to move files
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    // Move the files from one folder to another one
    val srcPath = new Path("/datascience/reporter/" + actual_path + fileName)
    val destPath = new Path("/datascience/reporter/" + dest_path)
    hdfs.rename(srcPath, destPath)
  }

  /**
   * This function is used to create a Meta file with all the necessary
   * parameters for later ingestion.
  */
  def generateMetaFile(
      file_name: String,
      jsonContent: Map[String, String]
  ) {
    // First of all we create a new map with all the information
    val int_fields = "priority reportId partnerId split jobId userId"
      .split(" ")
      .toList
    val fields =
      "split segmentsFilter userEmail reportId report_subtype jobId partnerId priority description queue interval userId"
        .split(" ")
        .toList
    val jsonMap: Map[String, String] = fields
      .map(field => (field, jsonContent(field).toString))
      .toMap + ("filepath" -> ("/datascience/reporter/processed/" + file_name))

    // Obtain the content out of the map
    val json_content = "{" + jsonMap
      .map(
        t =>
          """"%s": %s""".format(
            t._1,
            if (int_fields.contains(t._1)) t._2
            else "\"" + t._2 + "\""
          )
      )
      .mkString(", ") + "}" //scala.util.parsing.json.JSONObject(jsonMap)

    println("LOG: meta content")
    println(json_content)

    // Finally we store the json.
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path("/datascience/ingester/ready/%s.meta".format(file_name))
    )
    os.write(json_content.getBytes)
    os.close()
  }
}
