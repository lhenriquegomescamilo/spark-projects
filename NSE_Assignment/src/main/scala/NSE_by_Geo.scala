package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs, upper}
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.conf.Configuration

//Acá traemos los paquetes propios
import main.scala.homejobs.HomeJobs
import main.scala.crossdevicer.CrossDevicer
import main.scala.nseassignation.NSEAssignation



import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

/**
  Job Summary:
  * The goal of this job is to create an audiencie based on Points Of Interests (POIs). The method takes as input a time frame (be default, december 2018) and a dataset containing the POIs. This dataset should be already formatted in three columns segment|latitude|longitude (without the index) and with the latitude and longitude with point (".") as delimiter.
  * The method filters the safegraph data by country, and creates a geocode for both the POIs and the safegraph data. This geocode is used to match both datasets by performing a SQL join. The resulting rows will contain a user id, device type type, user latitude and longitude and POI id, latitude and longitude. Then the vincenty formula is used to calculate distance between the pairs of latitude and longitude.
   The method then proceeds to filter the users by a desired minimum distance returning a final dataset with user id and device type.
   The current method will provide the basis for future more customizable geolocation jobs.
  */
object NSE_by_Geo {

  /**
    * This method returns a Map with all the parameters obtained from the JSON file.
    *
    * @param path_geo_json: JSON file name. This is the json where all the parameters are going to be extracted from.
    */
  def get_variables(
      spark: SparkSession,
      path_geo_json: String
  ): Map[String, String] = {
    // First we read the json file and store everything in a Map.
    val file =
      "hdfs://rely-hdfs/datascience/geo/geo_json/%s.json".format(path_geo_json)
    println("LOGGER JSON FILE: " + file)
    val df = spark.sqlContext.read.json(file)
    val columns = df.columns
    val query = df
      .collect()
      .map(fields => fields.getValuesMap[Any](fields.schema.fieldNames))
      .toList(0)

    // Now we parse the Map assigning default values.
    val country =
      if (query.contains("country") && Option(query("country"))
            .getOrElse("")
            .toString
            .length > 0) query("country").toString
      else "argentina"

    //esto tiene que ser automático según el country, que lo tome del json
    val path_to_polygons =
      if (query.contains("path_to_polygons") && Option(query("path_to_polygons"))
            .getOrElse("")
            .toString
            .length > 0) query("path_to_polygons").toString
      else ""

    //esto tiene que ser automático, que lo tome desde el json
    val output_file =
      if (query.contains("output_file") && Option(query("output_file"))
            .getOrElse("")
            .toString
            .length > 0) query("output_file").toString
      else "custom"
    

     val crossdevice =
      if (query.contains("crossdevice") && Option(query("crossdevice"))
            .getOrElse("")
            .toString
            .length > 0) query("crossdevice").toString
      else "false"
    val nDays =
      if (query.contains("nDays") && Option(query("nDays"))
            .getOrElse("")
            .toString
            .length > 0) query("nDays").toString
      else "30"
    val since =
      if (query.contains("since") && Option(query("since"))
            .getOrElse("")
            .toString
            .length > 0) query("since").toString
      else "0"

    val HourFrom = 
      if (query.contains("HourFrom") && Option(query("HourFrom"))
            .getOrElse("")
            .toString
            .length > 0) query("since").toString
      else "19"

    val HourTo = 
      if (query.contains("HourTo") && Option(query("HourTo"))
            .getOrElse("")
            .toString
            .length > 0) query("HourTo").toString
      else "7"

    val UseType = 
      if (query.contains("UseType") && Option(query("UseType"))
            .getOrElse("")
            .toString
            .length > 0) query("UseType").toString
      else "home"
    
    val minFreq = 
      if (query.contains("minFreq") && Option(query("minFreq"))
            .getOrElse("")
            .toString
            .length > 0) query("minFreq").toString
      else "0"

      
   val priority =
    if (query.contains("priority") && Option(query("priority"))
            .getOrElse("")
            .toString
            .length > 0) query("priority").toString
      else "0"

   val queue =
    if (query.contains("queue") && Option(query("queue"))
            .getOrElse("")
            .toString
            .length > 0) query("queue").toString
      else "0"

   val jobid =
    if (query.contains("jobid") && Option(query("jobid"))
            .getOrElse("")
            .toString
            .length > 0) query("jobid").toString
      else "0"

  val description =
    if (query.contains("description") && Option(query("description"))
            .getOrElse("")
            .toString
            .length > 0) query("description").toString
      else "0"

  val as_view =
    if (query.contains("as_view") && Option(query("as_view"))
            .getOrElse("")
            .toString
            .length > 0) query("as_view").toString
      else "0"

  val push =
    if (query.contains("push") && Option(query("push"))
            .getOrElse("")
            .toString
            .length > 0) query("push").toString
      else "1"
       



    // Finally we construct the Map that is going to be returned
    val value_dictionary: Map[String, String] = Map(
      "country" -> country,
      "path_to_polygons" ->path_to_polygons,
      "output_file" ->output_file,
      "crossdevice" -> crossdevice,
      "nDays" -> nDays,
      "since" -> since,
      "HourFrom" -> HourFrom,
      "HourTo" -> HourTo,
      "UseType" -> UseType,
      "minFreq" -> minFreq,
      "priority" -> priority,
      "queue" -> queue,
      "jobid" -> jobid,
      "description" -> description,
      "as_view" -> as_view,
      "push"-> push)

    println("LOGGER PARAMETERS:")
    println(s"""
    "country" -> $country,
    "path_to_polygons" -> $path_to_polygons,
    "output_file" -> $output_file,    
    "crossdevice" -> $crossdevice,
    "nDays" -> $nDays,
    "since" -> $since,
    "HourFrom" -> $HourFrom,
    "HourTo" -> $HourTo,
    "UseType" -> $UseType,
    "minFreq" -> $minFreq,
    "priority" -> $priority,
    "queue" -> $queue,
    "jobid" -> $jobid,
    "description" -> $description,
    "as_view"-> $as_view,
    "push" -> $push)""")
    value_dictionary
  }

  type OptionMap = Map[Symbol, Any]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--path_geo_json" :: value :: tail =>
        nextOption(map ++ Map('path_geo_json -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val path_geo_json =
      if (options.contains('path_geo_json)) options('path_geo_json).toString
      else ""
   
    // Start Spark Session based on the type of matcher that will be used.
    val spark =
       SparkSession
          .builder()
          .config("spark.serializer", classOf[KryoSerializer].getName)
          .config(
            "spark.kryo.registrator",
            classOf[GeoSparkKryoRegistrator].getName
          )
          // .config("geospark.global.index", "true")
          // .config("geospark.global.indextype", "rtree")
          .config("geospark.join.gridtype", "kdbtree")
          // .config("geospark.join.numpartition", 200)
          .appName("GeoSpark Matcher")
          .getOrCreate()
     

    // Parsing parameters from json file.
    GeoSparkSQLRegistrator.registerAll(spark)
    val value_dictionary = get_variables(spark, path_geo_json)

    // Here we perform the operation

   HomeJobs.get_homejobs(spark, value_dictionary)
   
   NSEAssignation.nse_join(spark, value_dictionary)

   CrossDevicer.cross_device(spark,value_dictionary,column_name = "device_id",header = "true")
   

    // Now we generate the content for the json file.
   if (value_dictionary("push")=="1") {
   
    val json_content = """{"filePath":"%s", "priority":%s,
                                     "queue":"%s", "jobId":%s, "description":"%s","as_view":%s}"""
      .format(
        "/datascience/audiences/crossdeviced/%s_xd".format(value_dictionary("output_file")),
        value_dictionary("priority"),
        value_dictionary("queue"),
        value_dictionary("jobid"),
        value_dictionary("description"),
        value_dictionary("as_view")
      )
      .replace("\n", "")
    println("DEVICER LOG:\n\t%s".format(json_content))

    // Finally we store the json.
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path("/datascience/devicer/processed/%s_test.meta".format(value_dictionary("output_file")))
    )
    os.write(json_content.getBytes)
    fs.close() 
    }
                                    
    }
  }
