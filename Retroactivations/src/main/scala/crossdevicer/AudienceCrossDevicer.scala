package main.scala.crossdevicer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col, coalesce, udf}
import org.apache.spark.sql.SaveMode

/*
 * This object receives an audience and cross-device it using a cross-deviced index.
 * The result is stored in a new folder.
 */
object AudienceCrossDevicer {

  /**
    * This method generates the cross device of a given audience. It takes the audience given
    * by parameters, then loads the Cross-Device Index and performs a join. Also, it allows
    * to perform a filter in the cross-device index, so that only a part of the devices are
    * used for the operation.
    *
    * @param spark: Spark session object that will be used to load and store the DataFrames.
    * @param audience_name: name of the folder or file where the audience is stored. This folder
    * must exist in /datascience/audiences/output folder. It must be a csv file, separated by spaces.
    * Also the first column must be called _c0.
    * @param index_filter: this is a filter that can be used to use only a part of the index. For
    * example: index_filter = "index_type = 'c' AND device_type IN ('a', 'i')". This example is used
    * to pass from a cookie to android and ios devices.
    *
    * As a result, this method stores the cross-deviced audience in /datascience/audiences/crossdeviced
    * using the same name as the one sent by parameter.
    */
  def cross_device(
      spark: SparkSession,
      path_audience: String,
      index_filter: String,
      sep: String = " ",
      column_name: String = "_c1"
  ) {
    // First we get the audience. Also, we transform the device id to be upper case.
    //val path_audience = "/datascience/audiences/output/%s".format(audience_name)
    val audience_name = path_audience.split("/").last
    val audience_file = spark.read
      .format("csv")
      .option("sep", sep)
      .load(path_audience)
      .withColumnRenamed(column_name, "device_id")
    if (audience_file.columns.toList.contains("device_id")) {
      val audience = audience_file
        .withColumn("index", upper(col("device_id")))
        .withColumnRenamed("_c2", "segments")
        .distinct()

      // Get crossdevice Index. Here we transform the device id to upper case too.
      val typeMap = Map(
        "coo" -> "web",
        "and" -> "android",
        "ios" -> "ios",
        "con" -> "TV",
        "dra" -> "drawbridge"
      )
      val mapUDF = udf((dev_type: String) => typeMap(dev_type))

      val db_data = spark.read
        .format("parquet")
        .load("/datascience/crossdevice/double_index")
        .filter(index_filter)
        .withColumn("index", upper(col("index")))
        .select("index", "device", "device_type")
        .filter("device_type IN ('coo', 'ios', 'and')")
        .withColumn("device_type", mapUDF(col("device_type")))

      // Here we do the cross-device per se.
      val cross_deviced = db_data
        .join(audience, Seq("index"), "right_outer")
        .withColumn("device_id", coalesce(col("device"), col("device_id")))
        .withColumn("device_type", coalesce(col("device_type"), col("_c0")))
        .select("device_type", "device_id", "segments")

      // Finally, we store the result obtained.
      val output_path =
        "/datascience/audiences/crossdeviced/%s_xd".format(audience_name)

      // HOTFIX: we append the original users from the audience to the crossdevice file
      // val final_df = audience_file.unionAll(cross_deviced)

      cross_deviced.write
        .format("csv")
        .option("sep", "\t")
        .mode(SaveMode.Overwrite)
        .save(output_path)
    }
  }

  type OptionMap = Map[Symbol, String]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--filter" :: value :: tail =>
        nextOption(map ++ Map('filter -> value), tail)
      case "--sep" :: value :: tail =>
        nextOption(map ++ Map('sep -> value), tail)
      case "--column" :: value :: tail =>
        nextOption(map ++ Map('column -> value), tail)
    }
  }

  def main(args: Array[String]) {
    // First of all, we parse the parameters
    val audience_name = args.last
    val options =
      nextOption(Map(), args.toList.slice(0, args.toList.length - 1))
    val index_filter = if (options.contains('filter)) options('filter) else ""
    val sep = if (options.contains('sep)) options('sep) else " "
    val column = if (options.contains('column)) options('column) else "_c0"
    println(
      "\n\nLOGGER - PARAMETERS: \ncolumn: %s\nsep: '%s'\nindex filter: %s\npath: %s\n\n"
        .format(column, sep, index_filter, audience_name)
    )

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("audience generator by keywords")
      .getOrCreate()

    // Finally, we perform the cross-device
    cross_device(spark, audience_name, index_filter, sep, column)
  }

}
