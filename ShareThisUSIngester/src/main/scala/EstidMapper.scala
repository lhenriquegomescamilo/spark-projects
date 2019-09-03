package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{lit, col, upper, collect_list}
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ SaveMode, DataFrame }

object EstidMapper {

  /**
    * This function takes a day as input, and produces a set of tuples, where every tuple has the
    * estid, device_id, device_type, and day.
    */
  def getEstIdsMatching(spark: SparkSession, day: String) = {
    val df = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      .filter(
        "d17 is not null and event_type IN ('sync', 'ltm_sync')"
      )
      .select("d17", "device_id", "country")
      .withColumn("day", lit(day.replace("/", "")))
      .withColumnRenamed("d17", "estid")
      .dropDuplicates()

    df.write
      .format("parquet")
      .partitionBy("day", "country")
      .mode("overwrite")
      .save("/datascience/sharethis/estid_table/")
  }

  def getEstIdsMatchingAT(spark: SparkSession, start: String, end: String){
    import spark.implicits._
    val input = spark.read
        .format("parquet")
        .load("/datascience/sharethis/estid_table/")
        .filter(($"day" >= start && $"day" <= end ))
        .dropDuplicates()

    val group_by = input
        .groupBy("country", "estid")
        .agg(collect_list("device_id").as("device_id"))
    
    group_by.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("country")
        .save("/datascience/sharethis/estid_active_table/")
  }


  def getEstidTable(spark: SparkSession, nDays: Int = 60): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(1)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/sharethis/estid_table"
    
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days.map(day => path+"/day=%s".format(day))
                          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files:_*)

    df
  }

  def crossDeviceTable(spark: SparkSession) = {
    // Get the estid table
    val estid_table = getEstidTable(spark)
      .withColumn("device_id", upper(col("device_id")))
      .select("device_id", "d17")

    // Get DrawBridge Index. Here we transform the device id to upper case too.
    val db_data = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index")
      .filter("index_type = 'coo' and device_type IN ('and', 'ios')")
      .withColumn("index", upper(col("index")))
      .select("index", "device", "device_type")

    // Here we do the cross-device per se.
    val cross_deviced = db_data
      .join(estid_table, db_data.col("index") === estid_table.col("device_id"))
      .select("d17", "device", "device_type")
      .withColumnRenamed("d17", "estid")

    // Finally, we store the result obtained.
    val output_path =
      "/datascience/sharethis/estid_madid_table/"
    cross_deviced.write.mode(SaveMode.Overwrite).save(output_path)
  }

  def download_data(spark: SparkSession, nDays: Int, from: Int): Unit = {
    val format = "yyyy/MM/dd"
    val end   = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    days.foreach(day => getEstIdsMatching(spark, day))
  }

  def download_data_AT(spark: SparkSession, from: Int, evalDays: Int): Unit = {
    val format = "yyyyMMdd"
    val dt_end = DateTime.now.minusDays(from)
    val end = dt_end.toString(format)
    val start = dt_end.minusDays(evalDays).toString(format)

    getEstIdsMatchingAT(spark, start, end)
  }

  type OptionMap = Map[Symbol, Int]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toInt), tail)
      case "--mode" :: value :: tail =>
        nextOption(map ++ Map('mode -> value.toInt), tail)
      case "--evalDays" :: value :: tail =>
        nextOption(map ++ Map('evalDays -> value.toInt), tail)
    }
  }

  def main(args: Array[String]) {
    val options = nextOption(Map(), args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 1
    val from = if (options.contains('from)) options('from) else 1
    val mode = if (options.contains('mode)) options('mode) else 0
    val evalDays = if (options.contains('evalDays)) options('evalDays) else 20

    val spark = SparkSession.builder
        .appName("Run matching estid-device_id")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()

    if (mode == 0 || mode == 2){
        download_data(spark, nDays, from)
    }

    if (mode == 1 || mode == 2){
        download_data_AT(spark, from, evalDays)
    }

    //if (DateTime.now.getDayOfWeek()==7){
    //  crossDeviceTable(spark)
    //}
  }

  /*def main(args: Array[String]) {
    val spark = SparkSession.builder
        .appName("Run matching estid-device_id")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    val today = DateTime.now()
    val days = (1 until 2).map(
      days =>
        getEstIdsMatching(
          spark,
          today.minusDays(days).toString("yyyy/MM/dd")
        )
    )

    if (DateTime.now.getDayOfWeek()==7){
      crossDeviceTable(spark)
    }
  }*/
}
