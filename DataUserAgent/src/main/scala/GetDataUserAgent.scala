package main.scala
import org.apache.spark.sql.functions.{lit, length, split, col, udf}
import org.apache.spark.sql.{SparkSession, Row}
import org.joda.time.{DateTime, Days}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.uaparser.scala.{Parser, CachingParser}
import org.apache.log4j.{Level, Logger}

object GetDataUserAgent {

  /**
    * Given a day as a string, this method downloads the data from the eventqueue, keeps only the
    * user agent and device id, and filters it to keep only the given countries.
    * After that, it stores the results in parquet format with the given day as a partition.
    *
    * @param spark: Spark session that will be used to get the data.
    * @param day: day to be downloaded in the following format: YYYY/MM/DD. String.
    * @param countries: list of countries to be considered.
    *
    * As a result, this function downloads the DataFrame and stores it into a parquet
    * file that has a partition on the day, and the country. It also repartitions the
    * DataFrame before storing it, so that every folder has only 40 files. The
    * directory where the data is stored is /datascience/data_audiences_p.
    */
  def process_day_parquet(
      spark: SparkSession,
      day: String,
      countries: Seq[String]
  ) = {
    // First we read the data from the eventqueue
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load("/data/eventqueue/%s/".format(day))
      .select("device_id", "user_agent", "country", "url")

    // schema to be used for the parsed rdd
    val schema = new StructType()
      .add(StructField("device_id", StringType, false))
      .add(StructField("country", StringType, true))
      .add(StructField("brand", StringType, true))
      .add(StructField("model", StringType, true))
      .add(StructField("browser", StringType, true))
      .add(StructField("os", StringType, true))
      .add(StructField("os_min_version", StringType, true))
      .add(StructField("os_max_version", StringType, true))
      .add(StructField("user_agent", StringType, true))
      .add(StructField("url", StringType, true))
      .add(StructField("event_type", StringType, true))
    val parser = spark.sparkContext.broadcast(CachingParser.default(100000))

    // Here we filter the ros and
    val parsed = data
      .filter(
        "event_type IN ('pv', 'campaign') AND length(user_agent)>0 AND country IN (%s)".format(
          countries.map("'%s'".format(_)).mkString(", ")
        )
      )
      .select("device_id", "user_agent", "country", "url", "event_type")
      .dropDuplicates("device_id")
      .rdd // Now we parse the user agents
      .map(row => (row(0), row(2), parser.value.parse(row(1).toString), row(1), row(3), row(4)))
      .map(
        row =>
          Row(
            row._1, // device_id
            row._2, // country
            row._3.device.brand.getOrElse(""),
            row._3.device.model.getOrElse(""),
            row._3.userAgent.family,
            row._3.os.family,
            row._3.os.major.getOrElse(""),
            row._3.os.minor.getOrElse(""),
            row._4,
            row._5,
            row._6
          )
      )

    // Finally we store the information in parquet files
    val df = spark
      .createDataFrame(parsed, schema)
      .withColumn("day", lit(day.replace("""/""", "")))
      .coalesce(40)
    df.printSchema

    df.write
      .format("parquet")
      .partitionBy("day", "country")
      .mode("append")
      .save(
        "/datascience/data_useragents/"
      )
    println("Day %s processed!".format(day))
  }

  /**
    * This method downloads the data for the last N days.
    * Basically, this method prepares all the meta-data, such as the countries
    * to be considered. It also configures the dates that will be
    * downloaded. It starts downloading data from the actual day minus 1
    * (yesterday).
    *
    * @param spark: Spark session that will be used to get the data.
    * @param nDays: number of days to be downloaded. Integer.
    * @param from: number of days to be skipped. Integer.
    */
  def download_data(spark: SparkSession, nDays: Int, from: Int): Unit = {
    // Here we set the list of values that will be considered
    val countries =
      List("AR", "MX", "CL", "CO", "PE", "US", "BR", "UY", "EC", "BO")

    // Now we get the list of days to be downloaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we download the data
    days.foreach(
      day => process_day_parquet(spark, day, countries)
    )
  }

  type OptionMap = Map[Symbol, Int]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toInt), tail)
    }
  }

  /**
    * This method performs the whole execution. It takes up to two parameters:
    *    - nDays: number of days to be downloaded.
    *    - from: number of days to be skipped starting from today. Meaning, if it is one,
    *    - the current day is skipped, and it will start downloading from yesterday.
    * Once all the parameters are processed, it starts the SparkSession and continues
    * with the download process.
    */
  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 1
    val from = if (options.contains('from)) options('from) else 1

    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Get data for custom audiences")
      .getOrCreate()

    // Finally, we download the data
    download_data(spark, nDays, from)
  }
}
