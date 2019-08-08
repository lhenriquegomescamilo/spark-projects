package main.scala
import org.apache.spark.sql.functions.{lit, length, split, col}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days}

object GetDataPartnerID {
  /**
   * Given a day as a string, this method downloads the data from the eventqueue, keeps only the 
   * given set of columns, and filters it to keep only the seleceted event_types.
   * After that, it stores the results in parquet format with the given day as a partition.
   * 
   * @param spark: Spark session that will be used to get the data.
   * @param day: day to be downloaded in the following format: YYYY/MM/DD. String.
   * @param columns: list of columns to be downloaded.
   * @param event_types: list of event types to be considered.

   * As a result, this function downloads the DataFrame and stores it into a parquet
   * file that has a partition on the day. It also repartitions the
   * DataFrame before storing it, so that every folder has only 5 files. The 
   * directory where the data is stored is /datascience/data_partner/.
   */
  def process_day_parquet(spark: SparkSession, day:String, columns: Seq[String], 
                            ids_partners: Seq[String]) = {
      // Here we read the data into DataFrames and select the proper columns
      val data = spark.read.format("csv").option("sep", "\t").option("header", "true")
                                         .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      val by_columns = data.select(columns.head, columns.tail: _*).na.fill("")
      
      // Here we filter by event_type 
      val filtered = by_columns.filter(length(col("device_id"))>0 && col("id_partner").isin(ids_partners:_*) )//.repartition(500)
      
      // transform the multi-value columns into lists
      val ready = filtered.withColumn("day", lit(day.replace("/", "")))
                          //.withColumn("third_party", split(col("third_party"), "\u0001"))
                          //.withColumn("first_party", split(col("first_party"), "\u0001"))
      
      // store the results.
      ready.coalesce(1).write.mode("append")
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter","\t")
          .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
          .csv("/datascience/data_premium_partner/%s.tsv.gz".format(day))
          //.partitionBy("id_partner", "day")
          

  }
  
  /**
   * This method downloads the data for the last N days for building audiences.
   * Basically, this method prepares all the meta-data, columns, event types,
   * to be considered. It also configures the dates that will be
   * downloaded. It starts downloading data from the actual day minus 1 
   * (yesterday).
   * 
   * @param spark: Spark session that will be used to get the data.
   * @param nDays: number of days to be downloaded. Integer.
   * @param from: number of days to be skipped. Integer.
   **/
  def download_data(spark: SparkSession, nDays: Int, from: Int): Unit = {
    // Here we set the list of values that will be considered
    //val event_types = List("tk", "pv", "data", "batch", "sync", "xp", "retroactive", "xd", "xd_xp")

    val ids_partners = List("167","261","280","289","317","384","385","388","443","464","465","471","475","486","511","517","622","631","632","635","639","641",
      "643","648","651","652","653","656","657","661","662","663","668","693","694","712","713","714","743","748","749","754","775","796","839","841","849","868",
      "872","878","914","918","919","921","927","928","929","930","931","937","957","978","989","997","998","999","1000","1001","1010","1011","1012","1013","1014",
      "1026","1028","1030","1033","1034","1052","1053","1054","1056","1064","1069","1084","1085","1088","1102","1111","1119","1120","1133","1138","1144")

    val columns = """timestamp, id_partner, url_domain, url , referer_domain, referer, first_party, third_party, device_id, device_type, browser, ip
                      """.replace("\n", "").replace(" ", "").split(",").toList
    
    // Now we get the list of days to be downloaded
    val format = "yyyy/MM/dd"
    val end   = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    
    // Now we download the data
    days.foreach(day => process_day_parquet(spark, day, columns, ids_partners))
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
    
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Get data for some Partners ID").getOrCreate()
    
    // Finally, we download the data
    download_data(spark, nDays, from)
  }
}
