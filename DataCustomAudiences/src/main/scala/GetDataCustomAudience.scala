package main.scala
import org.apache.spark.sql.functions.{lit, length, split, col}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days}

object GetDataCustomAudience {
  /**
   * Given a day as a string, this method downloads the data from the eventqueue, keeps only the 
   * given set of columns, and filters it to keep only the given countries and event_types.
   * After that, it stores the results in parquet format with the given day as a partition.
   */
  def process_day_parquet(spark: SparkSession, day:String, columns: Seq[String], 
                          countries: Seq[String], event_types: Seq[String]) = {
      // Here we read the data into DataFrames and select the proper columns
      val data = spark.read.format("csv").option("sep", "\t").option("header", "true")
                                         .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      val by_columns = data.select(columns.head, columns.tail: _*).na.fill("")
      
      // Here we filter by country and event_type
      val filtered = by_columns.filter(length(col("device_id"))>0 && col("country").isin(countries:_*) && 
                                       col("event_type").isin(event_types:_*))
      
      // transform the multi-value columns into lists
      val ready = filtered.withColumn("day", lit(day.replace("/", "")))
                          .withColumn("all_segments", split(col("all_segments"), "\u0001"))
                          .withColumn("third_party", split(col("third_party"), "\u0001"))
                          .withColumn("segments", split(col("segments"), "\u0001"))
      
      // store the results.
      ready.coalesce(40).write.mode("append")
           .partitionBy("day", "country")
           .parquet("/datascience/data_audiences_p/".format(day))
  }
  
      
  def main(args: Array[String]) {    
    val usage = """
        Error while reading the parameters
        Usage: GetDataCustomAudience.jar ndays
        - ndays: number of days to be downloaded from today.
      """
    if (args.length == 0) println(usage)
    
    // First we parse the parameters
    val ndays = args(0).toInt
    
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Get data for custom audiences").getOrCreate()
    
    // Here we set the list of values that will be considered
    val event_types = List("tk", "pv", "data", "batch", "sync", "xp", "retroactive")
    val countries = List("AR", "MX", "CL", "CO", "PE", "US", "BR", "UY", "EC", "BO")
    val columns = List("device_id", "event_type", "country", "segments", "third_party", 
                       "all_segments", "url", "title", "category", "activable", "device_type")
    
    // Now we get the list of days to be downloaded
    val format = "yyyy/MM/dd"
    val start = DateTime.now.minusDays(1+ndays)
    val end   = DateTime.now.minusDays(1)
    val daysCount = Days.daysBetween(start, end).getDays()
    val days = (0 until daysCount).map(end.minusDays(_)).map(_.toString(format))
    
    // Now we download the data
    days.foreach(day => process_day_parquet(spark, day, columns, countries, event_types))
  }
}