package main.scala
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{lit, length, split, col, regexp_replace}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days}

object DataExporter {
  def process_day(spark: SparkSession, day:String, columns: Seq[String], 
        ids_partners: Seq[String],
        filters: String,
        out_path: String,
        del: String) = {
    
    import spark.implicits._
    val data = spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .load("/data/eventqueue/%s/*.tsv.gz".format(day))

    val by_columns = data.select(columns.head, columns.tail: _*).na.fill("")
      
      // Here we filter by event_type 
    val filter_partner = by_columns
        .filter( length($"device_id") >0 && $"id_partner".isin(ids_partners:_*) )
    
    val customm_filter = filter_partner
        .filter(filters)
      
      // transform the multi-value columns into lists
    val data_final = customm_filter
        .select( customm_filter.columns.map( r => regexp_replace(col(r), "\u0001", del).alias(r) ): _* )
      
    // store the results.
    data_final.write.repartition(24)
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("delimiter","\t")
        .option("compression", "gzip")
        .save(out_path + "/" + day)
          
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
  def download_data(spark: SparkSession, meta: String, nDays: Int, from: Int): Unit = {

    val configs = spark.read
            .format("csv")
            .option("sep", ":")
            .load(meta)
            .collect

    val ls = List(configs)(0)

    val mapa: Map[String, String] = ls.map(x => x(0).asInstanceOf[String] -> x(1).asInstanceOf[String] ).toMap

    //cargo variables
    val out_path: String = "/data/exports/%s".format(mapa("exportName"))
    val columns = mapa("fields").split(",").map(x => x.trim).toSeq
    val ids_partners = mapa("partnerIds").split(",").map(x => x.trim.replace("\"", "") ).toSeq
    val filters = mapa("filters")
    val del = mapa("arrayDelimiter")

    // Now we get the list of days to be downloaded
    val format = "yyyy/MM/dd"
    val end   = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    
    // Now we download the data
    days.foreach(day => process_day(spark, day, columns, ids_partners, filters, out_path, del))
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
    val spark = SparkSession.builder
          .appName("Get data for some Partners ID")
          .config("spark.sql.files.ignoreCorruptFiles", "true")
          .config("spark.sql.sources.partitionOverwriteMode","dynamic")
          .getOrCreate()
    
    // Get files
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val lista = fs.listStatus(new Path("/data/exports/"))

    lista.filter(x => x.getPath.toString.endsWith(".meta") )
        .foreach(x => download_data(spark, x.getPath, nDays, from))//println(x.getPath))

    // Finally, we download the data
    //download_data(spark, nDays, from)
  }
}
