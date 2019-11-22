package main.scala
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to generate earnings Report. 
  */
object earningsReportMonthly {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * This method returns a DataFrame with the data from the "data_triplets" pipeline, for the interval
    * of days specified. Basically, it loads every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned. 
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.  
    *
    * @return a DataFrame with the information coming from the read data. Columns: "seg_id","id_partner" and "device_id"
   **/

  def getDataTriplets(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("id_partner","feature","device_id","country")
      .withColumnRenamed("feature", "seg_id")

    df
  }

//////////////////////////////////////////////////////////////

  def getData_xd(
      spark: SparkSession
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val path = "/datascience/audiences/crossdeviced/taxo_gral_joint"

    val df = spark.read
          .option("sep", "\t")
          .option("header", "false")
          .format("csv")
          .load(path)
          .withColumnRenamed("_c1", "device_id")
          .withColumnRenamed("_c2", "segment")
          .select("device_id","segment")
          .withColumn("segment", split(col("segment"), ","))
          .withColumn("segment",col("segment").cast("array<int>"))  //cast each segment string to int (for mapping)

    df
  }
//////////////////////////////////////////////////////////////

  def getData(
      spark: SparkSession,
      nDays: Integer,
      date_current: String,
      path: String
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val formatter = DateTimeFormat.forPattern(format)
    val end = DateTime.parse(date_current, formatter)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)

    df
  }


/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR MERGING DATA     //////////////////////
    *
    */
   /**
    * This method joins "general taxonomy segment values" with data from the "data_triplets" pipeline,
    * obtaining "id_partner","seg_id" and "device_id" values for a given day for general taxo segments.
    * Then it drops duplicates and after that it groups by "device_id" and "id_partner" and counts,
    * obtaining the number of devices per partner per segment.
    *
    * @param df_keys: DataFrame obtained from json queries.
    * @param df_data_keywords: DataFrame obtained from getDataTriplets().
    *
    * @return a DataFrame with "device_type", "device_id", "kws", being "kws" a list of keywords.
   **/

  def getJoint(
      df_taxo: DataFrame,
      df_data_triplets: DataFrame
  ): DataFrame = {

    val df_joint = df_data_triplets
      .join(broadcast(df_taxo), Seq("seg_id"))
      .select("seg_id","id_partner", "device_id","country")
      .dropDuplicates()
    df_joint
  }

//////////////////////////////////////////////////////////////

  def getJoint_xd(
      spark: SparkSession,
      df: DataFrame
  ): DataFrame = {

    /** Read mapping to countries */
    val country_codes = Map("AD" -> 579, "AE" -> 580, "AF" -> 581, "AG" -> 582, "AI" -> 583, "AL" -> 584, "AM" -> 585, "AO" -> 586, "AQ" -> 587, "AR" -> 588, "AS" -> 589, "AT" -> 590, "AU" -> 591, "AW" -> 592, "AX" -> 593, "AZ" -> 594, "BA" -> 595, "BB" -> 596, "BD" -> 597, "BE" -> 598, "BF" -> 599, "BG" -> 600, "BH" -> 601, "BI" -> 602, "BJ" -> 603, "BL" -> 604, "BM" -> 605, "BN" -> 606, "BO" -> 607, "BQ" -> 608, "BR" -> 609, "BS" -> 610, "BT" -> 611, "BV" -> 612, "BW" -> 613, "BY" -> 614, "BZ" -> 615, "CA" -> 616, "CC" -> 617, "CD" -> 618, "CF" -> 619, "CG" -> 620, "CH" -> 621, "CI" -> 622, "CK" -> 623, "CL" -> 624, "CM" -> 625, "CN" -> 626, "CO" -> 627, "CR" -> 628, "CU" -> 629, "CV" -> 630, "CW" -> 631, "CX" -> 632, "CY" -> 633, "CZ" -> 634, "DE" -> 635, "DJ" -> 636, "DK" -> 637, "DM" -> 638, "DO" -> 639, "DZ" -> 640, "EC" -> 641, "EE" -> 642, "EG" -> 643, "EH" -> 644, "ER" -> 645, "ES" -> 646, "ET" -> 647, "FI" -> 648, "FJ" -> 649, "FK" -> 650, "FM" -> 651, "FO" -> 652, "FR" -> 653, "GA" -> 654, "GB" -> 655, "GD" -> 656, "GE" -> 657, "GF" -> 658, "GG" -> 659, "GH" -> 660, "GI" -> 661, "GL" -> 662, "GM" -> 663, "GN" -> 664, "GP" -> 665, "GQ" -> 666, "GR" -> 667, "GS" -> 668, "GT" -> 669, "GU" -> 670, "GW" -> 671, "GY" -> 672, "HK" -> 673, "HM" -> 674, "HN" -> 675, "HR" -> 676, "HT" -> 677, "HU" -> 678, "ID" -> 679, "IE" -> 680, "IL" -> 681, "IM" -> 682, "IN" -> 683, "IO" -> 684, "IQ" -> 685, "IR" -> 686, "IS" -> 687, "IT" -> 688, "JE" -> 689, "JM" -> 690, "JO" -> 691, "JP" -> 692, "KE" -> 693, "KG" -> 694, "KH" -> 695, "KI" -> 696, "KM" -> 697, "KN" -> 698, "KP" -> 699, "KR" -> 700, "KW" -> 701, "KY" -> 702, "KZ" -> 703, "LA" -> 704, "LB" -> 705, "LC" -> 706, "LI" -> 707, "LK" -> 708, "LR" -> 709, "LS" -> 710, "LT" -> 711, "LU" -> 712, "LV" -> 713, "LY" -> 714, "MA" -> 715, "MC" -> 716, "MD" -> 717, "ME" -> 718, "MF" -> 719, "MG" -> 720, "MH" -> 721, "MK" -> 722, "ML" -> 723, "MM" -> 724, "MN" -> 725, "MO" -> 726, "MP" -> 727, "MQ" -> 728, "MR" -> 729, "MS" -> 730, "MT" -> 731, "MU" -> 732, "MV" -> 733, "MW" -> 734, "MX" -> 735, "MY" -> 736, "MZ" -> 737, "NA" -> 738, "NC" -> 739, "NE" -> 740, "NF" -> 741, "NG" -> 742, "NI" -> 743, "NL" -> 744, "NO" -> 745, "NP" -> 746, "NR" -> 747, "NU" -> 748, "NZ" -> 749, "OM" -> 750, "PA" -> 751, "PE" -> 752, "PF" -> 753, "PG" -> 754, "PH" -> 755, "PK" -> 756, "PL" -> 757, "PM" -> 758, "PN" -> 759, "PR" -> 760, "PS" -> 761, "PT" -> 762, "PW" -> 763, "PY" -> 764, "QA" -> 765, "RE" -> 766, "RO" -> 767, "RS" -> 768, "RU" -> 769, "RW" -> 770, "SA" -> 771, "SB" -> 772, "SC" -> 773, "SD" -> 774, "SE" -> 775, "SG" -> 776, "SH" -> 777, "SI" -> 778, "SJ" -> 779, "SK" -> 780, "SL" -> 781, "SM" -> 782, "SN" -> 783, "SO" -> 784, "SR" -> 785, "SS" -> 786, "ST" -> 787, "SV" -> 788, "SX" -> 789, "SY" -> 790, "SZ" -> 791, "TC" -> 792, "TD" -> 793, "TF" -> 794, "TG" -> 795, "TH" -> 796, "TJ" -> 797, "TK" -> 798, "TL" -> 799, "TM" -> 800, "TN" -> 801, "TO" -> 802, "TR" -> 803, "TT" -> 804, "TV" -> 805, "TW" -> 806, "TZ" -> 807, "UA" -> 808, "UG" -> 809, "UM" -> 810, "US" -> 811, "UY" -> 812, "UZ" -> 813, "VA" -> 814, "VC" -> 815, "VE" -> 816, "VG" -> 817, "VI" -> 818, "VN" -> 819, "VU" -> 820, "WF" -> 821, "WS" -> 822, "YE" -> 823, "YT" -> 824, "ZA" -> 825, "ZM" -> 826, "ZW" -> 827)
    val seg_country_map = country_codes.map(_.swap)

    /** udf to get country from segment */
    val udfMap_countries = udf(
      (segments: Seq[Int]) =>
              (segments flatMap (seg_country_map get)).lift(0)) 

    /** Read mapping of xd segments to their parents */
    val mapping1 = spark.read
          .format("csv")
          .option("header", "true")
          .load("/data/metadata/xd_mapping_segments_exclusion.csv")

    val mapping2 = spark.read
          .format("csv")
          .option("header", "true")
          .load("/data/metadata/xd_mapping_segments.csv")

    val xd_map = ((mapping1.union(mapping2)).collect().map(row => (row(1).toString.toInt, row(0).toString.toInt)).toList).toMap

    /** udf to map xd segments to their parent segment */
    val udfMap_segs = udf(
      (segments: Seq[Int]) =>
              segments flatMap (xd_map get) ) 

    /** adds country, maps xd to parent segments and adds id_partner -1 */
    val df_joint = df
                  .withColumn("country",udfMap_countries(col("segment")))
                  .na.drop()
                  .withColumn("segment",udfMap_segs(col("segment")))
                  .withColumn("id_partner",lit("-1"))
                  .withColumn("segment", explode(col("segment")))
     df_joint
  }

  def getTotals(
      df: DataFrame,
      date_current: String
  ): DataFrame = {

    val df_total = df
      .groupBy("id_partner", "segment")
      .agg(sum(col("device_unique")) as "device_unique")
      .withColumn("country", lit("NN"))
      .withColumn("day", lit(date_current))
      .select("day", "id_partner", "segment", "country", "device_unique")

    df_total
  }

  def getGroupedbyCountry(
      df: DataFrame,
      date_current: String      
  ): DataFrame = {

    val df_grouped_country = df
      .groupBy("id_partner","seg_id","country")
      .count()
      .withColumnRenamed("count", "device_unique")
      .withColumn("day", lit(date_current)) 
      .select("day", "id_partner", "segment", "country", "device_unique")   
    df_grouped_country
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR SAVING DATA     //////////////////////
    *
    */
  /**
    * This method saves to /datascience/reports/earnings/, the filename is the current date.
    *
    * @param data: DataFrame that will be saved.
    * @param path: path to save to.
    *
  **/
  def saveData(
      data: DataFrame,
      path: String
  ) = {

    data.write
      .format("parquet")
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     XD REPORT     //////////////////////
    *
    */

  def getDataReport_xd(
      spark: SparkSession,
      date_current: String) = {
       
  /** Read from "taxo_gral_joint" database */
    val dfx = getData_xd(spark = spark)

    /** Get joint df with countries and xd parent segments*/
    val df_xd = getJoint_xd(spark = spark, df = dfx)

    /**  Get number of devices per partner_id per segment per country */
    val df_grouped_country =
      getGroupedbyCountry(df = df_xd, date_current = date_current)

    val dir = "/datascience/reports/monthly_earnings/"

    /** Here we store the first report */
    val savepath1 = dir + "xd_country"

    saveData(data = df_grouped_country, path = savepath1)

    /**  Get number of devices per partner_id per segment */
    val df1 = getData(
      spark = spark,
      nDays = 1,
      date_current = date_current,
      path = savepath1
    )

    val df_totals = getTotals(df = df1, date_current)

    /** Here we store the second report */
    val savepath2 = dir + "xd"

    saveData(data = df_totals, path = savepath2)

  }
  
  }

 /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD/REPORT     //////////////////////
    *
    */
  /**
    * Given ndays, since, returns a report of number of device ids per partner per segment (with and without country).
    * For regular segments and xd segments.
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param ndays: number of days to query.
    * @param since: number of days since to query.
    *
    * As a result this method stores files in /datascience/reports/monthly_earnings/ partitioned by day
  **/

  def getDataReport(
      spark: SparkSession,
      nDays: Integer,
      since: Integer) = {

    val date_now = DateTime.now
    val date_since = date_now.minusDays(since)
    val date_current = date_since.toString("yyyyMMdd")

    println("INFO:\n\tDay: %s".format(date_current))

    /** Read from "data_triplets" database */
    val df = getDataTriplets(
      spark = spark,
      nDays = nDays,
      since = since
    ).filter("id_partner NOT IN (1, 119)")

     /**  Get number of devices per partner_id per segment per country */
    val df_grouped_country = getGroupedbyCountry(df = df, date_current = date_current)

    val dir = "/datascience/reports/monthly_earnings/"

    /** Here we store the first report */
    val savepath1 = dir + "base_country"

    saveData(data = df_grouped_country, path = savepath1)   

    /**  Get number of devices per partner_id per segment */
    val df1 = getData(
      spark = spark,
      nDays = 1,
      date_current = date_current,
      path = savepath1
    )

    val df_totals = getTotals(df = df1, date_current)

    /** Here we store the second report */
    val savepath2 = dir + "base"

    saveData(data = df_totals, path = savepath2)

    /** XD SEGMENTS **/
    var savepath_xd: String = ""

    val day_current = date_since.toString("dd")

    /** If it's the first day of the month, xd segments distribution is calculated again. */
    if (("01").contains(day_current)) {
      getDataReport_xd(spark = spark, date_current = date_current)
    } else {
      val date_previous = date_now.toString("yyyyMM01")
      val path = dir + "xd/day="
      savepath_xd = path + date_previous

      val conf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(conf)

      if (!(fs.exists(new org.apache.hadoop.fs.Path(savepath_xd)))) {

        getDataReport_xd(spark = spark, date_current = date_previous)
      }

    }    

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
      case "--since" :: value :: tail =>
        nextOption(map ++ Map('since -> value.toInt), tail)
    }
  }    

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(Args: Array[String]) {

    // Parse the parameters
    val options = nextOption(Map(), Args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 30
    val since = if (options.contains('since)) options('since) else 1

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("EarningsReportMonthly")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()
    
    getDataReport(
       spark = spark,
       nDays = nDays,
       since = since)

    getDataReport_xd(
      spark = spark)  
    
  }
}
