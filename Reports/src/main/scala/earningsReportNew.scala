package main.scala
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to generate earnings Report. 
  */
object earningsReportNew {

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
    * @return a DataFrame with the information coming from the read data. Columns: "segment","id_partner" and "device_id"
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
      .select("id_partner","feature","device_id","country","day")
      .withColumnRenamed("feature", "segment")

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

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR MERGING DATA     //////////////////////
    *
    */
   /**
    * This method is a filter that keeps only general taxo segments.
    * It joins "general taxonomy segment values" with data from the "data_triplets" pipeline,
    * obtaining "id_partner","segment", "device_id" and "country", values for general taxo segments.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param df: DataFrame obtained from data_triplets pipeline.
    *
    * @return a DataFrame with "device_id", "segment", "id_partner", "country","day".
   **/

  def getJoint(
      spark: SparkSession,
      df: DataFrame
  ): DataFrame = {

    /** Read standard taxonomy segment_ids */
    val taxo_path = "/datascience/misc/standard_ids.csv"
    val df_taxo =  spark.read.format("csv")
        .option("header", "true")
        .load(taxo_path)
        .withColumnRenamed("seg_id", "segment")

    val db = df
      .join(broadcast(df_taxo), Seq("segment"))
      .select("segment","id_partner", "device_id","country","day")
      //.dropDuplicates()
    db
  }

     /**
    * This method is a filter that keeps only devices from last day and general taxonomy segments.
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param ndays: number of days to query.
    * @param since: number of days since to query.
    * @param date_current: date for filename. 
    *
    * @return a DataFrame with "device_id", "segment", "id_partner", "country","day".
   **/

  def saveRelevantDevicesDF(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ) = {

    /** Read from "data_triplets" database and get relevant devices */
    val df_nDays = getDataTriplets(spark, nDays, since)
    val df1 = getDataTriplets(spark, 1, since)

    /**  Join data_triplets with taxo segments */
    val df_nDays_taxo: DataFrame = getJoint(spark = spark,
                                 df = df_nDays)  

    val df1_taxo: DataFrame = getJoint(spark = spark,
                            df = df1)  

    /**  Get only users that appeared last day */    
    val users = df1_taxo.select("device_id").distinct()

    val df = df_nDays_taxo.join(users, Seq("device_id"), "inner")

    /** Here we store the relevant devices join */
    saveData(data = df,
             path = "/datascience/reports/earnings/temp/")
     
    }    

 /**
    * This method joins crossdeviced segments with their mappings, add countries by mapping certain segments,
    * obtaining "segment", "device_id" and "country" values. "id_partner" set to -1 by default.
    *
    * @param df: DataFrame obtained from audiences/crossdeviced/taxo_gral_joint
    *
    * @return a DataFrame with "device_id", "segment", "id_partner", "country"
   **/

  def getJoint_xd(
      spark: SparkSession,
      df: DataFrame
  ): DataFrame = {

    /** Read mapping to countries */
    val country_codes = Map("AD" -> 579, "AE" -> 580, "AF" -> 581, "AG" -> 582, "AI" -> 583, "AL" -> 584, "AM" -> 585, "AO" -> 586, "AQ" -> 587, "AR" -> 588, "AS" -> 589, "AT" -> 590, "AU" -> 591, "AW" -> 592, "AX" -> 593, "AZ" -> 594, "BA" -> 595, "BB" -> 596, "BD" -> 597, "BE" -> 598, "BF" -> 599, "BG" -> 600, "BH" -> 601, "BI" -> 602, "BJ" -> 603, "BL" -> 604, "BM" -> 605, "BN" -> 606, "BO" -> 607, "BQ" -> 608, "BR" -> 609, "BS" -> 610, "BT" -> 611, "BV" -> 612, "BW" -> 613, "BY" -> 614, "BZ" -> 615, "CA" -> 616, "CC" -> 617, "CD" -> 618, "CF" -> 619, "CG" -> 620, "CH" -> 621, "CI" -> 622, "CK" -> 623, "CL" -> 624, "CM" -> 625, "CN" -> 626, "CO" -> 627, "CR" -> 628, "CU" -> 629, "CV" -> 630, "CW" -> 631, "CX" -> 632, "CY" -> 633, "CZ" -> 634, "DE" -> 635, "DJ" -> 636, "DK" -> 637, "DM" -> 638, "DO" -> 639, "DZ" -> 640, "EC" -> 641, "EE" -> 642, "EG" -> 643, "EH" -> 644, "ER" -> 645, "ES" -> 646, "ET" -> 647, "FI" -> 648, "FJ" -> 649, "FK" -> 650, "FM" -> 651, "FO" -> 652, "FR" -> 653, "GA" -> 654, "GB" -> 655, "GD" -> 656, "GE" -> 657, "GF" -> 658, "GG" -> 659, "GH" -> 660, "GI" -> 661, "GL" -> 662, "GM" -> 663, "GN" -> 664, "GP" -> 665, "GQ" -> 666, "GR" -> 667, "GS" -> 668, "GT" -> 669, "GU" -> 670, "GW" -> 671, "GY" -> 672, "HK" -> 673, "HM" -> 674, "HN" -> 675, "HR" -> 676, "HT" -> 677, "HU" -> 678, "ID" -> 679, "IE" -> 680, "IL" -> 681, "IM" -> 682, "IN" -> 683, "IO" -> 684, "IQ" -> 685, "IR" -> 686, "IS" -> 687, "IT" -> 688, "JE" -> 689, "JM" -> 690, "JO" -> 691, "JP" -> 692, "KE" -> 693, "KG" -> 694, "KH" -> 695, "KI" -> 696, "KM" -> 697, "KN" -> 698, "KP" -> 699, "KR" -> 700, "KW" -> 701, "KY" -> 702, "KZ" -> 703, "LA" -> 704, "LB" -> 705, "LC" -> 706, "LI" -> 707, "LK" -> 708, "LR" -> 709, "LS" -> 710, "LT" -> 711, "LU" -> 712, "LV" -> 713, "LY" -> 714, "MA" -> 715, "MC" -> 716, "MD" -> 717, "ME" -> 718, "MF" -> 719, "MG" -> 720, "MH" -> 721, "MK" -> 722, "ML" -> 723, "MM" -> 724, "MN" -> 725, "MO" -> 726, "MP" -> 727, "MQ" -> 728, "MR" -> 729, "MS" -> 730, "MT" -> 731, "MU" -> 732, "MV" -> 733, "MW" -> 734, "MX" -> 735, "MY" -> 736, "MZ" -> 737, "NA" -> 738, "NC" -> 739, "NE" -> 740, "NF" -> 741, "NG" -> 742, "NI" -> 743, "NL" -> 744, "NO" -> 745, "NP" -> 746, "NR" -> 747, "NU" -> 748, "NZ" -> 749, "OM" -> 750, "PA" -> 751, "PE" -> 752, "PF" -> 753, "PG" -> 754, "PH" -> 755, "PK" -> 756, "PL" -> 757, "PM" -> 758, "PN" -> 759, "PR" -> 760, "PS" -> 761, "PT" -> 762, "PW" -> 763, "PY" -> 764, "QA" -> 765, "RE" -> 766, "RO" -> 767, "RS" -> 768, "RU" -> 769, "RW" -> 770, "SA" -> 771, "SB" -> 772, "SC" -> 773, "SD" -> 774, "SE" -> 775, "SG" -> 776, "SH" -> 777, "SI" -> 778, "SJ" -> 779, "SK" -> 780, "SL" -> 781, "SM" -> 782, "SN" -> 783, "SO" -> 784, "SR" -> 785, "SS" -> 786, "ST" -> 787, "SV" -> 788, "SX" -> 789, "SY" -> 790, "SZ" -> 791, "TC" -> 792, "TD" -> 793, "TF" -> 794, "TG" -> 795, "TH" -> 796, "TJ" -> 797, "TK" -> 798, "TL" -> 799, "TM" -> 800, "TN" -> 801, "TO" -> 802, "TR" -> 803, "TT" -> 804, "TV" -> 805, "TW" -> 806, "TZ" -> 807, "UA" -> 808, "UG" -> 809, "UM" -> 810, "US" -> 811, "UY" -> 812, "UZ" -> 813, "VA" -> 814, "VC" -> 815, "VE" -> 816, "VG" -> 817, "VI" -> 818, "VN" -> 819, "VU" -> 820, "WF" -> 821, "WS" -> 822, "YE" -> 823, "YT" -> 824, "ZA" -> 825, "ZM" -> 826, "ZW" -> 827)
    val seg_country_map = country_codes.map(_.swap) //invert map

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
                  .withColumn("id_partner",lit(-1))
                  .withColumn("segment", explode(col("segment")))
     df_joint
  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR TRANSFORMING DATA     //////////////////////
    *
    */
     /**
    * This method first partitions by "segment and "country", orders by "day"
    * and gets only the last partner that added a given segment for a given country.
    * After that it groups by "id_partner" and "segment" and "country" and counts,
    * obtaining the number of devices per partner per segment per country.
    *
    * @param df: DataFrame obtained from data_tripĺets, with segments from general taxonomy.
    *
    * @return a DataFrame with "segment","id_partner","device_unique", "country"
   **/


  def getGroupedbyCountry(
      dfy: DataFrame,
      date_current: String
  ): DataFrame = {

    val df_grouped_country = dfy
      .groupBy("id_partner","segment","country")
      .count()
      .withColumnRenamed("count", "device_unique")
      .withColumn("day", lit(date_current))
      .select("day","id_partner","segment","country","device_unique") 

  df_grouped_country
  }

   def getCountbyCountry(
      spark: SparkSession,
      df: DataFrame,
      date_current: String
  ): DataFrame = {

    val window = Window.partitionBy(col("segment"),col("device_id"),col("country")).orderBy(col("day").desc)  

    val dfy = df
      .withColumn("rn", row_number.over(window)).where(col("rn") === 1).drop("rn")
    
    val data = getGroupedbyCountry(dfy = dfy,
                                   date_current = date_current)

    data
  }

   /**
    * This method first partitions by "segment", orders by "day"
    * and gets only the last partner that added a given segment.
    * After that it groups by "id_partner" and "segment" and counts,
    * obtaining the number of devices per partner per segment.
    * Adds a ficticious column "country" with value "NN" to append this report to the other report with countries.
    *
    * @param df: DataFrame obtained from data_tripĺets, with segments from general taxonomy.
    *
    * @return a DataFrame with "segment","id_partner","device_unique", "country" (NN)
   **/

   def getTotals(
      df: DataFrame,
      date_current: String
  ): DataFrame = {

    val df_total = df
      .groupBy("id_partner", "segment")
      .agg(sum("device_unique"))
      .withColumn("country", lit("NN"))
      .withColumn("day", lit(date_current))
      .select("day","id_partner","segment","country","device_unique") 

    df_total
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
    * @param subdir: Subdir of root dir to save to.
    * @param date_current: date for filename. 
    *
  **/

  def saveData(
      data: DataFrame,
      path: String
  ) = {

    data
      .write
      .format("parquet")
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .save(path)
  }
  
    /**
    * This method appends to a given savepath.
    *
    * @param data: DataFrame that will be saved.
    * @param savepath: path to save data to.
    *
  **/

  def appendData(
        data: DataFrame,
        savepath: String
        ): String = {
      data
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .save(savepath)

      savepath
    }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     XD REPORT     //////////////////////
    *
    */

  def getDataReport_xd(
        spark: SparkSession,
        date_current: String
        ) = {
        
      /** Read from "taxo_gral_joint" database */
      val dfx =  getData_xd(spark = spark)

      /** Get joint df with countries and xd parent segments*/
      val df_xd = getJoint_xd(spark = spark,
                            df = dfx) 

      /**  Get number of devices per partner_id per segment per country */
      val df_grouped_country = getGroupedbyCountry(dfy = df_xd,
                                                   date_current = date_current)

      val dir = "/datascience/reports/earnings/"

      /** Here we store the first report */
      val savepath1 = dir + "xd_country"

      saveData(data = df_grouped_country,
               path = savepath1)

      /**  Get number of devices per partner_id per segment */
      val df1 = getData(spark = spark,
                        nDays = 1,
                        date_current = date_current,
                        path = savepath1)

      val df_totals= getTotals(df = df1,
                               date_current)

      /** Here we store the second report */
      val savepath2 = dir + "xd"

      saveData(data = df_totals,
              path = savepath2)            

    }    

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD/REPORT     //////////////////////
    *
    */

  /**
    * Given ndays, since, returns a report of number of device ids per partner per segment (with and without country).
    * For general taxonomy segments and xd segments.
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param ndays: number of days to query.
    * @param since: number of days since to query.
    *
    * As a result this method stores the file in /datascience/reports/gain/file_name_currentdate.csv.
  **/
  def getDataReport(
      spark: SparkSession,
      nDays: Integer,
      since: Integer) = {

    val date_now = DateTime.now
    val date_since = date_now.minusDays(since)
    val date_current = date_since.toString("yyyyMMdd")  
      
    saveRelevantDevicesDF(spark = spark,
                          nDays = nDays,
                          since = since)

    val dir = "/datascience/reports/earnings/"

    val df = getData(spark = spark,
                     nDays = nDays,
                     date_current = date_current,
                     path = dir + "temp")
    
    /**  Get number of devices per partner_id per segment per country */
    val df_count_country = getCountbyCountry(spark = spark,
                                             df = df,
                                             date_current)                                         

    /** Here we store the first report */
    val savepath1 = dir + "base_country"

    saveData(data = df_count_country,
             path = savepath1)

    /**  Get number of devices per partner_id per segment */
    val df1 = getData(spark = spark,
                      nDays = 1,
                      date_current = date_current,
                      path = savepath1)

    val df_totals= getTotals(df = df1,
                             date_current)

    /** Here we store the second report */
    val savepath2 = dir + "base"

    saveData(data = df_totals,
             path = savepath2)

   
    /** XD SEGMENTS **/

    var savepath_xd : String = ""

    val day_current = date_since.toString("dd")
    /** If it's the first day of the month, xd segments distribution is calculated again. */
    if (("01").contains(day_current)) {
      getDataReport_xd(spark = spark,
                       date_current = date_current)
                                                    }
    else {
      val date_previous = date_now.minusMonths(1).toString("day=yyyyMM01")
      val path = dir + "xd/"
      savepath_xd = dir + date_previous

      val conf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(conf)

      if (!(fs.exists(new org.apache.hadoop.fs.Path(savepath_xd)))) {   
        
        getDataReport_xd(spark = spark,
                        date_current = date_previous)
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
    val nDays = if (options.contains('nDays)) options('nDays) else 60
    val since = if (options.contains('since)) options('since) else 1

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("EarningsReport")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")      
      .getOrCreate()
    
     getDataReport(
       spark = spark,
       nDays = nDays,
       since = since)
    
  }
}
