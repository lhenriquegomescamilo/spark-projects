package main.scala
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to generate days of volumes by platform for platform Report.
  */
object platformsReport {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
  /**
    * This method returns a DataFrame with the data from the "platforms data" pipeline, for the day specified.
    * A DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the read data.
   **/
  def getPlatformsData(
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
    val path = "/datascience/reports/platforms/data"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("device_id","platforms","segment","day")

    df
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR TRANSFORMING DATA     //////////////////////
    *
    */
  /**
    * This method transform data from platforms data, obtaining volumes per platform.
    * Returns a dataframe.
    *
    * @param data: DataFrame obtained from reading platforms data.
    *
    * @return a DataFrame.
   **/

    /** Convert decimal to binary */
    def toBinary(n: Int): String = n match {
        case 0|1 => s"$n"
        case _   => s"${toBinary(n/2)}${n%2}" }

    /** Format binary number reversed and completing zeros */
    def getStringRepresentation =
        udf((n: Int) =>
            (toBinary(n).reverse.padTo(5, '0')).map(_.toString)
            )

    /** Map representation to combinations of platforms */        
    def getPlatforms =
        udf((j: IndexedSeq[String]) =>
            ((IndexedSeq("d2","d10","d11","d13","d14") zip j).toMap.filter( {case (k,v)=> v=="1"})).keys.toList
            )        
            
    /**
    def getVolumesPlatformFast(
        spark: SparkSession,
        data: DataFrame
    ): DataFrame = {

        val df = data
            .withColumn("platforms", getStringRepresentation(col("platforms")))
            .withColumn("platforms", getPlatforms(col("platforms")))
            .withColumn("platform", explode(col("platforms")))
            .groupBy("platform","segment").count()
            .select("platform","segment","count")
        df    
        }
    */ 

    def getVolumesPlatformFast(
        spark: SparkSession,
        data: DataFrame
    ): DataFrame = {

        val mapping_path = "/datascience/misc/mappingpl.csv"
        val mapping = spark.read.format("csv").option("header", "true").load(mapping_path)
        .withColumn("platforms", split(col("platforms"), ","))
        .select(col("decimal").cast("int"), col("platforms")).as[(Int, List[String])].collect.toMap

        def udfMap = udf((n: Int) => (mapping.get(n) ))      

        val df = data
            .withColumn("platforms",udfMap(col("platforms")))
            .withColumn("platform", explode(col("platforms")))
            .groupBy("platform","segment").count()
            .select("platform","segment","count")
        df    
        }

    /** Read mapping to countries */
    val country_codes = Map("AD" -> 579, "AE" -> 580, "AF" -> 581, "AG" -> 582, "AI" -> 583, "AL" -> 584, "AM" -> 585, "AO" -> 586, "AQ" -> 587, "AR" -> 588, "AS" -> 589, "AT" -> 590, "AU" -> 591, "AW" -> 592, "AX" -> 593, "AZ" -> 594, "BA" -> 595, "BB" -> 596, "BD" -> 597, "BE" -> 598, "BF" -> 599, "BG" -> 600, "BH" -> 601, "BI" -> 602, "BJ" -> 603, "BL" -> 604, "BM" -> 605, "BN" -> 606, "BO" -> 607, "BQ" -> 608, "BR" -> 609, "BS" -> 610, "BT" -> 611, "BV" -> 612, "BW" -> 613, "BY" -> 614, "BZ" -> 615, "CA" -> 616, "CC" -> 617, "CD" -> 618, "CF" -> 619, "CG" -> 620, "CH" -> 621, "CI" -> 622, "CK" -> 623, "CL" -> 624, "CM" -> 625, "CN" -> 626, "CO" -> 627, "CR" -> 628, "CU" -> 629, "CV" -> 630, "CW" -> 631, "CX" -> 632, "CY" -> 633, "CZ" -> 634, "DE" -> 635, "DJ" -> 636, "DK" -> 637, "DM" -> 638, "DO" -> 639, "DZ" -> 640, "EC" -> 641, "EE" -> 642, "EG" -> 643, "EH" -> 644, "ER" -> 645, "ES" -> 646, "ET" -> 647, "FI" -> 648, "FJ" -> 649, "FK" -> 650, "FM" -> 651, "FO" -> 652, "FR" -> 653, "GA" -> 654, "GB" -> 655, "GD" -> 656, "GE" -> 657, "GF" -> 658, "GG" -> 659, "GH" -> 660, "GI" -> 661, "GL" -> 662, "GM" -> 663, "GN" -> 664, "GP" -> 665, "GQ" -> 666, "GR" -> 667, "GS" -> 668, "GT" -> 669, "GU" -> 670, "GW" -> 671, "GY" -> 672, "HK" -> 673, "HM" -> 674, "HN" -> 675, "HR" -> 676, "HT" -> 677, "HU" -> 678, "ID" -> 679, "IE" -> 680, "IL" -> 681, "IM" -> 682, "IN" -> 683, "IO" -> 684, "IQ" -> 685, "IR" -> 686, "IS" -> 687, "IT" -> 688, "JE" -> 689, "JM" -> 690, "JO" -> 691, "JP" -> 692, "KE" -> 693, "KG" -> 694, "KH" -> 695, "KI" -> 696, "KM" -> 697, "KN" -> 698, "KP" -> 699, "KR" -> 700, "KW" -> 701, "KY" -> 702, "KZ" -> 703, "LA" -> 704, "LB" -> 705, "LC" -> 706, "LI" -> 707, "LK" -> 708, "LR" -> 709, "LS" -> 710, "LT" -> 711, "LU" -> 712, "LV" -> 713, "LY" -> 714, "MA" -> 715, "MC" -> 716, "MD" -> 717, "ME" -> 718, "MF" -> 719, "MG" -> 720, "MH" -> 721, "MK" -> 722, "ML" -> 723, "MM" -> 724, "MN" -> 725, "MO" -> 726, "MP" -> 727, "MQ" -> 728, "MR" -> 729, "MS" -> 730, "MT" -> 731, "MU" -> 732, "MV" -> 733, "MW" -> 734, "MX" -> 735, "MY" -> 736, "MZ" -> 737, "NA" -> 738, "NC" -> 739, "NE" -> 740, "NF" -> 741, "NG" -> 742, "NI" -> 743, "NL" -> 744, "NO" -> 745, "NP" -> 746, "NR" -> 747, "NU" -> 748, "NZ" -> 749, "OM" -> 750, "PA" -> 751, "PE" -> 752, "PF" -> 753, "PG" -> 754, "PH" -> 755, "PK" -> 756, "PL" -> 757, "PM" -> 758, "PN" -> 759, "PR" -> 760, "PS" -> 761, "PT" -> 762, "PW" -> 763, "PY" -> 764, "QA" -> 765, "RE" -> 766, "RO" -> 767, "RS" -> 768, "RU" -> 769, "RW" -> 770, "SA" -> 771, "SB" -> 772, "SC" -> 773, "SD" -> 774, "SE" -> 775, "SG" -> 776, "SH" -> 777, "SI" -> 778, "SJ" -> 779, "SK" -> 780, "SL" -> 781, "SM" -> 782, "SN" -> 783, "SO" -> 784, "SR" -> 785, "SS" -> 786, "ST" -> 787, "SV" -> 788, "SX" -> 789, "SY" -> 790, "SZ" -> 791, "TC" -> 792, "TD" -> 793, "TF" -> 794, "TG" -> 795, "TH" -> 796, "TJ" -> 797, "TK" -> 798, "TL" -> 799, "TM" -> 800, "TN" -> 801, "TO" -> 802, "TR" -> 803, "TT" -> 804, "TV" -> 805, "TW" -> 806, "TZ" -> 807, "UA" -> 808, "UG" -> 809, "UM" -> 810, "US" -> 811, "UY" -> 812, "UZ" -> 813, "VA" -> 814, "VC" -> 815, "VE" -> 816, "VG" -> 817, "VI" -> 818, "VN" -> 819, "VU" -> 820, "WF" -> 821, "WS" -> 822, "YE" -> 823, "YT" -> 824, "ZA" -> 825, "ZM" -> 826, "ZW" -> 827)
    val seg_country_map = country_codes.map(_.swap) //invert map

    /** udf to get country from segment */
    def udfMapCountries = udf(
    (segments: Seq[Int]) =>
            (segments flatMap (seg_country_map get)) )
            
    def udfFilterCountries = udf(
    (segments: Seq[Int]) =>
            (segments.filterNot(seg_country_map.keys.toList.contains)))          
    

    def transformDF(
        spark: SparkSession,
        data: DataFrame
    ): DataFrame = {

        val df = data
            .groupBy("device_id","platforms").agg(collect_list(col("segment")) as "segments")
            .withColumn("country",udfMapCountries(col("segments")))
            .withColumn("segments",udfFilterCountries(col("segments")))
            .withColumn("segment", explode(col("segments")))
            .withColumn("country", explode(col("country")))            
            .withColumn("platforms", getStringRepresentation(col("platforms")))
            .withColumn("platforms", getPlatforms(col("platforms")))
            .withColumn("platform", explode(col("platforms")))
            .select("device_id","country","segment","platform")
        df    
        }

     def getVolumesPlatform(
        spark: SparkSession,
        data: DataFrame
    ): DataFrame = {

        val df = data
            .groupBy("platform","segment").count()
        df    
        }       

     def getVolumesPlatformCountry(
        spark: SparkSession,
        data: DataFrame
    ): DataFrame = {

        val df = data
            .groupBy("platform","segment","country").count()
        df    
        }       


  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR SAVING DATA     //////////////////////
    *
    */
  /**
    * This method saves the data generated to /datascience/reports/gain/, the filename is the current date.
    *
    * @param data: DataFrame that will be saved.
    *
  **/

  def saveData(
      df: DataFrame,
      path: String
  ) = {

    df
      .write
      .format("parquet")
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD     //////////////////////
    *
    */
  /**
    * Given ndays, since and a filename, this file gives the number of devices per partner per segment.
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param ndays: number of days to query.
    * @param since: number of days since to query.
    *
    * As a result this method stores the file in /datascience/reports/gain/file_name_currentdate.csv.
  **/

  def getReportPlatformsFast(
      spark: SparkSession,
      nDays: Integer,
      since: Integer) = {

    /**Get current date */
    val format = "yyyyMMdd/"
    val date_current = DateTime.now.minusDays(since).toString(format)
    println("STREAMING LOGGER:\n\tDay: %s".format(date_current))

    /** Read from "platforms/data" */
    val data = getPlatformsData(spark = spark,
                                nDays = nDays,
                                since = since)

    /**  Transform data */
    val df = getVolumesPlatformFast(spark, data = data)
    .withColumn("day", lit(date_current))

    /** Store df */
    val dir = "/datascience/reports/platforms/"
    val path = dir + "done"

    saveData(df = df,
             path = path)


  }

  def getReportPlatformsCountry(
      spark: SparkSession,
      nDays: Integer,
      since: Integer) = {

    val dir = "/datascience/reports/platforms/"

    /**Get current date */
    val format = "yyyyMMdd/"
    val date_current = DateTime.now.minusDays(since).toString(format)
    println("STREAMING LOGGER:\n\tDay: %s".format(date_current))

    /** Read from "platforms/data" */
    val data = getPlatformsData(spark = spark,
                                nDays = nDays,
                                since = since)

    /**  Transform data and save to temp */
    val df_temp = transformDF(spark, data = data)
    .withColumn("day", lit(date_current))

    val temp_path = dir + "temp"
    saveData(df = df_temp,
             path = temp_path)    

    /**  Load transformed data */
    val df = spark.read
          .parquet(temp_path + "/day=" + date_current )             
    
    /** Get report per platform per segment */
    val df1 = getVolumesPlatform(spark, data = df)
     .withColumn("day", lit(date_current))

    val path_report = dir + "done"
    saveData(df = df1,
             path = path_report)

    /** Get report per country per platform per segment */
    val df2 = getVolumesPlatformCountry(spark, data = df)
    .withColumn("day", lit(date_current))
    
    val path_report_country = dir + "done_country"
    saveData(df = df2,
             path = path_report_country)


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
      .appName("PlatformsReportFast")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    getReportPlatformsFast(spark = spark, nDays = nDays, since = since)

  }
}
