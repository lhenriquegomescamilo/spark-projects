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
object platformsData {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * This method returns a DataFrame with the data from the "eventqueue" pipeline, for the day specified.
    * A DataFrame that will be returned. 
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.  
    *
    * @return a DataFrame with the information coming from the read data. Columns: "device_id","third_party" and several platforms.
   **/

  def getDayEventQueue(
      spark: SparkSession,
      since: Integer
  ): DataFrame = {

   // Current day
    val day = DateTime.now.minusDays(since).toString("yyyy/MM/dd/")
    println(
      "STREAMING LOGGER:\n\tDay: %s".format(day)
    )

    val columns = "device_id,third_party,d2,d10,d11,d13,d14".split(",").toList
    
    val df = spark.read
        .option("sep", "\t")
        .option("header", "true")
        .format("csv")
        .load("/data/eventqueue/%s".format(day))
        .select(columns.head, columns.tail: _*) // Here we select the columns to work with
        .filter("event_type != 'sync'") // filter sync, internal event

    df
  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR TRANSFORMING DATA     //////////////////////
    *
    */
   /**
    * This method transform data from the eventqueue, getting the unique users per platform per segment.
    * Returns a dataframe.
    *
    * @param data: DataFrame obtained from reading eventqueue.
    *
    * @return a DataFrame with "platform", "segment", "user_unique".
   **/

  def transformDF(
      data: DataFrame
  ): DataFrame = {

    val df = data
        .withColumn("platforms", udfPlatform(col("d2"), col("d10"), col("d11"), col("d13"), col("d14")))
        .withColumn("platform", explode(col("platforms")))
        .withColumn("segments", split(col("third_party"), "\u0001"))
        .withColumn("segment", explode(col("segments")))
        .select("device_id","segment","platform")
        .groupBy("platform", "segment").agg(countDistinct("device_id") as "user_unique")
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
      data: DataFrame,
      subdir: String
  ) = {

  val dir = "/datascience/reports/gain/"
  val format = "yyyy-MM"
  val date_current = DateTime.now.minusDays(1).toString(format)
  val fileNameFinal = dir + subdir + "/" + date_current

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
  }
  

/**
    ***
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

  def getGrouped(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped = df_joint
      .groupBy("id_partner", "seg_id")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped
  }

  def getGroupedbyCountry(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped_country = df_joint
      .groupBy("id_partner","seg_id","country")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped_country
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
      data: DataFrame,
      subdir: String
  ) = {

  val dir = "/datascience/reports/gain/"
  val format = "yyyy-MM"
  val date_current = DateTime.now.minusDays(1).toString(format)
  val fileNameFinal = dir + subdir + "/" + date_current

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
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
  def getDataReport(
      spark: SparkSession,
    ***
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

  def getGrouped(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped = df_joint
      .groupBy("id_partner", "seg_id")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped
  }

  def getGroupedbyCountry(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped_country = df_joint
      .groupBy("id_partner","seg_id","country")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped_country
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
      data: DataFrame,
      subdir: String
  ) = {

  val dir = "/datascience/reports/gain/"
  val format = "yyyy-MM"
  val date_current = DateTime.now.minusDays(1).toString(format)
  val fileNameFinal = dir + subdir + "/" + date_current

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
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
  def getDataReport(
      spark: SparkSession,///
    ***
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

  def getGrouped(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped = df_joint
      .groupBy("id_partner", "seg_id")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped
  }

  def getGroupedbyCountry(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped_country = df_joint
      .groupBy("id_partner","seg_id","country")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped_country
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
      data: DataFrame,
      subdir: String
  ) = {

  val dir = "/datascience/reports/gain/"
  val format = "yyyy-MM"
  val date_current = DateTime.now.minusDays(1).toString(format)
  val fileNameFinal = dir + subdir + "/" + date_current

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
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
  def getDataReport(
      spark: SparkSession,
    ***
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

  def getGrouped(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped = df_joint
      .groupBy("id_partner", "seg_id")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped
  }

  def getGroupedbyCountry(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped_country = df_joint
      .groupBy("id_partner","seg_id","country")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped_country
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
      data: DataFrame,
      subdir: String
  ) = {

  val dir = "/datascience/reports/gain/"
  val format = "yyyy-MM"
  val date_current = DateTime.now.minusDays(1).toString(format)
  val fileNameFinal = dir + subdir + "/" + date_current

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
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
  def getDataReport(
      spark: SparkSession,
  /****
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

  def getGrouped(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped = df_joint
      .groupBy("id_partner", "seg_id")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped
  }

  def getGroupedbyCountry(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped_country = df_joint
      .groupBy("id_partner","seg_id","country")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped_country
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
      data: DataFrame,
      subdir: String
  ) = {

  val dir = "/datascience/reports/gain/"
  val format = "yyyy-MM"
  val date_current = DateTime.now.minusDays(1).toString(format)
  val fileNameFinal = dir + subdir + "/" + date_current

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
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
  def getDataReport(
      spark: SparkSession,
    ***
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

  def getGrouped(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped = df_joint
      .groupBy("id_partner", "seg_id")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped
  }

  def getGroupedbyCountry(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped_country = df_joint
      .groupBy("id_partner","seg_id","country")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped_country
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
      data: DataFrame,
      subdir: String
  ) = {

  val dir = "/datascience/reports/gain/"
  val format = "yyyy-MM"
  val date_current = DateTime.now.minusDays(1).toString(format)
  val fileNameFinal = dir + subdir + "/" + date_current

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
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
  def getDataReport(
      spark: SparkSession,ces per partner per segment.
    ***
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

  def getGrouped(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped = df_joint
      .groupBy("id_partner", "seg_id")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped
  }

  def getGroupedbyCountry(
      df_joint: DataFrame
  ): DataFrame = {

    val df_grouped_country = df_joint
      .groupBy("id_partner","seg_id","country")
      .count()
      .withColumnRenamed("count", "device_unique")
    df_grouped_country
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
      data: DataFrame,
      subdir: String
  ) = {

  val dir = "/datascience/reports/gain/"
  val format = "yyyy-MM"
  val date_current = DateTime.now.minusDays(1).toString(format)
  val fileNameFinal = dir + subdir + "/" + date_current

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
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
  def getDataReport(
      spark: SparkSession,
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
       
    /** Read from "data_triplets" database */
    val df_data_triplets = getDataTriplets(
      spark = spark,
      nDays = nDays,
      since = since
    )

    /** Read standard taxonomy segment_ids */
    val taxo_path = "/datascience/misc/standard_ids.csv"
    val df_taxo =  spark.read.format("csv").option("header", "true").load(taxo_path)

    /**  Join data_triplets with taxo segments */
    val df_joint = getJoint(
      df_taxo = df_taxo,
      df_data_triplets = df_data_triplets)  

    /**  Get number of devices per partner_id per segment */
    val df_grouped = getGrouped(df_joint = df_joint)

    /** Here we store the first report */
    val subdir1 = "base_report"

    saveData(
      data = df_grouped,
      subdir = subdir1
    )

    /**  Get number of devices per partner_id per segment per country */
    val df_grouped_country = getGroupedbyCountry(df_joint = df_joint)

    /** Here we store the first report by country */
    val subdir2 = "base_report_by_country"

    saveData(
      data = df_grouped_country,
      subdir = subdir2
    )
  }    

  def getDataReport_xd(
      spark: SparkSession) = {
       
    /** Read from "taxo_gral_joint" database */
    val df =  getData_xd(
      spark = spark
    )

    /** Get joint df with countries and xd parent segments*/
    val df_joint = getJoint_xd(
      spark = spark,
      df = df)
  
    /**  Get number of devices per partner_id per segment */
    val df_grouped = getGrouped(df_joint = df_joint)

    /** Here we store the first report */
    val subdir1 = "xd_report"

     saveData(
       data = df_grouped,
       subdir = subdir1
     )

    /**  Get number of devices per partner_id per segment per country */
    val df_grouped_country = getGroupedbyCountry(df_joint = df_joint)

    /** Here we store the first report by country */
    val subdir2 = "xd_report_by_country"

    saveData(
      data = df_grouped_country,
      subdir = subdir2
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
      .appName("EarningsReport")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()
    
     getDataReport(
       spark = spark,
       nDays = nDays,
       since = since)

    //getDataReport_xd(
    //  spark = spark)  
    
  }
}
