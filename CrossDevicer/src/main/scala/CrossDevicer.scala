package main.scala

import org.joda.time.{Days, DateTime}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext, SaveMode}
import org.apache.spark.sql.functions.{
  udf,
  col,
  lit,
  size,
  collect_list,
  concat_ws
}
import org.apache.spark.sql.Column
import org.apache.hadoop.fs.{FileSystem, Path}

object CrossDevicer {

  /**
    * This method loads all the data from the eventqueue into a DataFrame.
    * The data loaded includes only three columns:
    *  - device_id
    *  - country
    *  - third_party
    *
    * It also removes the duplicated device ids, keeping the newest versions.
    * It uses the last N days, starting M days from now. That is, if we want
    * to get the last 30 days starting from yesterday, nDays should be 30 while
    * from should be 1.
    *
    * @param spark: Spark session that will be used to load the data.
    * @param nDays: number of days to be loaded. It is an integer. Default = 30.
    * @param from: number of days to be skipped from now into the past. That is,
    * if this value is 2, then the day of today as well as the day of yesterday
    * will not be considered to load the data. Integer. Default = 1.
    * @param column: column that will be used to get the segments. It might
    * be either segments, all_segments or third_party.
    *
    * @return a DataFrame with 4 columns: device_id, country, and third_party.
    * This DataFrame has no device_id duplicated.
    */
  def get_event_data(
      spark: SparkSession,
      nDays: Int = 30,
      from: Int = 1,
      column: String = "third_party"
  ): DataFrame = {
    // First of all we get the list of days
    val format = "yyyy-MM-dd"
    val start = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(start.minusDays(_)).map(_.toString(format))

    // Second we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Now we obtain the list of files to be loaded
    val paths = days
      .map(
        day =>
          "/datascience/data_audiences_p/day=%s".format(day.replace("-", ""))
      )
      .filter(path => fs.exists(new Path(path)))

    // Finally, we load all the data
    val events_data = spark.read
      .option("basePath", "/datascience/data_audiences_p/")
      .parquet(paths: _*)
      .select("device_id", "country", column)

    events_data
  }

  /**
    * This method generates the taxonomy cross-device. That is, it loads all the users from the last
    * N days, looks for the ones with some of the segment ids that will be cross-deviced, and finally
    * it assigns the new segment. Then it looks for those users in the cross-device index and assigns
    * the new segments to the users that have resulted from the cross-device.
    *
    * @param spark: Spark Session that will be used to load the data.
    * @param nDays: number of days to be loaded for the cross-device
    * @param from: number of days to be skipped from now into the past.
    */
  def regularCrossDevice(spark: SparkSession, nDays: Integer, from: Integer) = {
    // Here we get the mapping of old segments to new segments
    val mapping_s =
      """4752,4752|4753,4753|4754,4754|4755,4755|4756,4756|4757,4757|4758,4758|4759,4759|4760,4760
        4761,4761|4762,4762|4763,4763|4764,4764|4765,4765|4766,4766|4767,4767|4768,4768|4815,4815
        4816,4816|4750,4750|4751,4751|4744,4744|4745,4745|4746,4746|4747,4747|4748,4748|4749,4749
        4769,4769|4770,4770|4771,4771|4772,4772|4773,4773|4774,4774|4775,4775|4776,4776|4777,4777
        4778,4778|4779,4779|4780,4780|4781,4781|4782,4782|4783,4783|4784,4784|4785,4785|4786,4786
        4787,4787|4788,4788|4789,4789|4790,4790|4791,4791|4792,4792|4793,4793|4794,4794|4795,4795
        4796,4796|4797,4797|4798,4798|4799,4799|4800,4800|4801,4801|4802,4802|4803,4803|4804,4804
        4805,4805|4806,4806|4807,4807|4808,4808|4809,4809|4810,4810|4811,4811|4812,4812|4813,4813
        4814,4814|352,5289|366,5290|409,5291|909,5292|353,5293|354,5294|446,5295|395,5296|2,5202
        3,5203|4,5204|5,5205|6,5206|7,5207|8,5208|9,5209|144,5241|275,5242|61,5243|276,5260
        104,5261|165,5262|92,5297|82,5298|150,5299|3013,5300|3014,5301|152,5302|129,5303|32,5304
        2660,5305|26,5306|3055,5307|326,5308|302,5309|224,6731|2064,5310|3050,5311|20125,20125
        20126,20126|22510,22510|22512,22512|20107,20107|20108,20108|20109,20109|20110,20110
        22474,22474|22476,22476|22478,22478|22480,22480|20111,20111|20112,20112|20113,20113
        20114,20114|20115,20115|20116,20116|22482,22482|22484,22484|22486,22486|22488,22488
        22490,22490|22492,22492|20117,20117|20118,20118|22494,22494|22496,22496|20119,20119
        20120,20120|22498,22498|22500,22500|20121,20121|20122,20122|20123,20123|20124,20124
        22502,22502|22504,22504|22506,22506|22508,22508"""
        .replace("\n", "|")
        .split("\\|")
        .map(tuple => (tuple.trim.split(",")(0), tuple.trim.split(",").last))
        .toList
    // This is the list of country codes
    val mapping_c =
      ((579 to 827).map(_.toString) zip (579 to 827).map(_.toString)).toList
    // Now we merge both mappings
    val mapping = (mapping_s ::: mapping_c).toMap
    val mapping_segments = mapping.keys.toArray
    val country_codes = (579 to 827).map(_.toString).toArray
    // Finally we load also the exclusion segments
    val exclusion_segments = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/data/metadata/segment_exclusion.tsv")
      .select("_c1")
      .rdd
      .map(x => x(0).toString)
      .collect()
      .toArray

    // Some useful functions
    // getItems function takes a list of segments, checks whether those segments are in the cross-device mapping, if not it filters them out,
    // and also checks that the segments are not exclusive. Finally, it maps the original segments into the cross-device segments.
    val getItems = udf(
      (segments: Seq[String]) =>
        segments
          .filter(
            segment =>
              mapping_segments.contains(segment) && !exclusion_segments
                .contains(segment)
          )
          .map(mapping(_))
    )
    // This function takes a list of lists and returns only a list with all the values.
    val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)
    // This method takes the list of original segments and only keeps the country with most occurrences
    val getCountry = udf(
      (segments: Seq[String]) =>
        if (segments.filter(s => country_codes.contains(s)).length>0){
          segments.filter(s => !country_codes.contains(s)).distinct :+ // List of regular segments
            segments.filter(s => country_codes.contains(s)).groupBy(identity).mapValues(_.size).maxBy(_._2)._1 // Most popular country
        } else segments.distinct
    )

    // Now we can get event data
    val events_data = get_event_data(spark, nDays, from)

    // Here we do the mapping from original segments to the cross-deviced segments
    val new_segments = events_data
      .withColumn("new_segment", getItems(col("third_party")))
      .filter(size(col("new_segment")) > 0)

    // Now we load the cross-device index
    val index =
      spark.read.format("parquet").load("/datascience/crossdevice/double_index")

    // Finally, we perform the cross-device and keep only the new devices with their types and the
    // new segments.
    val joint = index
      .join(new_segments, index.col("index") === new_segments.col("device_id"))
      .groupBy("device", "device_type")
      .agg(flatten(collect_list("new_segment")).alias("new_segment"))
      .withColumn("new_segment", getCountry(col("new_segment")))
      .withColumn("new_segment", concat_ws(",", col("new_segment")))
      .select("device", "device_type", "new_segment")
      .distinct()

    joint.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("sep", "\t")
      .save("/datascience/audiences/crossdeviced/taxo_gral")
  }



  /**
    * This method generates the taxonomy cross-device for exclusion segments. That is, it loads all 
    * the users from the last N days, looks for the ones with some of the segment ids that will be 
    * cross-deviced and that is in the exclusion set, and finally it assigns the new segment. Then it 
    * looks for those users in the cross-device index and assigns the new segments to the users that 
    * have resulted from the cross-device.
    *
    * @param spark: Spark Session that will be used to load the data.
    * @param nDays: number of days to be loaded for the cross-device
    * @param from: number of days to be skipped from now into the past.
    */
    def exclusionCrossDevice(spark: SparkSession, nDays: Integer, from: Integer) = {
      // Here we get the mapping of old segments to new segments
      val mapping_s =
        """4752,4752|4753,4753|4754,4754|4755,4755|4756,4756|4757,4757|4758,4758|4759,4759|4760,4760
          4761,4761|4762,4762|4763,4763|4764,4764|4765,4765|4766,4766|4767,4767|4768,4768|4815,4815
          4816,4816|4750,4750|4751,4751|4744,4744|4745,4745|4746,4746|4747,4747|4748,4748|4749,4749
          4769,4769|4770,4770|4771,4771|4772,4772|4773,4773|4774,4774|4775,4775|4776,4776|4777,4777
          4778,4778|4779,4779|4780,4780|4781,4781|4782,4782|4783,4783|4784,4784|4785,4785|4786,4786
          4787,4787|4788,4788|4789,4789|4790,4790|4791,4791|4792,4792|4793,4793|4794,4794|4795,4795
          4796,4796|4797,4797|4798,4798|4799,4799|4800,4800|4801,4801|4802,4802|4803,4803|4804,4804
          4805,4805|4806,4806|4807,4807|4808,4808|4809,4809|4810,4810|4811,4811|4812,4812|4813,4813
          4814,4814|352,5289|366,5290|409,5291|909,5292|353,5293|354,5294|446,5295|395,5296|2,5202
          3,5203|4,5204|5,5205|6,5206|7,5207|8,5208|9,5209|144,5241|275,5242|61,5243|276,5260
          104,5261|165,5262|92,5297|82,5298|150,5299|3013,5300|3014,5301|152,5302|129,5303|32,5304
          2660,5305|26,5306|3055,5307|326,5308|302,5309|224,6731|2064,5310|3050,5311|20125,20125
          20126,20126|22510,22510|22512,22512|20107,20107|20108,20108|20109,20109|20110,20110
          22474,22474|22476,22476|22478,22478|22480,22480|20111,20111|20112,20112|20113,20113
          20114,20114|20115,20115|20116,20116|22482,22482|22484,22484|22486,22486|22488,22488
          22490,22490|22492,22492|20117,20117|20118,20118|22494,22494|22496,22496|20119,20119
          20120,20120|22498,22498|22500,22500|20121,20121|20122,20122|20123,20123|20124,20124
          22502,22502|22504,22504|22506,22506|22508,22508"""
          .replace("\n", "|")
          .split("\\|")
          .map(tuple => (tuple.trim.split(",")(0), tuple.trim.split(",").last))
          .toList
      // This is the list of country codes
      val mapping_c =
        ((579 to 827).map(_.toString) zip (579 to 827).map(_.toString)).toList
      // Now we merge both mappings
      val mapping = (mapping_s ::: mapping_c).toMap
      val mapping_segments = mapping_s.map(t => t._1).toArray
      val country_codes = (579 to 827).map(_.toString).toArray
      // Finally we load also the exclusion segments with all the information (group, segment id, and score)
      val exclusion_map = spark.read
        .format("csv")
        .option("sep", "\t")
        .load("/data/metadata/segment_exclusion.tsv")
        .select("_c0", "_c1", "_c3")
        .rdd
        .map(x => (x(1).toString, (x(0).toString, x(2).toString)))
        .collect()
        .toArray
        .toMap
      val exclusion_segments = exclusion_map.keys.toArray.filter(s => mapping_segments.contains(s))
      val new_exclusion_map = exclusion_segments.map(s => (mapping(s), exclusion_map(s))).toMap
  
      // Some useful functions
      // getItems function takes a list of segments, checks whether those segments are in the cross-device mapping, if not it filters them out,
      // and also checks that the segments are not exclusive. Finally, it maps the original segments into the cross-device segments.
      val getItems = udf(
        (segments: Seq[String]) =>
          segments
            .filter(
              segment =>
                exclusion_segments.contains(segment)//mapping_segments.contains(segment) && (exclusion_segments.contains(segment) or country_codes.contains(segment))
            )
            .map(mapping(_))
      )
      // This function takes a list of lists and returns only a list with all the values.
      val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)
      // This method takes the list of original segments and only keeps the country with most occurrences
      val getCountry = udf(
        (segments: Seq[String]) =>
          if (segments.filter(s => country_codes.contains(s)).length>0){
            segments.filter(s => !country_codes.contains(s)).distinct :+ // List of regular segments
              segments.filter(s => country_codes.contains(s)).groupBy(identity).mapValues(_.size).maxBy(_._2)._1 // Most popular country
          } else segments.distinct
      )
      // This function returns the best segment for every exclusion group
      val getSegments = udf(
        (segments: Seq[String]) =>
          segments.map(s => (s.toString, new_exclusion_map(s)._1.toString, new_exclusion_map(s)._2.toString.toInt))
          .groupBy(s => (s._1, s._2)).map(l => (l._1._1, l._1._2, l._2.map(_._3).reduce(_+_))) // Here I get the total score per segment
          .groupBy(_._2) //group by exclusion group
          .values // For every group I have a list of tuples of this format (segment, group, total_score)
          .map(l => l.maxBy(_._3)._1)  // Here I save the segment with best score for eah group
      )
  
      // Now we can get event data
      val events_data = get_event_data(spark, nDays, from)
  
      // Here we do the mapping from original segments to the cross-deviced segments
      val new_segments = events_data
        .withColumn("new_segment", getItems(col("segments")))
        .filter(size(col("new_segment")) > 0)
  
      // Now we load the cross-device index
      val index =
        spark.read.format("parquet").load("/datascience/crossdevice/double_index")
  
      // Finally, we perform the cross-device and keep only the new devices with their types and the
      // new segments.
      val joint = index
        .join(new_segments, index.col("index") === new_segments.col("device_id"))
        .groupBy("device", "device_type")
        .agg(flatten(collect_list("new_segment")).alias("new_segment"))
        .withColumn("new_segment", getSegments(col("new_segment")))
        .withColumn("new_segment", concat_ws(",", col("new_segment")))
        .select("device", "device_type", "new_segment")
        .distinct()
  
      joint.write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .option("sep", "\t")
        .save("/datascience/audiences/crossdeviced/taxo_gral_exclusion")
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
      case "--exclusion" :: tail =>
          nextOption(map ++ Map('exclusion -> true), tail)
    }
  }

  def main(Args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), Args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 30
    val from = if (options.contains('from)) options('from) else 1
    val regular = if (options.contains('exclusion)) false else true

    // First we obtain the Spark session
    val conf = new SparkConf()
      .setAppName("Cross Device")
      .setJars(
        Seq(
          "/home/rely/spark-projects/CrossDevicer/target/scala-2.11/cross-devicer_2.11-1.0.jar"
        )
      )
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    val spark = sqlContext.sparkSession

    if (regular) {
      regularCrossDevice(spark, nDays, from)
    } else {
      exclusionCrossDevice(spark, nDays, from)
    }

  }
}
