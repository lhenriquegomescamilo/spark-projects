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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, max, broadcast}

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
          "/datascience/data_audiences_streaming/hour=%s*".format(
            day.replace("-", "")
          )
      )
    // .filter(path => fs.exists(new Path(path)))

    // Finally, we load all the data
    val events_data = spark.read
      .option("basePath", "/datascience/data_audiences_streaming/")
      .parquet(paths: _*)
      .select("device_id", "country", "datetime", column)
      .filter("event_type NOT IN ('xp', 'xd')")

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
        22502,22502|22504,22504|22506,22506|22508,22508
        26,26|32,32|36,36|59,59|61,61|82,82|85,85|92,92|104,104|118,118|129,129|131,131|141,141
        144,144|145,145|147,147|149,149|150,150|152,152|154,154|155,155|158,158|160,160|165,165
        166,166|177,177|178,178|210,210|213,213|218,218|224,224|225,225|226,226|230,230|245,245
        247,247|250,250|264,264|265,265|270,270|275,275|276,276|302,302|305,305|311,311|313,313
        314,314|315,315|316,316|317,317|318,318|322,322|323,323|325,325|326,326|352,352|353,353
        354,354|356,356|357,357|358,358|359,359|363,363|366,366|367,367|374,374|377,377|378,378
        379,379|380,380|384,384|385,385|386,386|389,389|395,395|396,396|397,397|398,398|399,399
        401,401|402,402|403,403|404,404|405,405|409,409|410,410|411,411|412,412|413,413|418,418
        420,420|421,421|422,422|429,429|430,430|432,432|433,433|434,434|440,440|441,441|446,446
        447,447|450,450|451,451|453,453|454,454|456,456|457,457|458,458|459,459|460,460|462,462
        463,463|464,464|465,465|467,467|895,895|898,898|899,899|909,909|912,912|914,914|915,915
        916,916|917,917|919,919|920,920|922,922|923,923|928,928|929,929|930,930|931,931|932,932
        933,933|934,934|935,935|937,937|938,938|939,939|940,940|942,942|947,947|948,948|949,949
        950,950|951,951|952,952|953,953|955,955|956,956|957,957|1005,1005|1116,1116|1159,1159|1160,1160
        1166,1166|2064,2064|2623,2623|2635,2635|2636,2636|2660,2660|2719,2719|2720,2720|2721,2721
        2722,2722|2723,2723|2724,2724|2725,2725|2726,2726|2727,2727|2733,2733|2734,2734|2735,2735
        2736,2736|2737,2737|2743,2743|3010,3010|3011,3011|3012,3012|3013,3013|3014,3014|3015,3015
        3016,3016|3017,3017|3018,3018|3019,3019|3020,3020|3021,3021|3022,3022|3023,3023|3024,3024
        3025,3025|3026,3026|3027,3027|3028,3028|3029,3029|3030,3030|3031,3031|3032,3032|3033,3033
        3034,3034|3035,3035|3036,3036|3037,3037|3038,3038|3039,3039|3040,3040|3041,3041|3042,3042
        3043,3043|3044,3044|3045,3045|3046,3046|3047,3047|3048,3048|3049,3049|3050,3050|3051,3051
        3055,3055|3076,3076|3077,3077|3084,3084|3085,3085|3086,3086|3087,3087|3302,3302|3303,3303
        3308,3308|3309,3309|3310,3310|3388,3388|3389,3389|3418,3418|3420,3420|3421,3421|3422,3422
        3423,3423|3450,3450|3470,3470|3472,3472|3473,3473|3564,3564|3565,3565|3566,3566|3567,3567
        3568,3568|3569,3569|3570,3570|3571,3571|3572,3572|3573,3573|3574,3574|3575,3575|3576,3576
        3577,3577|3578,3578|3579,3579|3580,3580|3581,3581|3582,3582|3583,3583|3584,3584|3585,3585
        3586,3586|3587,3587|3588,3588|3589,3589|3590,3590|3591,3591|3592,3592|3593,3593|3594,3594
        3595,3595|3596,3596|3597,3597|3598,3598|3599,3599|3600,3600|3730,3730|3731,3731|3732,3732
        3733,3733|3779,3779|3782,3782|3843,3843|3844,3844|3913,3913|3914,3914|3915,3915|4097,4097|5025,5025|5310,5310|5311,5311
        103917,103917|103918,103918|103919,103919|103920,103920|103921,103921|103922,103922
        103923,103923|103924,103924|103925,103925|103926,103926|103927,103927|103928,103928
        103929,103929|103930,103930|103931,103931|99038,99038|103932,103932|103933,103933|103934,103934
        103935,103935|98867,98867|103936,103936|103937,103937|103938,103938|103939,103939|103940,103940
        103941,103941|103942,103942|103943,103943|103944,103944|103945,103945|103946,103946|103947,103947
        103948,103948|103949,103949|103950,103950|103951,103951|103952,103952|103953,103953|103954,103954
        103955,103955|103956,103956|103957,103957|103958,103958|103959,103959|103960,103960|103961,103961
        103962,103962|103963,103963|103964,103964|103965,103965|103966,103966|103967,103967|103968,103968
        103969,103969|103970,103970|103971,103971|103972,103972|103973,103973|103974,103974|103975,103975
        103976,103976|103977,103977|98868,98868|103978,103978|103979,103979|103980,103980|103981,103981
        103982,103982|103983,103983|103984,103984|103985,103985|103986,103986|103987,103987|103988,103988
        103989,103989|103990,103990|103991,103991|103992,103992|98943,98943|103998,103998|103999,103999
        104000,104000|104001,104001|104002,104002|98942,98942|103993,103993|103994,103994|103995,103995
        103996,103996|103997,103997|98944,98944|104003,104003|104004,104004|104005,104005|104006,104006
        104007,104007|98945,98945|104013,104013|104012,104012|104011,104011|104010,104010|104009,104009
        104008,104008|98946,98946|104014,104014|104016,104016|104015,104015|104018,104018|104017,104017
        104019,104019|98947,98947|104020,104020|104021,104021|104022,104022|104023,104023|104024,104024
        98948,98948|104025,104025|104029,104029|104026,104026|104027,104027|104028,104028|104030,104030
        104259,104259|104608,104608|104609,104609|99585,99585|99586,99586|104389,104389|104390,104390|104391,104391
        104392,104392|104393,104393|104394,104394|104395,104395|99592,99592|99593,99593|104396,104396|104397,104397
        99594,99594|99595,99595|104398,104398|104399,104399|99596,99596|104400,104400|104401,104401|104402,104402
        104403,104403|99597,99597|104404,104404|104405,104405|99598,99598|99599,99599|99600,99600|99601,99601
        99602,99602|99603,99603|99604,99604|104406,104406|104407,104407|99605,99605|99606,99606|104408,104408
        104409,104409|104410,104410|104411,104411|104412,104412|99607,99607|104413,104413|104414,104414|104415,104415
        104416,104416|104417,104417|104418,104418|104419,104419|104420,104420|104421,104421|99637,99637|104422,104422
        104423,104423|104424,104424|104425,104425|104426,104426|104427,104427|104428,104428|104429,104429|104430,104430
        104431,104431|104432,104432|104435,104435|104434,104434|104433,104433|99638,99638|99639,99639|104610,104610
        104611,104611|104612,104612|104613,104613|104614,104614|104615,104615|104616,104616|104617,104617|104618,104618
        104619,104619|104620,104620|104621,104621|104622,104622|99640,99640|99641,99641|99642,99642|99643,99643
        99644,99644|99645,99645|99646,99646"""
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
      (segments: Seq[Int]) =>
        segments
          .map(_.toString)
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
        if (segments.filter(s => country_codes.contains(s)).length > 0) {
          segments
            .filter(s => !country_codes.contains(s))
            .distinct :+ // List of regular segments
            segments
              .filter(s => country_codes.contains(s))
              .groupBy(identity)
              .mapValues(_.size)
              .maxBy(_._2)
              ._1 // Most popular country
        } else segments.distinct
    )

    // Now we can get event data
    val column = "all_segments"
    val events_data = get_event_data(spark, nDays, from, column)

    // Here we do the mapping from original segments to the cross-deviced segments
    val windowing = Window.partitionBy(col("device_id")).orderBy(col("datetime").desc)
    val new_segments = events_data
      .na
      .drop()
      .withColumn("row_number", row_number.over(windowing))
      .where("row_number = 1")
      .drop("row_number", "datetime")
      .withColumn("new_segment", getItems(col(column)))
      .filter(size(col("new_segment")) > 0)

    // Now we load the cross-device index
    val index =
      spark.read
        .format("parquet")
        .load("/datascience/crossdevice/double_index_individual")

    // Finally, we perform the cross-device and keep only the new devices with their types and the
    // new segments.
    val joint = index
      .join(new_segments, index.col("index") === new_segments.col("device_id"))
      // .groupBy("device", "device_type")
      // .agg(flatten(collect_list("new_segment")).alias("new_segment"))
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
  def exclusionCrossDevice(
      spark: SparkSession,
      nDays: Integer,
      from: Integer
  ) = {
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
      .select("_c0", "_c1", "_c3") // group, segment id, score
      .rdd
      .map(x => (x(1).toString, (x(0).toString, x(2).toString)))
      .collect()
      .toArray
      .toMap
    val exclusion_segments =
      exclusion_map.keys.toArray.filter(s => mapping_segments.contains(s))
    val new_exclusion_map =
      exclusion_segments.map(s => (mapping(s), exclusion_map(s))).toMap

    // Some useful functions
    // This function obtains the best exclusion segments from a cluster. That is, it checks all the occurrences of exclusion segments
    // in the cluster, calculates a score and pick the segment with highest score.
    val getExclusionSegments = (segments: Seq[String]) =>
      segments
        .map(
          s =>
            (
              s.toString,
              new_exclusion_map(s)._1.toString,
              new_exclusion_map(s)._2.toString.toInt
            )
        )
        .groupBy(s => (s._1, s._2))
        .map(l => (l._1._1, l._1._2, l._2.map(_._3).reduce(_ + _))) // Here I get the total score per segment
        .groupBy(_._2) //group by exclusion group
        .values // For every group I have a list of tuples of this format (segment, group, total_score)
        .map(l => l.maxBy(_._3)._1) // Here I save the segment with best score for eah group
        .toSeq

    // This method takes the list of original segments and only keeps the country with most occurrences
    val getMostPopularCountry = (segments: Seq[String]) =>
      segments
        .filter(s => country_codes.contains(s))
        .groupBy(identity)
        .mapValues(_.size)
        .maxBy(_._2)
        ._1 // Most popular country

    // getItems function takes a list of segments, checks whether those segments are in the cross-device mapping, if not it filters them out,
    // and also checks that the segments are not exclusive. Finally, it maps the original segments into the cross-device segments.
    val getItems = udf(
      (segments: Seq[Int]) =>
        if (segments.exists(s => exclusion_segments.contains(s.toString))) {
          segments
            .map(_.toString)
            .filter(
              segment =>
                exclusion_segments.contains(segment) || country_codes
                  .contains(segment)
            )
            .map(mapping(_))
        } else Seq()
    )
    // This function takes a list of lists and returns only a list with all the values.
    val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)

    // This function returns the best segment for every exclusion group concatenated with the country
    val getSegments = udf(
      (segments: Seq[String]) =>
        if (segments.filter(s => country_codes.contains(s)).length > 0) {
          getExclusionSegments(segments.filter(s => !country_codes.contains(s))) :+ getMostPopularCountry(
            segments
          )
        } else getExclusionSegments(segments)
    )

    // Now we can get event data
    val events_data = get_event_data(spark, nDays, from, "segments")

    // Here we do the mapping from original segments to the cross-deviced segments
    val new_segments = events_data
      .withColumn("new_segment", getItems(col("segments")))
      .filter(size(col("new_segment")) > 0)

    // Now we load the cross-device index
    val index =
      spark.read
        .format("parquet")
        .load("/datascience/crossdevice/double_index_individual")

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
        nextOption(map ++ Map('exclusion -> 0), tail)
      case "--merge" :: tail =>
        nextOption(map ++ Map('merge -> 0), tail)
    }
  }

  def mergeData(spark: SparkSession) = {
    val gral = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/audiences/crossdeviced/taxo_gral/")
      .withColumnRenamed("_c0", "device_id")
      .withColumnRenamed("_c1", "device_type")
      .withColumnRenamed("_c2", "segment_ids")
    val excl = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/audiences/crossdeviced/taxo_gral_exclusion/")
      .withColumnRenamed("_c0", "device_id")
      .withColumnRenamed("_c1", "device_type")
      .withColumnRenamed("_c2", "exclusion_ids")

    val concatUDF = udf(
      (segments: String, exclusion: String) =>
        if (segments.length > 0 && exclusion.length > 0)
          segments + "," + exclusion
        else if (segments.length > 0) segments
        else exclusion
    )
    val joint = excl
      .join(gral, Seq("device_id", "device_type"), "outer")
      .na
      .fill("")
      .withColumn("ids", concatUDF(col("segment_ids"), col("exclusion_ids")))

    // Get DrawBridge Index. Here we transform the device id to upper case too.
    val typeMap = Map(
      "coo" -> "web",
      "and" -> "android",
      "ios" -> "ios",
      "con" -> "TV",
      "dra" -> "drawbridge"
    )
    val mapUDF = udf((dev_type: String) => typeMap(dev_type))

    joint
      .select("device_type", "device_id", "ids")
      .withColumn("device_type", mapUDF(col("device_type")))
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("sep", "\t")
      .save("/datascience/audiences/crossdeviced/taxo_gral_joint")
  }

  def main(Args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), Args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 30
    val from = if (options.contains('from)) options('from) else 1
    val regular = if (options.contains('exclusion)) false else true
    val merge = if (options.contains('merge)) true else false

    // First we obtain the Spark session
    val conf = new SparkConf()
      .set("spark.sql.files.ignoreCorruptFiles", "true")
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
      if (merge) {
        mergeData(spark)
      } else {
        regularCrossDevice(spark, nDays, from)
      }
    } else {
      exclusionCrossDevice(spark, nDays, from)
    }

  }
}
