package main.scala

import org.joda.time.{Days, DateTime}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.log4j.{Level, Logger}

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
      """4097,150405|2660,150201|103917,150447|103918,150453|2721,150213|2623,150183|2724,150231|2726,150243|2736,150273|3085,150381|2722,150219
      2723,150225|2734,150261|2727,150249|2725,150237|2735,150267|2737,150279|2733,150255|103919,150459|103920,150465|103921,150471|210,150021
      103922,150477|103923,150483|103924,150489|103925,150495|103926,150501|103927,150507|166,150003|103928,150513|103929,150519|103930,150525
      103931,150531|103932,150537|99038,150441|103933,150543|103934,150549|103935,150555|103936,150561|98867,150429|103937,150567|178,150015
      103938,150573|177,150009|103939,150579|165,149997|322,150159|316,150141|317,150147|315,150135|2719,150207|3010,150285|302,150105|305,150111
      311,150117|314,150129|313,150123|155,149979|318,150153|3011,150291|103940,150585|3012,150297|160,149991|323,150165|103941,150591|325,150171
      103942,150597|36,149871|3021,150351|3022,150357|3018,150333|3019,150339|3020,150345|103943,150603|218,150033|61,149883|103944,150609
      103945,150615|6116,150417|103946,150621|118,149913|103947,150627|103948,150633|103949,150639|103950,150645|103951,150651|103952,150657
      103953,150663|103954,150669|104,149907|103955,150675|103956,150681|103957,150687|103958,150693|103959,150699|103960,150705|103961,150711
      103962,150717|103963,150723|92,149901|103964,150729|103965,150735|145,149943|103966,150741|85,149895|103967,150747|82,149889|213,150027
      3913,150399|103968,150753|6115,150411|103969,150759|103970,150765|103971,150771|103972,150777|103973,150783|131,149925|149,149955|3016,150321
      3014,150309|3013,150303|3015,150315|144,149937|150,149961|147,149949|226,150051|224,150039|225,150045|103974,150789|230,150057|103975,150795
      103976,150801|32,149865|103977,150807|26,149859|3077,150375|3017,150327|2636,150195|3055,150363|141,149931|3087,150393|3086,150387|129,149919
      2635,150189|245,150063|326,150177|103978,150813|103979,150819|103980,150825|103981,150831|103982,150837|103983,150843|98868,150435|103984,150849
      103985,150855|3076,150369|154,149973|158,149985|103986,150861|103987,150867|152,149967|275,150099|247,150069|103988,150873|103989,150879
      103990,150885|103991,150891|103992,150897|59,149877|6906,150423|265,150087|270,150093|264,150081|250,150075|144757,155793|145391,155979
      136569,155589|145393,155985|136571,155595|145395,155991|136573,155601|145397,155997|136575,155607|145399,156003|136577,155613|136483,155403
      144761,155799|145407,156027|136585,155637|145403,156015|136581,155625|145405,156021|136583,155631|145409,156033|136587,155643|145401,156009
      136579,155619|145381,155949|136559,155559|144751,155781|145373,155925|136549,155535|145375,155931|136551,155541|144745,155769|145339,155907
      136543,155517|145369,155913|136545,155523|144743,155763|145337,155901|136541,155511|145371,155919|136547,155529|145379,155943|136557,155553
      145377,155937|136555,155547|144749,155775|145299,155841|136497,155439|145297,155835|136495,155433|145295,155829|136493,155427|145289,155811
      136491,155421|144713,155745|145301,155847|136499,155445|136487,155409|144703,155739|136489,155415|145309,155871|136529,155481|145317,155895
      136537,155505|145313,155883|136533,155493|145311,155877|136531,155487|145315,155889|136535,155499|144741,155757|145385,155961|136563,155571
      145383,155955|136561,155565|145389,155973|136567,155583|145387,155967|136565,155577|144755,155787|136649,155685|136657,155709|136661,155721
      136599,155673|136603,155679|136595,155667|136651,155691|136655,155703|136593,155661|136665,155733|136663,155727|136653,155697|136589,155649
      136591,155655|136659,155715|144765,155805|145303,155853|136501,155451|145291,155817|136503,155457|145293,155823|136505,155463|145305,155859
      136507,155469|145307,155865|136527,155475|144729,155751|3418,151659|3420,151665|3421,151671|3473,151677|103998,149667|98943,149601
      103999,149673|104000,149679|104001,149685|104002,149691|98942,149595|103993,149637|103994,149643|103995,149649|103996,149655|103997,149661
      104003,149697|104004,149703|104005,149709|98944,149607|104006,149715|104007,149721|12,149565|13,149571|14,149577|15,149583|16,149589
      104013,149757|104012,149751|104011,149745|104010,149739|104009,149733|104008,149727|98945,149613|104014,149763|104016,149775|104015,149769
      104018,149787|104017,149781|104019,149793|98946,149619|104020,149799|104021,149805|104022,149811|104023,149817|104024,149823|98947,149625
      104025,149829|104029,149853|104026,149835|104027,149841|104028,149847|98948,149631|2,149501|3,149509|4,149517|5,149525|6,149533|7,149541
      8,149549|9,149557|28049,154779|28050,154785|28051,154791|28052,154797|28053,154803|28054,154809|28047,154767|28048,154773|2062,154209
      2063,154215|104030,155379|104259,155385|104608,155391|104609,155397|5025,155373|34791,154845|34798,154887|34788,154827|49909,154941
      49911,154953|49912,154959|49919,155001|49917,154989|49928,155055|49931,155073|49930,155067|49914,154971|49923,155025|49932,155079
      49920,155007|49915,154977|49921,155013|49922,155019|49924,155031|49925,155037|49910,154947|49918,154995|49913,154965|49933,155085
      49926,155043|49916,154983|49929,155061|49927,155049|34805,154929|34796,154875|34792,154851|34802,154911|34803,154917|34799,154893
      34795,154869|34794,154863|34804,154923|34789,154833|34801,154905|49935,155097|49936,155103|49937,155109|49938,155115|49939,155121
      49940,155127|49943,155145|49941,155133|49942,155139|49944,155151|49945,155157|49946,155163|49947,155169|49948,155175|49934,155091
      49949,155181|49950,155187|49951,155193|49952,155199|49953,155205|49954,155211|49955,155217|49956,155223|49957,155229|49958,155235
      49959,155241|49960,155247|49961,155253|49962,155259|49963,155265|49964,155271|49965,155277|49966,155283|34787,154821|34790,154839
      34800,154899|34793,154857|34806,154935|34786,154815|34797,154881|1087,154191|1088,154197|1089,154203|928,151341|929,151347|99585,151887
      930,151353|931,151359|99586,151893|932,151365|366,150951|367,150957|377,150969|374,150963|5022,151875|104389,152013|104390,152019|413,151095
      378,150975|379,150981|380,150987|3303,151629|460,151227|462,151233|3039,151605|3040,151611|3041,151617|389,151011|402,151053|938,151395
      933,151371|401,151047|405,151071|403,151059|935,151383|404,151065|937,151389|939,151401|934,151377|467,151257|940,151407|384,150993
      385,150999|432,151137|433,151143|465,151251|3915,151869|104391,152025|104392,152031|104393,152037|386,151005|434,151149|440,151155
      441,151161|1005,151479|2720,151503|420,151107|421,151113|422,151119|430,151131|418,151101|3038,151599|3037,151593|429,151125|3036,151587
      3030,151551|955,151461|952,151449|104394,152043|950,151437|949,151431|953,151455|948,151425|3564,151683|951,151443|104395,152049
      352,150903|99592,151899|3026,151527|3025,151521|920,151323|99593,151905|104396,152055|104397,152061|915,151299|99594,151911|923,151335
      3023,151509|99595,151917|3027,151533|919,151317|909,151281|104398,152067|917,151311|104399,152073|104400,152079|104401,152085|99596,151923
      104402,152091|104403,152097|922,151329|3029,151545|3028,151539|99597,151929|912,151287|104404,152103|104405,152109|3024,151515|916,151305
      914,151293|99598,151935|99599,151941|3308,151635|956,151467|464,151245|3309,151641|99600,151947|957,151473|463,151239|3302,151623
      99602,151959|99601,151953|99604,151971|104406,152115|104407,152121|99605,151977|3389,151653|104408,152127|104409,152133|104410,152139
      104411,152145|99606,151983|104412,152151|99603,151965|3388,151647|104413,152157|104414,152163|99607,151989|947,151419|942,151413|358,150933
      898,151269|104415,152169|411,151083|104416,152175|412,151089|363,150945|359,150939|357,150927|895,151263|899,151275|410,151077|104417,152181
      104418,152187|104419,152193|354,150915|104420,152199|356,150921|353,150909|104421,152205|99637,151995|104422,152211|399,151041|396,151023|104423,152217
      104424,152223|3031,151557|3033,151569|104425,152229|3032,151563|104426,152235|104427,152241|104428,152247|104429,152253|104430,152259|104431,152265|104432,152271|1166,151497|3035,151581|397,151029|104435,152289|104434,152283|3034,151575|104433,152277|398,151035|395,151017|99638,152001|3583,151797|1160,151491|3575,151749|3582,151791|3588,151821|3593,151833|454,151197|3597,151851|3581,151785|3576,151755|3579,151773|3599,151857|3567,151701|3584,151803|3592,151827|3573,151737|3574,151743|3596,151845|3580,151779|453,151191|3577,151761|3568,151707|3600,151863|3566,151695|450,151179|3587,151815|1159,151485|3578,151767|3565,151689|3571,151725|3572,151731|3569,151713|3570,151719|3595,151839|451,151185|11227,151881|457,151209|456,151203|447,151173|459,151221|99639,152007|458,151215|3585,151809|446,151167|581,152397|584,152415|635,152721|579,152385|586,152427|583,152409|587,152433|582,152403|771,153537|640,152751|588,152439|585,152421|592,152463|591,152457|590,152451|594,152475|610,152571|597,152493|596,152487|601,152517|598,152499|615,152601|603,152529|605,152541|614,152595|724,153255|607,152553|595,152481|613,152589|609,152565|606,152547|600,152511|599,152505|602,152523|611,152577|630,152691|695,153081|625,152661|616,152607|608,152559|765,153501|793,153669|624,152655|626,152667|633,152709|814,153795|627,152673|697,153093|699,153105|700,153111|622,152643|628,152679|676,152967|629,152685|631,152697|637,152733|638,152739|641,152757|643,152769|788,153639|580,152391|645,152781|780,153591|778,153579|646,152787|811,153777|642,152763|647,152793|755,153441|648,152799|649,152805|653,152829|654,152835|663,152889|657,152853|660,152871|661,152877|656,152847|667,152913|662,152883|665,152901|670,152931|669,152925|658,152859|659,152865|664,152895|666,152907|671,152937|672,152943|677,152973|675,152961|673,152949|678,152979|683,153009|679,152985|685,153021|686,153027|680,152991|612,152583|682,153003|632,152703|687,153033|593,152469|702,153123|617,152613|623,152649|652,152823|668,152919|674,152955|650,152811|727,153273|721,153237|759,153465|772,153543|792,153663|810,153771|817,153813|818,153819|681,152997|688,153039|690,153051|692,153063|689,153045|691,153057|703,153129|693,153069|694,153075|696,153087|701,153117|704,153135|710,153171|713,153189|705,153141|709,153165|714,153195|707,153153|711,153177|712,153183|726,153267|722,153243|720,153231|736,153327|734,153315|733,153309|723,153249|731,153297|715,153201|728,153279|732,153303|729,153285|824,153855|735,153321|651,152817|717,153213|716,153207|725,153261|718,153219|730,153291|737,153333|738,153339|747,153393|746,153387|743,153369|740,153351|742,153363|748,153399|741,153357|745,153381|739,153345|749,153405|750,153411|744,153375|756,153447|763,153489|761,153477|751,153417|754,153435|764,153495|752,153423|753,153429|757,153453|762,153483|760,153471|655,152841|644,152775|619,152625|634,152715|806,153747|620,152631|618,152619|639,152745|766,153507|770,153531|767,153513|769,153525|822,153843|589,152445|604,152535|698,153099|782,153603|719,153225|758,153459|815,153801|777,153573|706,153147|787,153633|783,153609|768,153519|773,153549|781,153597|776,153567|789,153645|790,153651|784,153615|708,153159|791,153657|825,153861|774,153555|786,153627|775,153561|621,152637|785,153621|779,153585|796,153687|807,153753|797,153693|684,153015|794,153675|799,153705|795,153681|798,153699|802,153723|804,153735|801,153717|800,153711|803,153729|805,153741|808,153759|809,153765|812,153783|813,153789|820,153831|816,153807|819,153825|821,153837|823,153849|636,152727|826,153867|827,153873|104610,155295|104611,155301|104612,155307|104613,155313|104614,155319|104615,155325|104616,155331|104617,155337|104618,155343|104619,155349|104620,155355|104621,155361|3050,155289|104622,155367|3051,154269|3049,154263|2064,154221|12914,154323|12906,154317|12900,154299|12905,154311|12902,154305|4538,154293|3843,154281|3844,154287|3048,154257|3450,154275|3045,154245|3046,154251|3043,154233|3044,154239|3042,154227|560,152295|561,152301|562,152307|99640,154149|1190,153885|1195,153915|1191,153891|1324,153927|1194,153909|1193,153903|1069,153879|3230,154089|1192,153897|99641,154155|1335,153957|1336,153963|1339,153975|1338,153969|1357,154059|1326,153939|99642,154161|1325,153933|1327,153945|1328,153951|3226,154065|3227,154071|99643,154167|1323,153921|3228,154077|3229,154083|1344,153999|1340,153981|1341,153987|1342,153993|1345,154005|99644,154173|1346,154011|1347,154017|1348,154023|1349,154029|1350,154035|1352,154047|1351,154041|99645,154179|1354,154053|4641,154095|4642,154101|4648,154131|4650,154143|4646,154125|4645,154119|4649,154137|99646,154185|4643,154107|4644,154113|567,152337|563,152313|564,152319|565,152325|568,152343|566,152331|571,152361|570,152355|573,152373|572,152367|569,152349|574,152379|4806,154713|4785,154587|4784,154581|4786,154593|4769,154491|4770,154497|4809,154731|4810,154737|4771,154503|4773,154515|4772,154509|4777,154539|4775,154527|4774,154521|4778,154545|4776,154533|4782,154569|4781,154563|4783,154575|4779,154551|4780,154557|4790,154617|4791,154623|4789,154611|4804,154701|4796,154653|4793,154635|4795,154647|4802,154689|4797,154659|4801,154683|4798,154665|4799,154671|4794,154641|4792,154629|4800,154677|4803,154695|4805,154707|4788,154605|4787,154599|4808,154725|4807,154719|4811,154743|4814,154761|4813,154755|4812,154749|4815,154431|4750,154443|4751,154449|4754,154341|4755,154347|4753,154335|4756,154353|4758,154365|4757,154359|4759,154371|4752,154329|4760,154377|4768,154425|4767,154419|4762,154389|4763,154395|4764,154401|4765,154407|4766,154413|4761,154383|4744,154455|4745,154461|4746,154467|4747,154473|4748,154479|4749,154485|4816,154437"""
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
    val windowing =
      Window.partitionBy(col("device_id")).orderBy(col("datetime").desc)
    val new_segments = events_data.na
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
    val column = "segments"
    val events_data = get_event_data(spark, nDays, from, "segments")

    // Here we do the mapping from original segments to the cross-deviced segments
    val new_segments = events_data.na
      .drop()
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

  def getDataTriplets(
      spark: SparkSession,
      nDays: Int = -1,
      from: Int = 1,
      path: String = "/datascience/data_triplets/segments/"
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val df = if (nDays > 0) {
      // read files from dates
      val format = "yyyyMMdd"
      val endDate = DateTime.now.minusDays(from)
      val days =
        (0 until nDays.toInt)
          .map(endDate.minusDays(_))
          .map(_.toString(format))
      // Now we obtain the list of hdfs folders to be read
      val hdfs_files = days
        .map(day => path + "/day=%s/".format(day))
        .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      spark.read.option("basePath", path).parquet(hdfs_files: _*)
    } else {
      // read all date files
      spark.read.load(path)
    }
    df
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
  def crossDevice(spark: SparkSession, nDays: Int, from: Int) = {
    // This is the list of segments that we are going to cross-device.
    // This list contains two columns: parentId and segmentId. The parentId is the
    // original segment, the segmentId is the cross-device counterpart.
    val mapping = spark.read
      .format("csv")
      .option("header", "true")
      .load("/data/metadata/xd_mapping_segments.csv")
      .collect()
      .map(row => (row(0).toString.toInt, row(1).toString.toInt))
      .toMap

    val map_udf = udf(
      (segment: Integer) => if (m.contains(segment)) m(segment) else -1
    )

    // This pipeline contains the devices along with their segments.
    // We will remove duplicates here.
    val data_triplets = getDataTriplets(spark, nDays, from)
      .withColumnRenamed("feature", "segment_id")
      .select("device_id", "segment_id")
      .withColumn("segment_id", map_udf(col("segment_id")))
      .filter("segment_id > 0")
      .distinct()

    // Now we transform original segment ids to cross-device segment ids.
    val devices_segments = data_triplets
      .join(
        broadcast(mapping.withColumnRenamed("parentId", "segment_id")),
        Seq("segment_id")
      )
      .select("device_id", "segmentId")

    // This is the Tapad Index, that we will use to cross-device the users.
    val index = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index_individual")

    // This is the actual cross-device
    val cross_deviced = devices_segments
      .withColumnRenamed("device_id", "index")
      .withColumn("index", upper(col("index")))
      .join(index.withColumn("index", upper(col("index"))), Seq("index"))
      .select("device", "segmentId", "device_type")

    // Finally, we group by user, so that we store the list of segments per user, and then store everything
    // in the given folder
    cross_deviced
      .groupBy("device", "device_type")
      .agg(collect_list("segmentId") as "segments")
      .withColumn("segments", concat_ws(",", col("segments")))
      .select("device_type", "device", "segments")
      .write
      .format("csv")
      .option("sep", "\t")
      .mode("overwrite")
      .save("/datascience/audiences/crossdeviced/taxo_gral_new")
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

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val conf = new SparkConf()
      .set("spark.sql.files.ignoreCorruptFiles", "true")
      .setAppName("Cross Device")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    val spark = sqlContext.sparkSession

    if (regular) {
      if (merge) {
        mergeData(spark)
      } else {
        //regularCrossDevice(spark, nDays, from)
        crossDevice(spark, nDays, from)
      }
    } else {
      exclusionCrossDevice(spark, nDays, from)
    }

  }
}
