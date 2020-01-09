package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType}
import java.util.zip.DataFormatException

object AggregateData {

  def getRawData(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      partners: List[String]
  ): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    var format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_insights/raw"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .flatMap(
        day =>
          partners.map(
            partner => path + "/day=%s/id_partner=%s/".format(day, partner)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    format = "yyyy-MM-dd"
    val today = end.toString(format)

    val df_chkpt = df
      .withColumn("day", lit(today))
      .withColumn("datediff", datediff(col("day"), col("time")))
      .withColumn(
        "periodo",
        when(
          col("datediff") <= 1,
          Seq("Last 1 day", "Last 7 day", "Last 30 day")
        ).otherwise(
          when(col("datediff") <= 7, Seq("Last 7 days", "Last 30 day"))
            .otherwise("Last 30 day")
        )
      )
      .withColumn("periodo", explode(col("periodo")))
      .withColumn("ID", concat(col("periodo"), lit("-"), col("campaign_id")))
      .withColumn(
        "data_type_clk",
        when(col("data_type") == "clk", 1).otherwise(0)
      )
      .withColumn(
        "data_type_imp",
        when(col("data_type") == "imp", 1).otherwise(0)
      )
      .withColumn(
        "data_type_cnv",
        when(col("data_type") == "cnv", 1).otherwise(0)
      )

    df_chkpt
  }

  def aggregateKPIs(df_chkpt: DataFrame, today: String) = {
    // Data Agregada KPIS
    df_chkpt
      .groupBy("id_partner", "ID")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
        sum(col("data_type_imp")).as("impressions"),
        sum(col("data_type_clk")).as("clicks"),
        sum(col("data_type_cnv")).as("conversions")
      )
      .withColumn(
        "hits",
        col("impressions") + col("clicks") + col("conversions")
      )
      .withColumn("day", lit(today))
      .write
      .format("parquet")
      .partitionBy("day", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/data_kpis/")
  }

  def aggregateSegments(df_chkpt: DataFrame, today: String) = {

    // Data Agregada Segments
    val age_segments = List("4", "5", "6", "7", "8", "9")

    val gender_segments = List("2", "3")

    val in_market = List(
      "352",
      "353",
      "354",
      "356",
      "357",
      "358",
      "359",
      "363",
      "366",
      "367",
      "374",
      "377",
      "378",
      "379",
      "380",
      "384",
      "385",
      "386",
      "389",
      "395",
      "396",
      "397",
      "398",
      "399",
      "401",
      "402",
      "403",
      "404",
      "405",
      "409",
      "410",
      "411",
      "412",
      "413",
      "418",
      "420",
      "421",
      "422",
      "429",
      "430",
      "432",
      "433",
      "434",
      "440",
      "441",
      "446",
      "447",
      "450",
      "451",
      "453",
      "454",
      "456",
      "457",
      "458",
      "459",
      "460",
      "462",
      "463",
      "464",
      "465",
      "467",
      "895",
      "898",
      "899",
      "909",
      "912",
      "914",
      "915",
      "916",
      "917",
      "919",
      "920",
      "922",
      "923",
      "928",
      "929",
      "930",
      "931",
      "932",
      "933",
      "934",
      "935",
      "937",
      "938",
      "939",
      "940",
      "942",
      "947",
      "948",
      "949",
      "950",
      "951",
      "952",
      "953",
      "955",
      "956",
      "957",
      "1005",
      "1116",
      "1159",
      "1160",
      "1166",
      "2623",
      "2720",
      "2721",
      "2722",
      "2723",
      "2724",
      "2725",
      "2726",
      "2727",
      "2733",
      "2734",
      "2735",
      "2736",
      "2737",
      "3023",
      "3024",
      "3025",
      "3026",
      "3027",
      "3028",
      "3029",
      "3030",
      "3031",
      "3032",
      "3033",
      "3034",
      "3035",
      "3036",
      "3037",
      "3038",
      "3039",
      "3040",
      "3041",
      "3084",
      "3085",
      "3302",
      "3303",
      "3308",
      "3309",
      "3310",
      "3388",
      "3389",
      "3418",
      "3420",
      "3421",
      "3422",
      "3423",
      "3470",
      "3472",
      "3473",
      "3564",
      "3565",
      "3566",
      "3567",
      "3568",
      "3569",
      "3570",
      "3571",
      "3572",
      "3573",
      "3574",
      "3575",
      "3576",
      "3577",
      "3578",
      "3579",
      "3580",
      "3581",
      "3582",
      "3583",
      "3584",
      "3585",
      "3586",
      "3587",
      "3588",
      "3589",
      "3590",
      "3591",
      "3592",
      "3593",
      "3594",
      "3595",
      "3596",
      "3597",
      "3598",
      "3599",
      "3600",
      "3779",
      "3782",
      "3914",
      "3915"
    )

    val interest = List(
      "26",
      "32",
      "36",
      "59",
      "61",
      "82",
      "85",
      "92",
      "104",
      "118",
      "129",
      "131",
      "141",
      "144",
      "145",
      "147",
      "149",
      "150",
      "152",
      "154",
      "155",
      "158",
      "160",
      "165",
      "166",
      "177",
      "178",
      "210",
      "213",
      "218",
      "224",
      "225",
      "226",
      "230",
      "245",
      "247",
      "250",
      "264",
      "265",
      "270",
      "275",
      "276",
      "302",
      "305",
      "311",
      "313",
      "314",
      "315",
      "316",
      "317",
      "318",
      "322",
      "323",
      "325",
      "326",
      "2635",
      "2636",
      "2660",
      "2719",
      "2743",
      "3010",
      "3011",
      "3012",
      "3013",
      "3014",
      "3015",
      "3016",
      "3017",
      "3018",
      "3019",
      "3020",
      "3021",
      "3022",
      "3055",
      "3076",
      "3077",
      "3086",
      "3087",
      "3913",
      "4097"
    )

    val generation = "5025,104030,104259,104608".split(",").toList
    val devices = "560,561,562,569,570,571,572,573,574".split(",").toList

    val taxo_segments = age_segments ::: gender_segments ::: in_market ::: interest ::: generation ::: devices

    // Get total per segments and id partner
    df_chkpt
      .withColumn("segments", explode(col("segments")))
      .groupBy("id_partner", "periodo", "segments")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices_x_seg")
      )
      .withColumn("day", lit(today))
      .write
      .format("parquet")
      .partitionBy("day", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/data_segments_campaign/")

    // Totals per segment, id and campaign id
    df_chkpt
      .withColumnRenamed("third_party", "segments")
      .filter(col("segments").isin(taxo_segments: _*))
      .groupBy("id_partner", "ID", "segments")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
        sum(col("data_type_imp")).as("impressions"),
        sum(col("data_type_clk")).as("clicks"),
        sum(col("data_type_cnv")).as("conversions")
      )
      .withColumn("day", lit(today))
      .write
      .format("parquet")
      .partitionBy("day", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/data_segments/")
  }

  def aggregateUserAgent(df_chkpt: DataFrame, today: String) = {
    df_chkpt
      .groupBy("id_partner", "ID", "brand")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids")
      )
      .withColumn("day", lit(today))
      .write
      .format("parquet")
      .partitionBy("day", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/data_brand/")
  }

  def aggregateHour(df_chkpt: DataFrame, today: String) = {
    df_chkpt
      .withColumn("hour", date_format(col("time"), "HH"))
      .withColumn("hour", col("hour").cast(IntegerType))
      .groupBy("id_partner", "ID", "hour")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
        sum(col("data_type_imp")).as("impressions"),
        sum(col("data_type_clk")).as("clicks"),
        sum(col("data_type_cnv")).as("conversions")
      )
      .withColumn(
        "moment_day",
        when(col("hour") <= 12 & col("hour") >= 7, "Morning").otherwise(
          when(col("hour") <= 18 & col("hour") >= 13, "Afternoon")
            .otherwise(
              when(col("hour") <= 24 & col("hour") >= 19, "Evening")
                .otherwise("Night")
            )
        )
      )
      .withColumn("day", lit(today))
      .write
      .format("parquet")
      .partitionBy("day", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/data_horario/")
  }

  def aggregateDay(df_chkpt: DataFrame, today: String) = {
    df_chkpt
      .filter("periodo = 'Last 30 day'")
      .withColumn("dom", date_format(col("time"), "dd"))
      .withColumn("dom", col("dom").cast(IntegerType))
      .groupBy("id_partner", "campaign_id", "dom")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
        sum(col("data_type_imp")).as("impressions"),
        sum(col("data_type_clk")).as("clicks"),
        sum(col("data_type_cnv")).as("conversions")
      )
      .withColumn("day", lit(today))
      .write
      .format("parquet")
      .partitionBy("day", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/data_horario/")
  }

  def get_aggregated_data(df_chkpt: DataFrame, today: String) {
    aggregateKPIs(df_chkpt, today)
    aggregateSegments(df_chkpt, today)
    aggregateUserAgent(df_chkpt, today)
    aggregateHour(df_chkpt, today)
    aggregateDay(df_chkpt, today)

    // val devices_campaign = df_chkpt.groupBy("campaign_id")
    //                                 .agg(approx_count_distinct(col("device_id")).as("tot_devices_x_camp"),
    //                                       first("ID").as("ID"))
    //                                 .select("ID","tot_devices_x_camp")
    // TODO: Sacar del anterior

    // // Data Agregada GEO
    // df_chkpt.groupBy("campaign_id","estate")
    //           .agg(approx_count_distinct(col("device_id")).as("devices"),
    //                 approx_count_distinct(col("nid_sh2")).as("nids"),
    //                 first("ID").as("ID"),
    //                 first("campaign_name").as("campaign_name"),
    //                 first("periodo").as("periodo"))
    //           .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
    //           .write
    //           .format("parquet")
    //           .partitionBy("day","id_partner")
    //           .mode("append")
    //           .save("/datascience/data_insights/aggregated/data_geo/")
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Data Insights Process")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    /// Parseo de parametros
    val format = "yyyy-MM-dd"
    val actual_day = DateTime.now.toString(format)
    val since = if (args.length > 0) args(0).toInt else 0
    val ndays = if (args.length > 1) args(1).toInt else 30

    format = "yyyyMMdd"
    val today = DateTime.now.minusDays(since).toString(format)
    val df_chkpt = getRawData(spark, ndays, since)
    get_aggregated_data(spark, today)
  }
}
