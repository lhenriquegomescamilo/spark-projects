package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{
  upper,
  count,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit,
  explode,
  when,
  datediff,
  ceil,
  first,
  countDistinct,
  concat,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  approx_count_distinct,
  sum
}
import org.apache.spark.sql.types.{IntegerType}

object AggregateData {

  def get_aggregated_data(spark:SparkSession, ndays:Int, since:Int){

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    var format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_insights/raw"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    format = "yyyy-MM-dd"
    val today = end.toString(format)
    val magic_ratio = 6.798

    val udfClk = udf((keyword: String) => if (keyword == "clk") 1 else 0)
    val udfImp = udf((keyword: String) => if (keyword == "imp") 1 else 0)
    val udfCnv = udf((keyword: String) => if (keyword == "cnv") 1 else 0)
    val udfCTR = udf((impressions: String, click: String) => if (impressions.toDouble > 0) (click.toDouble)/(impressions.toDouble) else 0)
    val udfCnvRate = udf((convertions: String, click: String) => if (convertions.toDouble > 0) (click.toDouble)/(convertions.toDouble) else 0)

    df.withColumn("day",lit(today))
      .withColumn("datediff",datediff(col("day"),col("time")))
      .withColumn("periodo",when(col("datediff") <= 1, "Last 1 day").otherwise(when(col("datediff") <= 7, "Last 7 days").otherwise("Last 30 days")))
//    TODO: Agregar un vector que tenga todas las inclusiones y despues explotar por ese vector
      .withColumn("ID",concat(col("periodo"),lit("-"),col("campaign_id")))
      .write
      .format("parquet")
      .partitionBy("day","id_partner")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_insights/aggregated/data_chkpt")

    val df_chkpt = spark.read.load("/datascience/data_insights/aggregated/data_chkpt")

    // Data Agregada KPIS
    df_chkpt.withColumn("data_type_clk",udfClk(col("data_type")))
            .withColumn("data_type_imp",udfImp(col("data_type")))
            .withColumn("data_type_cnv",udfCnv(col("data_type")))
            .groupBy("campaign_id")
            .agg(count(col("id_partner")).as("hits"),
                  approx_count_distinct(col("device_id"),0.03).as("devices"),
                  approx_count_distinct(col("nid_sh2"),0.03).as("nids"),
                  sum(col("data_type_imp")).as("impressions"),
                  sum(col("data_type_clk")).as("clicks"),
                  sum(col("data_type_cnv")).as("convertions"),
                  first("ID").as("ID"),
                  first("day").as("day"),
                  first("id_partner").as("id_partner"),
                  first("periodo").as("periodo"))
            .withColumn("ctr",udfCTR(col("impressions"),col("clicks")))
            .withColumn("cnvrate",udfCnvRate(col("convertions"),col("clicks")))
            .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
            .write
            .format("parquet")
            .partitionBy("day","id_partner")
            .mode("append")
            .save("/datascience/data_insights/aggregated/data_kpis/")

    // Data Agregada Segments
    val age_segments = List("4", "5", "6", "7", "8", "9")

    val gender_segments = List("2", "3")

    val in_market = List("352", "353", "354",  "356",  "357",  "358",  "359",  "363",  "366",  "367",  "374",  "377",
                          "378",  "379",  "380",  "384",  "385",  "386",  "389",  "395",  "396",  "397",  "398",  "399",  "401",  "402",  "403",
                          "404",  "405",  "409",  "410",  "411",  "412",  "413",  "418",  "420",  "421",  "422",  "429",  "430",  "432",  "433",
                          "434",  "440",  "441",  "446",  "447",  "450",  "451",  "453",  "454",  "456",  "457",  "458",  "459",  "460",  "462",
                          "463",  "464",  "465",  "467",  "895",  "898",  "899",  "909",  "912",  "914",  "915",  "916",  "917",  "919",  "920",
                          "922",  "923",  "928",  "929",  "930",  "931",  "932",  "933",  "934",  "935",  "937",  "938",  "939",  "940",  "942",
                          "947",  "948",  "949",  "950",  "951",  "952",  "953",  "955",  "956",  "957", "1005", "1116", "1159", "1160", "1166",
                          "2623", "2720", "2721", "2722", "2723", "2724", "2725", "2726", "2727", "2733", "2734", "2735", "2736", "2737", "3023",
                          "3024", "3025", "3026", "3027", "3028", "3029", "3030", "3031", "3032", "3033", "3034", "3035", "3036", "3037", "3038",
                          "3039", "3040", "3041", "3084", "3085", "3302", "3303", "3308", "3309", "3310", "3388", "3389", "3418", "3420", "3421",
                          "3422", "3423", "3470", "3472", "3473", "3564", "3565", "3566", "3567", "3568", "3569", "3570", "3571", "3572", "3573",
                          "3574", "3575", "3576", "3577", "3578", "3579", "3580", "3581", "3582", "3583", "3584", "3585", "3586", "3587", "3588",
                          "3589", "3590", "3591", "3592", "3593", "3594", "3595", "3596", "3597", "3598", "3599", "3600", "3779", "3782", "3914",
                          "3915")

    val interest = List("26","32","36","59","61","82","85","92","104","118","129","131","141","144","145",
                          "147","149","150","152","154","155","158","160","165","166","177","178","210","213",
                          "218","224","225","226","230","245","247","250","264","265","270","275","276","302",
                          "305","311","313","314","315","316","317","318","322","323","325","326","2635","2636",
                          "2660","2719","2743","3010","3011","3012","3013","3014","3015","3016","3017","3018","3019",
                          "3020","3021","3022","3055","3076","3077","3086","3087","3913","4097")

    val taxo_segments = age_segments ::: gender_segments ::: in_market ::: interest

    // val devices_campaign = df_chkpt.groupBy("campaign_id")
    //                                 .agg(approx_count_distinct(col("device_id")).as("tot_devices_x_camp"),
    //                                       first("ID").as("ID"))
    //                                 .select("ID","tot_devices_x_camp")
                                    // TODO: Sacar del anterior

    val segments_campaign = df_chkpt.groupBy("segments","id_partner")
                                    .agg(approx_count_distinct(col("device_id")).as("devices_x_seg"),
                                          first("periodo").as("periodo"),
                                          first("day").as("day"))
                                    .write
                                    .format("parquet")
                                    .partitionBy("day","id_partner")
                                    .mode("append")
                                    .save("/datascience/data_insights/aggregated/data_segments_campaign/")
                                    
    //val total_base = df_chkpt.select("device_id").distinct.count
    
    df_chkpt.filter(col("segments").isin(taxo_segments: _*))
            .groupBy("id_partner","campaign_id","segments")
            .agg(approx_count_distinct(col("device_id"),0.03).as("devices"),
                  approx_count_distinct(col("nid_sh2"),0.03).as("nids"),
                  first("ID").as("ID"),
                  first("campaign_name").as("campaign_name"),
                  first("day").as("day"),
                  first("periodo").as("periodo"))
            .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
            // .join(devices_campaign,Seq("ID"),"left")
            // .withColumn("porc_devices_tot_dev_x_camp",col("devices")/col("tot_devices_x_camp"))
            // .join(segments_campaign,Seq("segments","periodo"),"left")
            // .withColumn("total_base",lit(total_base))
            // .withColumn("porc_seg_tot",col("devices_x_seg")/col("total_base"))
            .write
            .format("parquet")
            .partitionBy("day","id_partner")
            .mode("append")
            .save("/datascience/data_insights/aggregated/data_segments/")

    // Data Agregada Device type
    df_chkpt.groupBy("id_partner","campaign_id","device_type")
            .agg(approx_count_distinct(col("device_id"),0.03).as("devices"),
                  approx_count_distinct(col("nid_sh2"),0.03).as("nids"),
                  first("ID").as("ID"),
                  first("campaign_name").as("campaign_name"),
                  first("day").as("day"),
                  first("periodo").as("periodo"))
            .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
            .write
            .format("parquet")
            .partitionBy("day","id_partner")
            .mode("append")
            .save("/datascience/data_insights/aggregated/data_device_type/")
    
    // Data Agregada Brand
    df_chkpt.groupBy("id_partner","campaign_id","brand")
            .agg(approx_count_distinct(col("device_id"),0.03).as("devices"),
                  approx_count_distinct(col("nid_sh2"),0.03).as("nids"),
                  first("ID").as("ID"),
                  first("campaign_name").as("campaign_name"),
                  first("day").as("day"),
                  first("periodo").as("periodo"))
            .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
            .write
            .format("parquet")
            .partitionBy("day","id_partner")
            .mode("append")
            .save("/datascience/data_insights/aggregated/data_brand/")
  


    // Data Agregada Horario
    val udfMoment = udf((hour: Int) =>if (7 <= hour & hour <= 12) "Morning"
                                        else if (13 <= hour & hour <= 18) "Afternoon"
                                            else if (19 <= hour & hour <= 24) "Evening"
                                                else "Night")

    // TODO: Cambiar a entero y hacerlo con mayor y menor
    
     df_chkpt.withColumn("hour", date_format(col("time"), "HH"))
              .withColumn("moment_day",udfMoment(col("hour").cast(IntegerType)))
              .groupBy("id_partner","campaign_id","hour")
              .agg(approx_count_distinct(col("device_id"),0.03).as("devices"),
                    approx_count_distinct(col("nid_sh2"),0.03).as("nids"),
                    first("ID").as("ID"),
                    first("campaign_name").as("campaign_name"),
                    first("day").as("day"),
                    first("moment_day").as("moment_day"),
                    first("periodo").as("periodo"))
              .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
              .write
              .format("parquet")
              .partitionBy("day","id_partner")
              .mode("append")
              .save("/datascience/data_insights/aggregated/data_horario/")

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
    val spark = SparkSession.builder.appName("Data Insights Process")
                                    .config("spark.sql.files.ignoreCorruptFiles", "true")
                                    .config("spark.sql.sources.partitionOverwriteMode","dynamic")
                                    .getOrCreate()

    /// Parseo de parametros
    val format = "yyyy-MM-dd"
    val actual_day = DateTime.now.toString(format)
    val since = if (args.length > 0) args(0).toInt else 0
    val ndays = if (args.length > 1) args(1).toInt else 30

    get_aggregated_data(spark,ndays,since)

  }
}
