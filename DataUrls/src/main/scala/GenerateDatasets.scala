package main.scala
import main.scala.datasets.{DatasetKeywordContent, DatasetReferer, DatasetTimestamp, DatasetUserAgent, DatasetSegmentsBranded, UrlUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.types.{
 StructType,
 StructField,
 StringType,
 IntegerType
}

object GenerateDatasetsUrls {

  def generate_expansion_datasets(spark:SparkSession,ndays:Int,since:Int,country:String,data_urls: DataFrame,ndays_dataset:Int){
    
    // First we get the untagged urls
    val untagged_df = UrlUtils.get_data_untagged(spark,ndays,since,country)
    untagged_df.cache()

    // Then we download each dataset making an inner join with the untagged urls
    val data_keywords_content = DatasetKeywordContent.get_url_content(spark,
                                                      country = country,
                                                      since = since,
                                                      ndays = ndays_dataset,
                                                      gtDF = untagged_df,
                                                      joinType = "inner",
                                                      name = "dataset_keyword_content_expansion")


    val data_timestamp = DatasetTimestamp.get_url_timestamp(spark,
                                                      country = country,
                                                      since = since,
                                                      ndays = ndays_dataset,
                                                      gtDF = untagged_df,
                                                      joinType = "inner",
                                                      df_urls = data_urls,
                                                      name = "dataset_timestamp_expansion")

    val data_user_agent = DatasetUserAgent.get_url_user_agent(spark,
                                                    ndays_dataset,
                                                    since,
                                                    country,
                                                    untagged_df,
                                                    "inner",
                                                    name = "dataset_user_agent_expansion")

    val data_segments_branded = DatasetSegmentsBranded.get_segment_branded(spark,
                                                    ndays_dataset,
                                                    since,
                                                    country,
                                                    untagged_df,
                                                    "inner",
                                                    name = "dataset_segments_branded_expansion")
  }

  def generate_training_datasets(spark:SparkSession,ndays:Int,since:Int,country:String,data_urls: DataFrame,ndays_dataset:Int){
    // Defining segments for GT
    val segments = List(26,   32,   36,   59,   61,   82,   85,   92,  104,  118,  129,
                        131,  141,  144,  145,  147,  149,  150,  152,  154,  155,  158,
                        160,  165,  166,  177,  178,  210,  213,  218,  224,  225,  226,
                        230,  245,  247,  250,  264,  265,  270,  275,  276,  302,  305,
                        311,  313,  314,  315,  316,  317,  318,  322,  323,  325,  326,
                        2635, 2636, 2660, 2719, 2743, 3010, 3011, 3012, 3013, 3014, 3015,
                        3016, 3017, 3018, 3019, 3020, 3021, 3022, 3055, 3076, 3077, 3086,
                        3087, 3913, 4097)     

    // Filtering data from url to get GT segments from ndays
    // val format = "yyyyMMdd"
    // val start = DateTime.now.minusDays(ndays).toString(format).toInt

    // var gtDF = data_urls.withColumn("date",date_format(col("time"), format))
    //                     .withColumn("date",col("date").cast(IntegerType))
    //                     .filter("date > %s".format(start))
    //                     .select("url", "segments")
    //                     .withColumn("segments", explode(col("segments")))
    //                     .filter(
    //                       col("segments")
    //                         .isin(segments: _*)
    //                     ).distinct()
    
    // gtDF.cache()

    // // Saving GT dataframe grouped by url and list of segments
    // gtDF.groupBy("url")
    //     .agg(collect_list(col("segments")).as("segments"))
    //     .withColumn("segments", concat_ws(";", col("segments")))
    //     .withColumn("country",lit(country))
    //     .write
    //     .format("parquet")
    //     .mode(SaveMode.Overwrite)
    //     .partitionBy("country")
    //     .save("/datascience/data_url_classifier/gt")

    // var gtDF = UrlUtils.get_gt_new_taxo(spark, ndays = ndays, since = since, country = country)

    // gtDF.write
    //      .format("parquet")
    //      .mode(SaveMode.Overwrite)
    //      .save("/datascience/data_url_classifier/gt_new_taxo_filtered")

    gtDF = spark.read
                .load("/datascience/data_url_classifier/gt_new_taxo_filtered")
     gtDF.cache()
     gtDF = broadcast(gtDF.select("url"))

     var data_keywords_content = DatasetKeywordContent.get_url_content(spark,
                                                         country = country,
                                                         since = since,
                                                         ndays = ndays_dataset,
                                                        gtDF = gtDF,
                                                        joinType = "inner",
                                                        name = "dataset_keyword_content_new_taxo_training")

    var data_timestamp = DatasetTimestamp.get_url_timestamp(spark,
                                                        country = country,
                                                        since = since,
                                                        ndays = ndays_dataset,
                                                        gtDF = gtDF,
                                                        joinType = "inner",
                                                        df_urls = data_urls,
                                                        name = "dataset_timestamp_new_taxo_training")

    var data_user_agent = DatasetUserAgent.get_url_user_agent(spark,
                                                      ndays_dataset,
                                                      since,
                                                      country,
                                                      gtDF,
                                                      "inner",
                                                      name = "dataset_user_agent_new_taxo_training")

    var data_segments_branded = DatasetSegmentsBranded.get_segment_branded(spark,
                                                      ndays_dataset,
                                                      since,
                                                      country,
                                                      gtDF,
                                                      "inner",
                                                      name = "dataset_segments_branded_new_taxo_training")

  }


type OptionMap = Map[Symbol, String]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--ndays" :: value :: tail =>
        nextOption(map ++ Map('ndays -> value), tail)
      case "--since" :: value :: tail =>
        nextOption(map ++ Map('since -> value), tail)
      case "--country" :: value :: tail =>
        nextOption(map ++ Map('country -> value), tail)
      case "--train" :: value :: tail =>
        nextOption(map ++ Map('train -> value), tail)
      case "--expansion" :: value :: tail =>
        nextOption(map ++ Map('expansion -> value), tail)  
      case "--ndaysDataset" :: value :: tail =>
        nextOption(map ++ Map('ndaysDataset -> value), tail)  
    }
  }

 def main(args: Array[String]) {
    // Spark configuration
    val spark = SparkSession.builder
      .appName("Data URLs: Generate All datasets")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    // Parseo de parametros
    val options = nextOption(Map(), args.toList)
    val ndays = if (options.contains('ndays)) options('ndays).toInt else 10
    val since = if (options.contains('since)) options('since).toInt else 1
    val country = if (options.contains('country)) options('country).toString else ""
    val train = if (options.contains('train)) options('train).toString else "false"
    val expansion = if (options.contains('expansion)) options('expansion).toString else "false"
    val ndays_dataset = if (options.contains('ndaysDataset)) options('ndaysDataset).toInt else 30
    
    val data_urls = UrlUtils.get_data_urls(spark, ndays_dataset, since, country)

    // Training datasets
    if (Set("1", "true", "True").contains(train)) {
      generate_training_datasets(spark,ndays,since,country,data_urls,ndays_dataset)
    }

    // Expansion datasets
    if (Set("1", "true", "True").contains(expansion)){
      generate_expansion_datasets(spark,ndays,since,country,data_urls,ndays_dataset)
    }
  }
}
