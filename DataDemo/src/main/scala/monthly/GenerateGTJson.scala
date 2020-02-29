package main.scala.monthly
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}

object GenerateGTJson {

  def generate_json_genre_ar(
      spark: SparkSession,
      conf: Configuration,
      current_month: String
  ) {

    var fs = FileSystem.get(conf)
    var os = fs.create(
      new Path(
        "/datascience/devicer/to_process/AR_genero_%s.json"
          .format(current_month)
      )
    )

    val content =
      """{"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20125))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 2, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20126))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 3, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()

  }

  def generate_json_age_ar(
      spark: SparkSession,
      conf: Configuration,
      current_month: String
  ) {

    var fs = FileSystem.get(conf)
    var os = fs.create(
      new Path(
        "/datascience/devicer/to_process/AR_edad_%s.json".format(current_month)
      )
    )

    val content =
      """{"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('xd', 'retroactive') and array_contains(segments, 20115))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 8, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20116))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 9, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20114))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 7, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20112))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 5, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20113))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 6, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20111))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 4, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()

  }

  def generate_json_genre_mx(
      spark: SparkSession,
      conf: Configuration,
      current_month: String
  ) {

    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path(
        "/datascience/devicer/to_process/MX_genero_%s.json"
          .format(current_month)
      )
    )

    val content =
      """{"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 69228))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 2, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 69207))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 3, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()

  }

  def generate_json_age_mx(
      spark: SparkSession,
      conf: Configuration,
      current_month: String
  ) {

    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path(
        "/datascience/devicer/to_process/MX_edad_%s.json".format(current_month)
      )
    )

    val content =
      """{"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106399))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 8, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106399))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 9, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106399))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 7, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106395))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 5, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106397))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 6, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106394))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 4, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()

  }

  def generate_json_gender_cl(
      spark: SparkSession,
      conf: Configuration,
      current_month: String
  ) = {
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path(
        "/datascience/devicer/to_process/CL_genero_%s.json"
          .format(current_month)
      )
    )

    val content =
      """{"xd": 0, "partnerId": "", "query": "(country = 'CL' and array_contains(all_segments, 136487))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 2, "since": 1, "priority": 14, "common": "country = 'CL'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'CL' and array_contains(all_segments, 136489))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 3, "since": 1, "priority": 14, "common": "country = 'CL'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()
  }

  def generate_json_age_cl(
      spark: SparkSession,
      conf: Configuration,
      current_month: String
  ) = {
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path(
        "/datascience/devicer/to_process/CL_age_%s.json"
          .format(current_month)
      )
    )

    val content =
      """{"xd": 0, "partnerId": "", "query": "(country = 'CL' and array_contains(all_segments, 136491))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 4, "since": 1, "priority": 14, "common": "country = 'CL'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'CL' and array_contains(all_segments, 136493))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 5, "since": 1, "priority": 14, "common": "country = 'CL'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'CL' and array_contains(all_segments, 136495))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 6, "since": 1, "priority": 14, "common": "country = 'CL'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'CL' and array_contains(all_segments, 136497))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 7, "since": 1, "priority": 14, "common": "country = 'CL'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'CL' and array_contains(all_segments, 136499))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 8, "since": 1, "priority": 14, "common": "country = 'CL'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()
  }

  def generate_json_age_br(
      spark: SparkSession,
      conf: Configuration,
      current_month: String
  ) = {
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path(
        "/datascience/devicer/to_process/BR_age_%s.json"
          .format(current_month)
      )
    )

    val content =
      """{"xd": 0, "partnerId": "", "query": "(country = 'BR' and array_contains(all_segments, 24609))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 4, "since": 1, "priority": 14, "common": "country = 'BR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'BR' and (array_contains(all_segments, 24610) or array_contains(all_segments, 24611)))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 5, "since": 1, "priority": 14, "common": "country = 'BR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'BR' and (array_contains(all_segments, 24612) or array_contains(all_segments, 24613)))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 6, "since": 1, "priority": 14, "common": "country = 'BR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'BR' and (array_contains(all_segments, 24614) or array_contains(all_segments, 24615)))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 7, "since": 1, "priority": 14, "common": "country = 'BR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'BR' and (array_contains(all_segments, 24616) or array_contains(all_segments, 24617)))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 8, "since": 1, "priority": 14, "common": "country = 'BR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'BR' and (array_contains(all_segments, 24618) or array_contains(all_segments, 24619)))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 9, "since": 1, "priority": 14, "common": "country = 'BR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()
  }

  def generate_json_gender_br(
      spark: SparkSession,
      conf: Configuration,
      current_month: String
  ) = {
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path(
        "/datascience/devicer/to_process/BR_gender_%s.json"
          .format(current_month)
      )
    )

    val content =
      """{"xd": 0, "partnerId": "", "query": "country = 'BR' and (array_contains(all_segments, 24621) or (array_contains(segments, 2) AND id_partner = 916))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 2, "since": 1, "priority": 14, "common": "country = 'BR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "country = 'BR' and (array_contains(all_segments, 24622) or (array_contains(segments, 3) AND id_partner = 916))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 3, "since": 1, "priority": 14, "common": "country = 'BR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Monthly Generation of GT Jsons")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    // Defining parameter configuration
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")

    // Get actual month to put in gt jsons name
    val current_month = DateTime.now().toString("yyyyMM")

    // AR
    generate_json_age_ar(spark, conf, current_month)
    generate_json_genre_ar(spark, conf, current_month)

    // MX
    generate_json_genre_mx(spark, conf, current_month)

    // CL
    generate_json_gender_cl(spark, conf, current_month)
    generate_json_age_cl(spark, conf, current_month)

    // BR
    generate_json_gender_br(spark, conf, current_month)
    generate_json_age_br(spark, conf, current_month)

  }
}
