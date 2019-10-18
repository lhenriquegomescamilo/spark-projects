package main.scala.monthly
import org.apache.hadoop.fs.{ FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}

object GenerateGTJson{

  def generate_json_genre_ar(spark:SparkSession, conf:Configuration){

    var fs = FileSystem.get(conf)
    var os = fs.create(new Path("/datascience/devicer/priority/AR_genero_%s.json".format(current_month)))

    var content = """{"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20125))", "ndays": 3, "queue": "datascience", "pipeline": 0, "segmentId": 2, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20126))", "ndays": 3, "queue": "datascience", "pipeline": 0, "segmentId": 3, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()

  }


  def generate_json_age_ar(spark:SparkSession, conf:Configuration){

    var fs = FileSystem.get(conf)
    var os = fs.create(new Path("/datascience/devicer/priority/AR_edad_%s.json".format(current_month)))

    content = """{"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('xd', 'retroactive') and array_contains(segments, 20115))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 8, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20116))", "ndays": 3, "queue": "datascience", "pipeline": 0, "segmentId": 9, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20114))", "ndays": 3, "queue": "datascience", "pipeline": 0, "segmentId": 7, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20112))", "ndays": 3, "queue": "datascience", "pipeline": 0, "segmentId": 5, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20113))", "ndays": 3, "queue": "datascience", "pipeline": 0, "segmentId": 6, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'AR' and event_type IN ('retroactive') and array_contains(segments, 20111))", "ndays": 3, "queue": "datascience", "pipeline": 0, "segmentId": 4, "since": 1, "priority": 14, "common": "country = 'AR'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()

  }


  def generate_json_genre_mx(spark:SparkSession, conf:Configuration, current_month: String){

    val fs = FileSystem.get(conf)
    val os = fs.create(new Path("/datascience/devicer/priority/MX_genero_%s.json".format(current_month)))

    content = """{"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 69228))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 2, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 69207))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 3, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()

  }


  def generate_json_age_mx(spark:SparkSession, conf:Configuration, current_month: String){

    val fs = FileSystem.get(conf)
    val os = fs.create(new Path("/datascience/devicer/priority/MX_edad_%s.json".format(current_month)))

    content = """{"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106399))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 8, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106399))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 9, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106399))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 7, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106395))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 5, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106397))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 6, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}
    {"xd": 0, "partnerId": "", "query": "(country = 'MX' and array_contains(all_segments, 106394))", "ndays": 30, "queue": "datascience", "pipeline": 0, "segmentId": 4, "since": 1, "priority": 14, "common": "country = 'MX'", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
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
    val current_month = DateTime.now().getMonthOfYear.toString

    generate_json_age_ar(spark, conf, current_month)

    generate_json_genre_ar(spark, conf, current_month)

    generate_json_genre_mx(spark, conf, current_month)

    generate_json_age_mx(spark, conf, current_month)

  }
}
