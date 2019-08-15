package main.scala.aggregators

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime

object Aggregations {

//This function is used with POIs
  def userAggregate(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we load the file with all the Geo information obtained after the join.
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/geo/%s".format(value_dictionary("poi_output_file")))

    // This function takes two lists: timestamps and distances. It checks that the user has been in a POI a number of minutes within the range:
    //                  umbralmin < n_minutes_in_poi < umbralmax
    // Also it checks that the corresponding distances is at most umbraldist (in meters).
    val hasUsedPoi = udf(
      (timestamps: Seq[String], stopids: Seq[String]) =>
        ((timestamps.slice(1, timestamps.length) zip timestamps).map(
          t =>
            (t._1.toInt - t._2.toInt < (value_dictionary("umbralmax").toInt * 60)) & ((value_dictionary(
              "umbralmin"
            ).toInt * 60) < t._1.toInt - t._2.toInt)
        ) zip (stopids.slice(1, stopids.length) zip stopids).map(
          s =>
            ((s._1.toFloat < (value_dictionary("umbraldist").toInt)) | (s._2.toFloat < (value_dictionary(
              "umbraldist"
            ).toInt)))
        )).exists(b => b._1 && b._2)
    )

    // Path where we will store the results.
    val output_path_anlytics = "/datascience/geo/geo_processed/%s_aggregated"
      .format(value_dictionary("poi_output_file"))

    // Here we do the aggregation
    data
      .withColumn("distance", col("distance").cast("double"))
      .groupBy(value_dictionary("poi_column_name"), "device_id", "device_type")
      // We obtain the list of timestamps and distances, along with the minimum distance
      .agg(
        collect_list(col("timestamp")).as("timestamp_list"),
        collect_list(col("distance")).as("distance_list"),
        min(col("distance")).as("min_distance")
      )
      // Now we obtain the frequency
      .withColumn("frequency", size(col("timestamp_list")))
      // Here we calculate if the user is valid based on the thresholds
      .withColumn(
        "validUser",
        hasUsedPoi(col("timestamp_list"), col("distance_list"))
      )
      // Here we transform the lists into strings
      .withColumn("timestamp_list", concat_ws(",", col("timestamp_list")))
      .withColumn("distance_list", concat_ws(",", col("distance_list")))
      // Finally we write the results
      .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path_anlytics)

  }
//this function sis used to create the map with POIs


  def POIAggregate(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we load the data.
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/geo/geo_processed/%s_aggregated"
          .format(value_dictionary("poi_output_file"))
      )

    // Here we perform the aggregation
    data
      // First we keep the users that are valid in a new column
      .withColumn(
        "device_id_valid",
        when(col("validUser") === "true", col("device_id")).otherwise(null)
      )
      // Now we group by the POI id
      .groupBy(value_dictionary("poi_column_name"))
      // Here we do all the aggregations
      .agg(
        countDistinct("device_id_valid").as("unique_true_users"),
        count("device_id_valid").as("true_users_visits"),
        countDistinct("device_id").as("unique_paserby"),
        count("device_id").as("total_detections")
      )
      // Finally we store the results
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/geo/map_data/%s_map"
          .format(value_dictionary("poi_output_file"))
      )
}


//This function is used to create aggregations from polygons
def userAggregateFromPolygon(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we load the file with all the Geo information obtained after the join.
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/geo/%s".format(value_dictionary("poi_output_file")))

    // This function takes two lists: timestamps and distances. It checks that the user has been in a POI a number of minutes within the range:
    //                  umbralmin < n_minutes_in_poi < umbralmax
    // Also it checks that the corresponding distances is at most umbraldist (in meters).
    val stayedThere = udf( (timestamps: Seq[String]) => 
            (timestamps.slice(1, timestamps.length) zip timestamps).map(
                    t =>
                ( t._1.toInt-t._2.toInt < value_dictionary("umbralmax").toInt * 60) 
                                  & (
                  t._1.toInt-t._2.toInt > value_dictionary("umbralmin").toInt * 60))
                    .exists(b=>b) )
    
    // Path where we will store the results.
    val output_path_anlytics = "/datascience/geo/geo_processed/%s_aggregated"
      .format(value_dictionary("poi_output_file"))

    // Here we do the aggregation
    data
      .groupBy(value_dictionary("poi_column_name"), "device_id", "device_type")
      // We obtain the list of timestamps and distances, along with the minimum distance
      .agg(
        collect_list(col("utc_timestamp")).as("timestamp_list"))
      // Now we obtain the frequency
      .withColumn("frequency", size(col("timestamp_list")))
      // Here we calculate if the user is valid based on the thresholds
      .withColumn(
        "validUser",
        stayedThere(col("timestamp_list"))
      )
      // Here we transform the lists into strings
      .withColumn("timestamp_list", concat_ws(",", col("timestamp_list")))
      
      // Finally we write the results
      .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path_anlytics)

  }

//this function sis used to create the map with Polygons


  def PolygonAggregate(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we load the data.
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/geo/geo_processed/%s_aggregated"
          .format(value_dictionary("poi_output_file"))
      )

    // Here we perform the aggregation
    data
      // First we keep the users that are valid in a new column
      .withColumn(
        "device_id_valid",
        when(col("validUser") === "true", col("device_id")).otherwise(null)
      )
      // Now we group by the POI id
      .groupBy(value_dictionary("poi_column_name"))
      // Here we do all the aggregations
      .agg(
        countDistinct("device_id_valid").as("unique_true_users"),
        count("device_id_valid").as("true_users_visits"),
        countDistinct("device_id").as("unique_paserby"),
        count("device_id").as("total_detections")
      )
      // Finally we store the results
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/geo/map_data/%s_map"
          .format(value_dictionary("poi_output_file"))
      )
}


//This are the functions to get the segments for the users
 def getDataPipeline(
      spark: SparkSession,
      path: String,
      value_dictionary: Map[String, String]) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    //specifying country
    val country_iso =
      if (value_dictionary("country") == "argentina")
           "AR"
      else "MX"

    val nDays = value_dictionary("web_days")
    val since = value_dictionary("since")

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country_iso))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }



  //add segments
    def get_segments  (
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {

        val audiences = getDataPipeline(spark,"/datascience/data_audiences",value_dictionary)
        val user_segments = audiences.select("device_id","all_segments","timestamp")

        //Nos quedamos únicamente con la versión del usuario de mayor timestamp
        val w = Window.partitionBy(col("device_id")).orderBy(col("timestamp").desc)
        val dfTop = user_segments.withColumn("rn", row_number.over(w)).where(col("rn") === 1).drop("rn")

        val segments = dfTop.select("device_id","all_segments")

        //hay que elegir una opción para la agregación de los segmentos
        //if(web_agreggator = "audience"
        //if  value_dictionary("poi_column_name")
        //    value_dictionary("audience_column_name")

        val data = spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load(
        "/datascience/audiences/crossdeviced/%s_xd"
          .format(value_dictionary("poi_output_file"))
        ).filter("device_type == 'web'")


        val joint = data.select("device_id",value_dictionary("audience_column_name"))
                              .join(segments, Seq("device_id"))
                              .withColumn("all_segments", explode(col("all_segments")))
                              .withColumn(value_dictionary("poi_column_name"), explode(split(col(value_dictionary("poi_column_name")),",")))
                              .groupBy(value_dictionary("poi_column_name"), "all_segments")
                              .agg(count(col("device_id")) as "unique_count")
                              //.agg(countDistinct(col("device_id")) as "unique_count" )
        

      val output_path_segments = "/datascience/geo/geo_processed/%s_w_segments"
                                                            .format(value_dictionary("poi_output_file"))

       joint.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_segments)
             
             /*                 
        val total_by_poi = data.groupBy(value_dictionary("poi_column_name"))
                            .agg(countDistinct(col("device_id")) as "total_count_poi" )
                            
        val total_by_segment = segments.groupBy("all_segments")
                              .agg(countDistinct(col("device_id")) as "total_count_sgement" )                      

        

                                                            total_by_segment.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_segments + "_total_by_segment")            


        total_by_poi.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_segments + "_total_by_poi") 
                    */



       
//add segments
    def get_segments_from_triplets (
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {

        val segments = getDataPipeline(spark,"/datascience/data_triplets/segments/",value_dictionary)
                        .drop(col("count"))
                        .dropDuplicates()

        val data = spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load(
        "/datascience/audiences/crossdeviced/%s_xd"
          .format(value_dictionary("poi_output_file"))
        ).filter("device_type == 'web'")


        val joint = data.select("device_id",value_dictionary("audience_column_name"))
                              .join(segments, Seq("device_id"))
                              .withColumn(value_dictionary("poi_column_name"), explode(split(col(value_dictionary("poi_column_name")),",")))
                              .groupBy(value_dictionary("poi_column_name"), "all_segments")
                              .agg(count(col("device_id")) as "unique_count")
                              //.agg(countDistinct(col("device_id")) as "unique_count" )
        

      val output_path_segments = "/datascience/geo/geo_processed/%s_w_segments"
                                                            .format(value_dictionary("poi_output_file"))

       joint.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_segments)
             
         

                      
  }
                      
  }

  //add segments
    def get_segments_from_triplets_old  (
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {

        val country_iso =
        if (value_dictionary("country") == "argentina")
           "AR"
        else "MX"

        val segments = spark.read   
                      .option("delimiter","\t")      
                      .parquet("/datascience/data_demo/triplets_segments/country=%s/".format(country_iso))
                      .withColumnRenamed("feature","all_segments")
                      .drop(col("count"))

        val data = spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load(
        "/datascience/audiences/crossdeviced/%s_xd"
          .format(value_dictionary("poi_output_file"))
        ).filter("device_type == 'web'")


        val joint = data.select("device_id",value_dictionary("audience_column_name"))
                              .join(segments, Seq("device_id"))
                              .withColumn(value_dictionary("poi_column_name"), explode(split(col(value_dictionary("poi_column_name")),",")))
                              .groupBy(value_dictionary("poi_column_name"), "all_segments")
                              .agg(count(col("device_id")) as "unique_count")
                              //.agg(countDistinct(col("device_id")) as "unique_count" )
        

      val output_path_segments = "/datascience/geo/geo_processed/%s_w_segments"
                                                            .format(value_dictionary("poi_output_file"))

       joint.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_segments)
             
         

                      
  }

}