package main.scala.aggregators

import main.scala.Geodevicer
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
      .load("/datascience/geo/raw_output/%s".format(value_dictionary("poi_output_file")))

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

// this function gets the map and the result of the xd and counts the devices by aggregated feature
def POIAggregate_w_xd(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {

  val map_data = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/geo/map_data/%s_map"
          .format(value_dictionary("poi_output_file"))
      )
    
  val xd_result = spark.read      
                  .format("csv") 
                  .option("header",true)
                  .option("sep", "\t")
                  .load(
        "/datascience/geo/crossdeviced/%s_xd"
          .format(value_dictionary("poi_output_file")))

  val audienceByCode = xd_result.withColumn(value_dictionary("poi_column_name"),explode(split(col(value_dictionary("poi_column_name")),",")))

  val countByCode = audienceByCode
                        .groupBy(value_dictionary("poi_column_name"))
                        .agg(countDistinct("device_id") as "unique_device")


  map_data.join(countByCode,Seq(value_dictionary("poi_column_name")))
    .write.format("csv")
    .option("header",true)
    .option("sep", "\t")
    .mode(SaveMode.Overwrite)
    .save( "/datascience/geo/map_data/%s_map_w_xd"
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
      .load("/datascience/geo/raw_output/%s".format(value_dictionary("poi_output_file")))

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
    val since = 1

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
        "/datascience/geo/crossdeviced/%s_xd"
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



       

                      
  }

  //add segments
    def get_segments_from_triplets(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {

        val segments = getDataPipeline(spark,"/datascience/data_triplets/segments/",value_dictionary)
                        .drop(col("count"))
                        

        val data = spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load(
        "/datascience/geo/crossdeviced/%s_xd"
          .format(value_dictionary("poi_output_file"))
        ).filter("device_type == 'web'")


        val joint = data.select("device_id",value_dictionary("audience_column_name"))
                              .join(segments, Seq("device_id"))
                              .withColumn(value_dictionary("poi_column_name"), explode(split(col(value_dictionary("poi_column_name")),",")))
                              .groupBy(value_dictionary("poi_column_name"), "feature")
                              .agg(countDistinct(col("device_id")) as "unique_count" )
                              //.agg(count(col("device_id")) as "unique_count")  

      val output_path_segments = "/datascience/geo/geo_processed/%s_w_segments"
                                                            .format(value_dictionary("poi_output_file"))

       joint.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_segments)


      val output_path_horrible_users = "/datascience/geo/geo_processed/%s_output_path_users_data"
                                                            .format(value_dictionary("poi_output_file"))

      val joint_users_for_analysis = data.select("device_id",value_dictionary("audience_column_name"))
                              .join(segments, Seq("device_id"))

      joint_users_for_analysis.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_horrible_users)
                
                     
  }


  //This function takes as input a number. this number is substracted from the current date
    def create_audiences_from_attribution_date(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {

      //we get the current date and substract the desired number of days
    val atribution_date = DateTime.now.minusDays(value_dictionary("atribution_date").toInt).minusDays(value_dictionary("since").toInt).getMillis()/1000
    val atribute_day_name = DateTime.now.minusDays(value_dictionary("atribution_date").toInt).minusDays(value_dictionary("since").toInt).toString("YYYYMMDD")

    //we load the user aggregated data
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/geo/geo_processed/%s_aggregated"
          .format(value_dictionary("poi_output_file"))
            )

    
    //we create this functions to detect if a user was before/after the desired date
    val wasbefore = udf( (timestamps: Seq[String]) => timestamps.map(t => (t.toInt<atribution_date)).exists(b => b) )
    val wasafter = udf( (timestamps: Seq[String]) => timestamps.map(t => (t.toInt>atribution_date)).exists(b => b) )
 
    val user_attributions = data.withColumn("timestamp_list",split(col("timestamp_list"),",")) //since the data is joined, we split it
                .withColumn("before",wasbefore(col("timestamp_list"))) 
                .withColumn("after",wasafter(col("timestamp_list")))
                .withColumn("new_user", when(col("before") ===false && col("after") === true,1).otherwise(0))
                .withColumn("churn_user", when(col("before") ===true && col("after") === false,1).otherwise(0) )
                .withColumn("fidelity_user", when(col("before") ===true && col("after") === true,1).otherwise(0))



    //val new_user = user_attributions.filter("new_user == 1").select("device_type","device_id",value_dictionary("audience_column_name"))
    //val churn_user = user_attributions.filter("churn_user == 1").select("device_type","device_id",value_dictionary("audience_column_name"))
    //val fidelity_user = user_attributions.filter("fidelity_user == 1").select("device_type","device_id",value_dictionary("audience_column_name"))


      user_attributions.select("device_type","device_id",value_dictionary("poi_column_name"),"new_user","churn_user","fidelity_user")
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/geo_processed/%s_att_date-%s"
          .format(value_dictionary("poi_output_file"),atribute_day_name))
/*
      new_user.write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/geo/audience/%s_new_users_att_date%s"
          .format(value_dictionary("poi_output_file"),atribute_day_name))

          churn_user.write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/geo/audience/%s_churn_user_att_date%s"
          .format(value_dictionary("poi_output_file"),atribute_day_name))

          fidelity_user.write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/geo/audience/%s_churn_user_att_date%s"
          .format(value_dictionary("poi_output_file"),atribute_day_name))
      
  */                   
  }

//this function create audiences from multiple stops of public transport
//the variables for the input are:
//
def user_aggregate_for_moving_transport(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {


    //we load the raw NON-aggregated data
        val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/geo/%s".format(value_dictionary("poi_output_file")))

    
    val filter_by_distance = data.filter("distance < %s".format(value_dictionary("umbraldist").toInt))
    val exploded_by_transport_id =  filter_by_distance
                    .withColumn("transport_id", explode(split(col(value_dictionary("column_w_stop_list_id")), ",")))
 
 
      //hacemos un collect de timestamps y stop id para cada usuario en cada linea. 
      val poi_line = exploded_by_transport_id.groupBy("transport_id","device_id","device_type")
            .agg(
              collect_list(col("timestamp")).as("times_array"), 
              collect_list(value_dictionary("poi_column_name")).as("location_array"), 
              collect_list("distance").as("distance_array"))
            .withColumn("frequency", size(col("times_array")))
           .filter("frequency >1")
 

      // this function checks if the user is a user of the specific transport
      // it checks if the the time between stops is below a TRESHOLD
      //it checks if the stations corresponding to the that time difference are different (i.e. the user moved)
        //it checks if the user was very close to any station multiple times
      val hasUsedTransport = udf( (timestamps: Seq[String],stopid: Seq[String], distance: Seq[String]) => 
                                (
        (timestamps.slice(1, timestamps.length) zip timestamps).map( 
          t => t._1.toInt-t._2.toInt < value_dictionary("umbralmax").toInt * 60) , //1) this checks if the time between stops is less than the threshold
        (stopid.slice(1,stopid.length) zip stopid).map(s => s._1!=s._2) , //2)this checks if the stations are different stations
        Seq((distance.filter(d=> d.toFloat < value_dictionary("transport_min_distance").toInt)).size>value_dictionary("transport_min_ocurrence").toInt) //3)this checks if the distance to the point is less than a threshold
                                  ).zipped.toList.exists(b => (b._1 & b._2)|b._3) ) 
                                                                    //this checks if 1) AND 2) are true, OR 3) happened
 
 
      //creamos una columna si nos dice si es un usuario o no usando la función. filtramos para que no esté vacía en línea y que no sea nula
        val users_aggregated_by_transport_id = poi_line
        .withColumn("validUser",hasUsedTransport(
                poi_line("times_array"),
                poi_line("location_array"),
                poi_line("distance_array")))
        .filter("validUser == true")
        .filter((col("transport_id") =!= "") && ((col("transport_id").isNotNull)))
        // Here we transform the lists into strings
      .withColumn("times_array", concat_ws(",", col("times_array")))
      .withColumn("location_array", concat_ws(",", col("location_array")))
      .withColumn("distance_array", concat_ws(",", col("distance_array")))
      .withColumn("distance_array", concat_ws(",", col("distance_array")))
      .withColumn(value_dictionary("poi_column_name"),col("transport_id")) 
                //this creates a column with the lines as names, so that the process can continue using this column
      
               // Path where we will store the results.
    val output_path_anlytics = "/datascience/geo/geo_processed/%s_aggregated"
      .format(value_dictionary("poi_output_file"))

    users_aggregated_by_transport_id
      // Finally we write the results
      .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path_anlytics)
    }
   
                     
  /***
****/
}