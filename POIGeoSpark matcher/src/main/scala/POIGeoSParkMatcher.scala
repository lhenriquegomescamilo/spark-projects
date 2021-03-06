package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

/**
  Job Summary:
  * The goal of this job is to create an audiencie based on Points Of Interests (POIs). The method takes as input a time frame (be default, december 2018) and a dataset containing the POIs. This dataset should be already formatted in three columns segment|latitude|longitude (without the index) and with the latitude and longitude with point (".") as delimiter.
  * The method filters the safegraph data by country, and creates a geocode for both the POIs and the safegraph data. This geocode is used to match both datasets by performing a SQL join. The resulting rows will contain a user id, device type type, user latitude and longitude and POI id, latitude and longitude. Then the vincenty formula is used to calculate distance between the pairs of latitude and longitude.
   The method then proceeds to filter the users by a desired minimum distance returning a final dataset with user id and device type.

   The current method will provide the basis for future more customizable geolocation jobs.
  */
object POIGeoSparkMatcher {

  /**
This method reads the safegraph data, selects the columns "ad_id" (device id), "id_type" (user id), "latitude", "longitude", creates a geocode for each row and future spatial operations and finally removes duplicates users that were detected in the same location (i.e. the same user in different lat long coordinates will be conserved, but the same user in same lat long coordinates will be dropped).

   @param spark: Spark session that will be used to load the data.
   @param nDays: parameter to define the number of days. Currently not used, hardcoded to the whole month of december 2018.
   @param country: country from with to filter the data, it is currently hardcoded to Mexico
   @return df_safegraph: dataframe created with the safegraph data, filtered by the desired country, extracting the columns user id, device id, latitude and longitude removing duplicate users that have repeated locations and with added geocode.

    */
  def get_safegraph_data(
      spark: SparkSession,
      nDays: Integer,
      country: String,
      since: Integer = 1
  ) = {
    //loading user files with geolocation, added drop duplicates to remove users who are detected in the same location
    // Here we load the data, eliminate the duplicates so that the following computations are faster, and select a subset of the columns
    // Also we generate a new column call 'geocode' that will be used for the join
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val path = "/data/geo/safegraph/"
    val hdfs_files = days.map(day => path + "%s/*.gz".format(day))
    val df_safegraph = spark.read
      .option("header", "true")
      .csv(hdfs_files: _*)
      .dropDuplicates("ad_id", "latitude", "longitude")
      .filter("country = '%s'".format(country))
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")

    df_safegraph
  }

  /**
  This method reads the user provided POI dataset, adds a geocode for each POI and renames the columns. The file provided must be correctly formatted as described below.

   @param spark: Spark session that will be used to load the data.
   @param file_name: path of the dataset containing the POIs. Must be correctly formated as described (name|latitude|longitude (without the index), with the latitude and longitude with point (".") as decimal delimiter.)
   @param return df_pois_final: dataframe created from the one provided by the user containing the POIS: contains the geocode and renamed columns.
    */
  def get_POI_coordinates(spark: SparkSession, file_name: String) = {
    // Loading POIs. The format of the file is Name, Latitude, Longitude
    val df_pois = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(file_name)

    //creating geocodes the POIs
    val df_pois_parsed =
      df_pois //.withColumn("radius", (col("radius").cast("float")))

    // Here we rename the columns
    // val columnsRenamed_poi = Seq("name", "latitude", "longitude", "radius")
    val columnsRenamed_poi = Seq("latitude", "longitude", "price_per_m2")

    //renaming columns based on list
    val df_pois_final =
      df_pois_parsed
        .toDF(columnsRenamed_poi: _*)
        .withColumn("radius", lit(0.01))

    df_pois_final
  }

  /**
  This method performs the vincenty distance calculation between the POIs and the Safegraph data for the selected country and month.
  To do this, we perform a join between both dataframes using the geocode. The resulting dataframe has a row where both the latitude and longitude of the user and the poi are present. Then using the vincenty formula, a distance is obtained. Finaly, we filter it by a minium distance.

   @param spark: Spark session that will be used to load the data.
   @param file_name: path of the dataset containing the POIs. Must be correctly formated as described (name|latitude|longitude (without the index), with the latitude and longitude with point (".") as decimal delimiter.)
   @param return df_pois_final: dataframe created from the one provided by the user containing the POIS: contains the geocode and renamed columns.
    */
  def match_POI(
      spark: SparkSession,
      safegraph_days: Integer,
      POI_file_name: String,
      country: String,
      output_file: String
  ) = {

    //getting POIs
    get_POI_coordinates(spark, POI_file_name)
      .repartition(200)
      .write
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/mx_nse/sales_parquet/")

    val df_pois_final =
      spark.read.load("/datascience/custom/mx_nse/sales_parquet/")
    df_pois_final.createOrReplaceTempView("pointtable")

    var pointDf1 = spark.sql(
      """select price_per_m2,
                radius,
                ST_Point(cast(cast(pointtable.latitude as double) as Decimal(24,20)),
                         cast(cast(pointtable.longitude as double) as Decimal(24,20))) as pointshape1 
         from pointtable"""
    )
    pointDf1.repartition(200).createOrReplaceTempView("pointdf1")

    //getting safegraph users
    val df_users = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/geo/AR/ar_home_120_07-02-19")
      .withColumnRenamed("_c0", "ad_id")
      .withColumnRenamed("_c1", "count")
      .withColumnRenamed("_c2", "utc_timestamp")
      .withColumnRenamed("_c3", "latitude")
      .withColumnRenamed("_c4", "longitude")
      .filter("count > 2") //get_safegraph_data(spark, safegraph_days, country)
    df_users.createOrReplaceTempView("pointtable")

    var pointDf2 = spark.sql(
      """select ad_id,
                utc_timestamp,
                ST_Point(cast(cast(pointtable.latitude as double) as Decimal(24,20)), 
                         cast(cast(pointtable.longitude as double) as Decimal(24,20))) as pointshape2 
         from pointtable"""
    )
    pointDf2.repartition(200).createOrReplaceTempView("pointdf2")

    // Here we obtain the points that are closer than the radius
    var distanceJoinDf = spark.sql(
      """select ad_id,
                utc_timestamp,
                price_per_m2
         from pointdf1 as pointdfdist1, pointdf2 as pointdfdist2
         where ST_Distance(pointdfdist2.pointshape2, pointdfdist1.pointshape1)  < radius"""
    )

    println("EXPLAIN")
    distanceJoinDf.explain()

    //storing result
    val filtered =
      distanceJoinDf
        .repartition(200)
        .write
        // .format("csv")
        // .option("sep", "\t")
        .mode(SaveMode.Overwrite)
        .save(output_file)
  }

  type OptionMap = Map[Symbol, Any]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--country" :: value :: tail =>
        nextOption(map ++ Map('country -> value.toString), tail)
      case "--poi_file" :: value :: tail =>
        nextOption(map ++ Map('poi_file -> value.toString), tail)
      case "--output" :: value :: tail =>
        nextOption(map ++ Map('output -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val safegraph_days =
      if (options.contains('nDays)) options('nDays).toString.toInt else 30
    val country =
      if (options.contains('country)) options('country).toString else "mexico"
    // val POI_file_name =
    //   if (options.contains('poi_file)) options('poi_file).toString else ""
    // val output_file =
    //   if (options.contains('output)) options('output).toString else ""

    // Start Spark Session
    val spark = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config(
        "spark.kryo.registrator",
        classOf[GeoSparkKryoRegistrator].getName
      )
      // .config("geospark.global.index", "true")
      // .config("geospark.global.indextype", "rtree")
      // .config("geospark.join.gridtype", "kdbtree")
      // .config("geospark.join.numpartition", 200)
      .appName("match_POI_geospark")
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)

    // chequear que el POI_file_name este especificado y el output_file tambien

    val POI_file_name = "hdfs://rely-hdfs/datascience/custom/mx_nse/sales.csv"
    val output_file = "/datascience/custom/price_per_m2_AR"

    // match_POI(spark, safegraph_days, POI_file_name, country, output_file)

    import org.datasyslab.geospark.spatialRDD.PointRDD
    import org.datasyslab.geospark.enums.FileDataSplitter
    import org.apache.spark.storage.StorageLevel
    import org.datasyslab.geospark.spatialOperator.JoinQuery
    import org.datasyslab.geospark.enums.IndexType
    import org.datasyslab.geospark.spatialRDD.CircleRDD
    import org.datasyslab.geospark.enums.GridType
    import scala.collection.JavaConverters._

    val sales = new PointRDD(
      spark.sparkContext,
      "hdfs://rely-hdfs/datascience/custom/mx_nse/homes_csv",
      0,
      FileDataSplitter.CSV,
      true,
      400,
      StorageLevel.MEMORY_ONLY
    )
    val homes = new PointRDD(
      spark.sparkContext,
      "hdfs://rely-hdfs/datascience/custom/mx_nse/sales_csv",
      0,
      FileDataSplitter.CSV,
      true,
      400,
      StorageLevel.MEMORY_ONLY
    )
    val salesCircles = new CircleRDD(sales, 0.01)

    homes.spatialPartitioning(GridType.QUADTREE);
    salesCircles.spatialPartitioning(homes.getPartitioner())

    homes.buildIndex(IndexType.RTREE, true)

    homes.indexedRDD.persist(StorageLevel.MEMORY_ONLY);
    salesCircles.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    JoinQuery
      .DistanceJoinQuery(homes, salesCircles, true, true)
      .rdd
      .map(
        t =>
          "%s,%s".format(
            t._1.getUserData(),
            t._2.asScala
              .map(m => m.getUserData().toString)
              .filter(_!="inf")
              .map(_.toDouble)
              .sum / t._2.asScala.size
          )
      )
      .saveAsTextFile("/datascience/custom/mx_nse/join_homes_sales/")
  }
}
