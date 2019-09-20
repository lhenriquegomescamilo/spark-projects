package main.scala

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object FromEventqueuePII {

  /**
    * Given a particular day, this method downloads the data from the eventqueue to build a PII table. Basically, it takes the following columns:
    * device_id, device_type, country, id_partner, data_type, ml_sh2, mb_sh2, nid_sh2, day.
    *
    * Only a small sample of the eventqueue is obtained, so it does not take so much memory space.
    */
  def getPII(spark: SparkSession, day: String) {
    // First we load the data
    val filePath = "/data/eventqueue/%s/*.tsv.gz".format(day)
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load(filePath)

    // Now we process the data and store it
    data
      .withColumn("day", lit(day.replace("/", "")))
      .filter(
        (col("ml_sh2").isNotNull || col(
          "mb_sh2"
        ).isNotNull || col("nid_sh2").isNotNull)
      )
      .select(
        "device_id",
        "device_type",
        "country",
        "id_partner",
        "data_type",
        "ml_sh2",
        "mb_sh2",
        "nid_sh2",
        "day"
      )
      .distinct()
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("day")
      .save("/datascience/pii_matching/pii_tuples")
  }

  def procesPII(spark: SparkSession) {
    // First we load all the data generated from PIIs
    val data = spark.read
        .format("parquet")
        .load("/datascience/pii_matching/pii_tuples/")
        //.filter("day >= 20190918")
        .filter("country in('AR', 'CL', 'PE')")
    // Then we separate the data acording to the PII type
    var mails = data
      .filter("ml_sh2 is not null")
      .select("device_id","country")
      .withColumnRenamed("ml_sh2", "device_id")
      .withColumn("device_type", lit("mail"))
      .dropDuplicates("country", "device_id")

    var dnis = data
      .filter("nid_sh2 is not null")
      .select("device_id","country")
      .withColumnRenamed("nid_sh2", "device_id")
      .withColumn("device_type", lit("nid"))
      .dropDuplicates("country", "device_id")

    var mobs = data
      .filter("mb_sh2 is not null")
      .select("device_id","country")
      .withColumnRenamed("mb_sh2", "device_id")
      .withColumn("device_type", lit("mob"))
      .dropDuplicates("country", "device_id")

    var total = mails
        .unionAll(dnis)
        .unionAll(mobs)
        .orderBy("country", "device_type", "device_id")
    // We group the data and get the list of pii for each device_id with the correspondant id_partner and timestamp in a tuple.
    //var grouped = total
    //  .groupBy("device_id", "country", "pii", "pii_type")
    //  .agg(
    //    collect_list("id_partner").as("id_partner"),
    //    collect_list("day").as("days"),
    //    collect_list("device_type").as("device_type")
    //  )
    // Then we sort the tuples and we keep the one with the smallest timestamp.
    val udfSort = udf(
      (id_partner: Seq[String], days: Seq[String]) =>
        (id_partner zip days).sortBy(_._2).toList(0)
    )
    //val df_final = grouped
    //  .withColumn("result", udfSort(col("id_partner"), col("days")))
    //  .withColumn("id_partner", col("result").getItem("_1"))
    //  .withColumn("days", col("result").getItem("_2"))
    //  .select("device_id", "country", "pii", "pii_type", "id_partner", "days")

    // We save the generated file
    total.write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/pii_matching/temp/")

    // Load files again 
    val fls = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/temp/")
    
    fls.repartition(1).write
      .format("csv")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/pii_matching/pii_table")
  }


  type OptionMap = Map[Symbol, String]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toString), tail)
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val from = if (options.contains('from)) options('from).toInt else 1
    val nDays = if (options.contains('nDays)) options('nDays).toInt else 1

    val spark =
      SparkSession.builder
        .appName("Get Pii from Eventqueue")
        .getOrCreate()

    // Here we obtain the list of days to be downloaded
    // val nDays = 1
    // val from = 1
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we effectively download the data day by day
    days.map(day => getPII(spark, day))

    procesPII(spark)
  }

}
