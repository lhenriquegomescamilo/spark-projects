package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object EstidMapper {
    /**
     * This function takes a day as input, and produces a set of tuples, where every tuple has the 
     * estid, device_id, device_type, and day.
    */
  def getEstIdsMatching(spark: SparkSession, day: String) = {
    val df = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      .filter(
        "d17 is not null and country = 'US' and event_type = 'sync'"
      )
      .select("d17", "device_id", "device_type")
      .withColumn("day", lit(day))
      .dropDuplicates()

    df.write
      .format("parquet")
      .patitionBy("day")
      .mode("append")
      .save("/datascience/sharethis/estid_table/")
  }

  
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Run matching estid-device_id").getOrCreate()
      
    val today = DateTime.now()
    val days = (1 until 30).map(
      days =>
        getEstIdsMatching(
          spark,
          today.minusDays(days).toString("yyyy/MM/dd")
        )
    )
  }
}
