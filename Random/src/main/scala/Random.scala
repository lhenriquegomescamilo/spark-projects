package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.SaveMode


/**
 * The idea of this script is to run random stuff. Most of the times, the idea is
 * to run quick fixes, or tests.
 */
object Random {
  def main(args: Array[String]) {
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Random").getOrCreate()

    val taxo = spark.read.format("csv").option("sep", "\t").load("/datascience/audiences/crossdeviced/taxo")
    taxo.cache()
    val audiencias = "5205, 5208, 5203, 5202, 5243, 5298, 5299, 5262, 5303, 5308, 5242, 5309, 5310, 5290, 5291, 5295, 5296".split(", ").toList

    for (audience_xd <- audiencias)
    {
      val counts = taxo.filter("_c2 LIKE '%" + audience_xd + "%'")
                       .groupBy("_c1")
                       .agg( count(col("_c0")) ).collect()
      println("%s,%s,%s,%s".format(audience_xd, counts(0)(1), counts(1)(1), counts(2)(1)))
    }
  }
}
