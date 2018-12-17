package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col}
import org.apache.spark.sql.SaveMode

/**
 * The idea of this script is to run random stuff. Most of the times, the idea is
 * to run quick fixes, or tests.
 */
object Random {
  def main(args: Array[String]) {
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Random").getOrCreate()
    
    
  }
}