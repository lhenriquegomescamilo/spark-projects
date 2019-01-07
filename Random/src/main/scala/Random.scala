package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions.{explode,desc,lit,size,concat,col,concat_ws,collect_list,udf,broadcast,sha2}
import org.joda.time.{Days,DateTime}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.SaveMode


/**
 * The idea of this script is to run random stuff. Most of the times, the idea is
 * to run quick fixes, or tests.
 */
object Random {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Run matching estid-device_id").getOrCreate()
    val format = "yyyy/MM/dd"
    val start = DateTime.now.minusDays(15)
    val end   = DateTime.now.minusDays(15)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val dfs = (0 until daysCount).map(start.plusDays(_))
      .map(_.toString(format))
      .map(x => spark.read.format("csv").option("sep", "\t")
                      .option("header", "true")
                      .load("/data/eventqueue/%s/*.tsv.gz".format(x))
                      .filter("d17 is not null and country = 'US' and event_type is sync")
                      .select("d17","device_id")
                      .dropDuplicates())

    val df = dfs.reduce(_ union _)

    df.write.format("csv").option("sep", "\t")
                    .option("header",true)
                    .mode(SaveMode.Overwrite)
                    .save("/datascience/matching_estid_2")

  }
}