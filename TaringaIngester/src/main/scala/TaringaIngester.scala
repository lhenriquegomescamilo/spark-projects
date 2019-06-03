package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode

object TaringaIngester {
    def process_day(spark:SparkSession, day:String){
        
        val countries = List("AR","MX")

        for(c <- countries){
            spark.read.load("/datascience/data_audiences/day=%s/country=%s".format(day,c))
                        .filter("url LIKE '%taringa%'")
                        .withColumn("all_segments",concat_ws(",", col("all_segments")))
                        .select("device_id","all_segments","url","timestamp")
                        .write.format("csv")
                        .option("header","true")
                        .save("/datascience/taringa_ingester/data_%s_%s".format(day,c))
        }
    
    }
    
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Data GCBA Process").getOrCreate()

         /// Parseo de parametros
        val since = if (args.length > 0) args(0).toInt else 1
        val ndays = if (args.length > 1) args(1).toInt else 0

        val format = "YYYYMMdd"
        val start = DateTime.now.minusDays(since+ndays)
        val end   = DateTime.now.minusDays(since)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

        days.map(day => process_day(spark,day))
    }
}