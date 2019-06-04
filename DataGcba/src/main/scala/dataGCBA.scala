package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode

object dataGCBA {
    def process_day(spark:SparkSession, day:String){
        val df = spark.read.format("csv").option("sep","\t").option("header",true)
                        .load("/data/eventqueue/%s/*.tsv.gz".format(day))
                        .select("id_partner","event_type","url","timestamp")
                        .filter("event_type = 'tk' and id_partner = 349")
                        .select("url","timestamp")
        df.write.format("csv").mode(SaveMode.Overwrite).save("/datascience/data_gcba/%s".format(day))    
    }
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Data GCBA Process").getOrCreate()

         /// Parseo de parametros
        val since = if (args.length > 0) args(0).toInt else 0
        val ndays = if (args.length > 1) args(1).toInt else 1

        val format = "YYYYMMdd"
        val start = DateTime.now.minusDays(since+ndays)
        val end   = DateTime.now.minusDays(since)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

        days.map(day => process_day(spark,day))

        //process_day(spark,today)
    }
}