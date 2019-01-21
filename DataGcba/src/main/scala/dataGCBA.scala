package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode

object dataGCBA {
    def process_day(spark:SparkSession, day:String){
        val df = spark.read.format("csv").option("sep","\t").option("header",true)
                        .load("/data/eventqueue/%s/*.tsv.gz".format(day))
                        .select("id_partner","event_type","url")
                        .filter("event_type = 'tk' and id_partner = 349")
                        .select("url")
        df.write.format("csv").mode(SaveMode.Overwrite).save("/datascience/data_gcba/%s".format(day))    
    }
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Data GCBA Process").getOrCreate()
        //val today = DateTime.now().toString("yyyy/MM/dd")
        val today = "2019/01/14"

        process_day(spark,today)
    }
}