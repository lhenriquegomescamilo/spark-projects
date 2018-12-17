import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.broadcast

object keywordIngestion {
    def parseLineGCBA(line:String) = {
        val fields = line.split("\t", -1)

        // val time = fields(1)
        val id_partner = if (fields.size>16) fields(16) else ""
        val event_type = if (fields.size>8) fields(8) else ""
        val url = if (fields.size>31) fields(31) else ""

        (id_partner,event_type,url)
    }
    def process_day(day:String) = {
        println(day)
        val rdd = sc.textFile("/data/eventqueue/%s/*.tsv.gz".format(day))
        val processed = rdd.map(parseLineGCBA).filter(x => x._1 == "349" &&  event_types.contains(x._2)).map(tuple => tuple._3)

        processed.saveAsTextFile("/datascience/data_gcba/%s".format(day))
    }
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("data gcba process").getOrCreate

        ////////////////////// ACTUAL EXECUTION
        val event_types = List("tk")
        // Procesamos la data de ayer
        val format = "yyyy/MM/dd"
        val start = DateTime.now.minusDays(1)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

        days.foreach(day => process_day(day))
