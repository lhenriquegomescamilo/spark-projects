package main.scala
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions.{lit, length, split, col, concat_ws, collect_list}
import org.joda.time.{Days, DateTime}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object PipelineUS {
    def process_data(spark:SparkSession, day:String){

        /// Configuraciones de spark
        val sc = spark.sparkContext
        val conf = sc.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        
        val df_historic = spark.read.load("/datascience/sharethis/historic/day=%s".format(day))
                                    .select("estid","url")
        
        val matching_madid = spark.read.load("/datascience/sharethis/estid_madid_table/")
                                        .withColumnRenamed("device","device_id")

        val format = "yyyyMMdd"
        val start = DateTime.now.minusDays(60)
        val end   = DateTime.now.minusDays(0)
        
        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
        
        val dfs = days
                    .filter(d => fs.exists(new org.apache.hadoop.fs.Path("/datascience/sharethis/estid_table/day=%s".format(d))))
                    .map(x => spark.read.parquet("/datascience/sharethis/estid_table/day=%s".format(x))
                    .withColumnRenamed("d17","estid"))

        val matching_web = dfs.reduce((df1,df2) => df1.union(df2)).dropDuplicates()
        
        val matching_union = matching_web.unionAll(matching_madid)
        
        val join = df_historic.join(matching_union,Seq("estid"),"left").select("device_id","url","device_type")

        join.write.mode(SaveMode.Overwrite).save("/datascience/data_us_p/day=%s".format(day))
    }
    
    def get_data_us(spark: SparkSession, ndays:Int) {
        /// Configuraciones de spark
        val sc = spark.sparkContext
        val conf = sc.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        
        /// Obtenemos la data de los ultimos ndays
        val format = "yyyyMMdd"
        val start = DateTime.now.minusDays(ndays)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
        
        val dfs = days
                    .filter(day => fs.exists(new org.apache.hadoop.fs.Path("/datascience/sharethis/historic/day=%s".format(day))))
                    .map(x => process_data(spark,x))

    }
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Get data for pipeline data US").getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 1

    get_data_us(spark,ndays)
  }
}