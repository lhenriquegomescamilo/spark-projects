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
        
        //val matching_madid = spark.read.load("/datascience/sharethis/estid_madid_table/")
        //                                .withColumnRenamed("device","device_id")

        val days = (1 until 60).map(DateTime.now.minusDays(_)).map(_.toString("yyyyMMdd"))
        val dfs = days
                    .filter(d => fs.exists(new org.apache.hadoop.fs.Path("/datascience/sharethis/estid_table/day=%s".format(d))))
                    .map(x => spark.read.parquet("/datascience/sharethis/estid_table/day=%s".format(x))
                    .withColumnRenamed("d17","estid"))

        val matching_web = dfs.reduce((df1,df2) => df1.union(df2)).dropDuplicates()
        
        print("Get data US: Processing data ...")
        //val matching_union = matching_web.unionAll(matching_madid)
        
        val join = df_historic.join(matching_web,Seq("estid"),"left").select("device_id","url","device_type")

        join.write.mode(SaveMode.Overwrite).save("/datascience/data_us_p/day=%s".format(day))
    }
    
    def get_data_us(spark: SparkSession, ndays:Int, since:Int) {
        /// Configuraciones de spark
        val sc = spark.sparkContext
        val conf = sc.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        
        /// Obtenemos la data de los ultimos ndays
        val days = (since until ndays+since).map(DateTime.now.minusDays(_)).map(_.toString("yyyyMMdd"))
        
        val dfs = days
                    .filter(day => fs.exists(new org.apache.hadoop.fs.Path("/datascience/sharethis/historic/day=%s".format(day))))
                    .map(x => process_data(spark,x))
    }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Get data for pipeline data US").getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 1
    val since = if (args.length > 1) args(1).toInt else 2

    get_data_us(spark,ndays,since)
  }
}