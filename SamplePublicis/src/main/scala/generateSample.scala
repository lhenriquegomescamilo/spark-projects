package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions.{explode,desc,lit,size,concat,col,concat_ws,collect_list,udf,broadcast}
import org.joda.time.{Days,DateTime}
import org.apache.hadoop.fs.Path

object generateSample {
    def generate_sample(spark:SparkSession){

        val organic = spark.read.format("csv").option("sep", "\t")
                                .load("/datascience/data_publicis/organic")
                                .withColumnRenamed("_c0","device_id")
                                .withColumnRenamed("_c1","general_segments")
                                .withColumnRenamed("_c2","geo_segments")
        
        val modeled = spark.read.format("csv").option("sep", "\t")
                                .load("/datascience/data_publicis/modeled")
                                .withColumnRenamed("_c0","device_id")
                                .withColumnRenamed("_c1","modeled_segments")
        
        val sample = organic.join(modeled,Seq("device_id"),"left_outer")
        
        sample.write.format("csv").option("sep", "\t")
                            .mode(SaveMode.Overwrite)
                            .save("/datascience/data_publicis/sample_publicis")

    }
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Generate Sample").getOrCreate()
        
        generate_sample(spark)
    }
}
