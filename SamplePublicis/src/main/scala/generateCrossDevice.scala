package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions.{explode,desc,lit,size,concat,col,concat_ws,collect_list,udf,broadcast}
import org.joda.time.{Days,DateTime}

object generateCrossDevice {
    def generate_organic_xd(spark:SparkSession){
    /**
        val df = spark.read.format("parquet").load("/datascience/crossdevice/double_index")
                                          .filter("index_type = 'c' and device_type in ('a','i')")
                                          .withColumn("device",concat(col("device_type"),col("device")))
                                          .groupBy("index")
                                          .agg(collect_list("device"))

        val udfAndroid = udf((segments: Seq[String]) => segments.filter(segment => segment.charAt(0) == 'a')
                                                                .map(segment => segment.substring(1,segment.length)))
        val udfIos = udf((segments: Seq[String]) => segments.filter(segment => segment.charAt(0) == 'i')
                                                            .map(segment => segment.substring(1,segment.length)))
        val udfString = udf((lista: Seq[String]) => if (lista.length > 0) lista.reduce((seg1, seg2) => seg1+","+seg2)
                                                              else "")
                                                              
        val index_xd = df.withColumn("android",udfAndroid(col("collect_list(device)")))
                        .withColumn("ios",udfIos(col("collect_list(device)")))
                        .withColumn("android",udfString(col("android")))
                        .withColumn("ios",udfString(col("ios")))
                        .withColumnRenamed("index","device_id")
                        .drop("collect_list(device)")
**/
        val index_xd = spark.read.format("parquet").load("/datascience/crossdevice/list_index")

        val organic = spark.read.format("csv").option("sep", "\t")
                              .load("/datascience/data_publicis/organic")
                              .withColumnRenamed("_c0","device_id")
                              .withColumnRenamed("_c1","general_segments")
                              .withColumnRenamed("_c2","geo_segments")

        val joint = organic.join(index_xd,Seq("device_id"),"left_outer")
                        
        joint.write.format("csv")
                    .mode(SaveMode.Overwrite)
                    .save("/datascience/data_publicis/organic_xd")
    }
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Generate Cross device").getOrCreate()

        generate_organic_xd(spark)
    }
}
