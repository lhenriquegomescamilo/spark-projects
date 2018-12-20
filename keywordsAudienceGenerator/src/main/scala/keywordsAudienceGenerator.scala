import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode

object keywordsAudienceGenerator {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()

        /////////////////////////// PARAMETER PARSING ////////////////////////

        // First we specify the parameters:
        // - query: query in SparkSQL format
        // - cantDays: number of days that will be used for the query
        //val query    = "event_type IN ('pv', 'batch') AND country IN ('AR','MX','CO','CL') AND str_contains(url, 'autocosmo')"

        if (args.length < 2) {
            println("Parameters are missing. \n  1 => Quantity of Days. Possible values from 1 to 60\n  2 => Output Path. Folder to be created inside /datascience/audiences/output/\n")
            System.exit(1)
        }

        var cantDays = 1
        try {
            cantDays = args(0).toInt
        } catch {
            case e: Exception => 0
        }

        if (cantDays<1 || cantDays>60) {
            println("Invalid quantity of days, must be between 1 and 60 days. Value: %s".format(cantDays))
            System.exit(1)
        }

        val output_path = args(1);
        val query = args(2)
        if (Option(output_path).getOrElse("").isEmpty) {
            println("Invalid output file parameter.")
            System.exit(1)
        }

        val since = 3
        ////////////////////// ACTUAL EXECUTION ////////////////////////

        // In the first place we get the files that will be used for the query //
        // First we specify the range of days
        val format = "yyyy-MM-dd"
        val start = DateTime.now.minusDays(cantDays+since)
        val end   = DateTime.now.minusDays(since)
        val daysCount = Days.daysBetween(start, end).getDays()

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        val range = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
        val hdfs_files = range.map(day => "/datascience/data_audiences_p/day=%s".format(day.replace("-", "")))
                            .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

        // Now we read the dataframes and append all of them
        val df = spark.read.option("basePath","/datascience/data_audiences_p/").parquet(hdfs_files:_*)
        // Here we define a function that might be used when asking for an IN in a multivalue column
        spark.udf.register("array_intersect", (xs: Seq[String], ys: Seq[String]) => xs.intersect(ys).size>0)
        // Here I define a function that will be useful for testing if a string is contained in another string
        spark.udf.register("str_contains", (x: String, y: String) => x.contains(y))
        // Now we create a view to be able to use SparkSQL in our dataframe
        df.createOrReplaceTempView("dataset")
        // Finally we execute the query
        val users = spark.sql("SELECT device_id, device_type FROM dataset WHERE %s".format(query)).distinct()
        users.write.mode(SaveMode.Overwrite).format("csv").option("sep", " ").save("/datascience/audiences/output/%s".format(output_path))
    }
}
