
val SparkVersion = "2.3.1"

val SparkCompatibleVersion = "2.3.1"

val GeoSparkVersion = "1.1.3"

val dependencyScope = "compile"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_".concat(GeoSparkVersion) % GeoSparkVersion ,
  "org.datasyslab" % "geospark-viz" % GeoSparkVersion
)
