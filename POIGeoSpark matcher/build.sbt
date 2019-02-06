name := "POI Geo SparkMatcher"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.datasyslab" % "geospark" % "1.1.3"
libraryDependencies +=   "org.datasyslab" % "geospark-sql_".concat(2.3.1) %
libraryDependencies +=     "org.datasyslab" % "geospark-viz" % "1.1.3"
