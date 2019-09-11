name := "GeoDevicer"

version := "1.0"

scalaVersion := "2.11.8"

val dependencyScope = "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"
libraryDependencies += "org.datasyslab" % "geospark-sql_2.3" % "1.2.0"
libraryDependencies += "org.datasyslab" % "geospark-viz" % "1.1.3"


resolvers +=
 "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers +=
 "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"