name := "Random"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.1"
libraryDependencies ++= Seq("org.uaparser" %% "uap-scala" % "0.3.0","org.uaparser" % "scala-reflect" % "2.11.1")

