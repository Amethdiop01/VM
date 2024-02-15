name := "MonProjet"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.6.5",
  "com.lihaoyi" %% "ujson" % "1.4.0",
  "com.lihaoyi" %% "os-lib" % "0.9.1",
  "org.apache.spark" %% "spark-sql" % "3.3.0"
)
