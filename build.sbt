ThisBuild / name := "shapeless-arrow"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2",
  "org.apache.arrow" % "arrow-vector" % "8.0.0",
  "org.apache.arrow" % "arrow-memory-netty" % "8.0.0" % "runtime",
  "org.scala-lang" % "scala-reflect" % "2.13.8",
  "com.chuusai" %% "shapeless" % "2.3.3",
  "com.esotericsoftware" % "kryo" % "5.3.0",
  "com.twitter" %% "chill" % "0.10.0"
)
