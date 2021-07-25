name := "kafka-learning"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.30"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.30"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.30"
libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2"
