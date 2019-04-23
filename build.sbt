name := "ETLclean"

version := "0.1"

scalaVersion := "2.12.8"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.15"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// https://mvnrepository.com/artifact/net.liftweb/lift-json
libraryDependencies += "net.liftweb" %% "lift-json" % "3.3.0"

