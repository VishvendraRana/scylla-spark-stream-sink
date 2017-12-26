name := "scylla-spark-example"

version := "1.0"

scalaVersion := "2.11.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"