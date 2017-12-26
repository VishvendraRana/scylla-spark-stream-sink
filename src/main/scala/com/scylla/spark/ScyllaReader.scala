package com.scylla.spark

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.SparkSession

/**
  * Created by rana on 21/12/17.
  */
object ScyllaReader extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("CassandraDataFrame")
    .getOrCreate()

  import org.apache.spark.sql.cassandra._

  spark.setCassandraConf(
    CassandraConnectorConf.KeepAliveMillisParam.option(10000))
  spark.setCassandraConf("Test Cluster",
    CassandraConnectorConf.ConnectionHostParam.option("172.16.238.6") ++
      CassandraConnectorConf.ConnectionPortParam.option(9042))

  spark.setCassandraConf("Test Cluster",
    "spark_example",
    ReadConf.SplitSizeInMBParam.option(128))

  val df = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "persons", "keyspace" -> "spark_example", "cluster" -> "Test Cluster"))
    .load()

  df.printSchema()
  df.show()
}