package com.scylla.spark

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by rana on 26/12/17.
  */
object ScyllaIngestor extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("CassandraDataFrame")
    .config("spark.sql.streaming.schemaInference", true)
    .getOrCreate()

  import org.apache.spark.sql.cassandra._
  spark.setCassandraConf("Test Cluster",
    "spark_example",
    ReadConf.SplitSizeInMBParam.option(128))

  spark.setCassandraConf(
    CassandraConnectorConf.KeepAliveMillisParam.option(10000))

  val df = spark.readStream.json("json/*")
  val writer = df.writeStream.outputMode(OutputMode.Append())
    .format("io.fluirdb.spark.connector")
    .options(Map( "table" -> "persons", "keyspace" -> "spark_example", "cluster" -> "Test Cluster"))
    .option("checkpointLocation", "/tmp/checkpoint_json")
    .start()

  writer.awaitTermination()
}
