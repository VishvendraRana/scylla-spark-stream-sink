package io.fluirdb.spark.connector

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by rana on 22/12/17.
  */
class ScyllaSink(sqlContext: SQLContext,
                 parameters: Map[String, String],
                 partitionColumns: Seq[String],
                 outputMode: OutputMode) extends Sink with Serializable {

  val writeConf: WriteConf = WriteConf.fromSparkConf(sqlContext.sparkContext.getConf)
  val connector = CassandraConnector(sqlContext.sparkContext)

  val keyspaceName: String = parameters.getOrElse("keyspace",
    throw new IllegalArgumentException("keyspace can't be empty"))
  val tableName: String = parameters.getOrElse("table",
    throw new IllegalArgumentException("table can't be empty"))
  val clusterName: String = parameters.getOrElse("cluster",
    throw new IllegalArgumentException("cluster can't be empty"))

  val ksName = sqlContext.sparkContext.broadcast[String](keyspaceName)
  val tName = sqlContext.sparkContext.broadcast[String](tableName)
  val cName = sqlContext.sparkContext.broadcast[String](clusterName)
  val cConnector = sqlContext.sparkContext.broadcast[CassandraConnector](connector)
  val wConf = sqlContext.sparkContext.broadcast[WriteConf](writeConf)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    @transient val schema = data.schema
    data.queryExecution.toRdd.foreachPartition { iter =>

      val scyllaWriter = new ScyllaWriter(cConnector.value, wConf.value,
      ksName.value, tName.value, cName.value)

      if (scyllaWriter.open(TaskContext.getPartitionId(), batchId)) {
        try {
          scyllaWriter.process(iter, schema)
        } catch {
          case e: Throwable =>
            scyllaWriter.close(e)
            throw e
        }
        scyllaWriter.close(null)
      } else {
        scyllaWriter.close(null)
      }
    }
  }
}
