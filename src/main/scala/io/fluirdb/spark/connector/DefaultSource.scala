package io.fluirdb.spark.connector

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by rana on 22/12/17.
  */
class DefaultSource extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    new ScyllaSink(sqlContext, parameters, partitionColumns, outputMode)
  }
}
