package io.fluirdb.spark.connector

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{SqlRowWriter, TableWriter, WriteConf}
import com.datastax.spark.connector.{CassandraRow, ColumnRef, SomeColumns}
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
  * Created by rana on 22/12/17.
  */
class ScyllaWriter(connector: CassandraConnector,
                   writeConf: WriteConf,
                   keyspaceName: String,
                   tableName: String,
                   clusterName: String) extends Serializable {
  def open(partitionId: Long, version: Long): Boolean = {
    //todo: check the scylladb whether this partition was ingested successfully or not earlier
    true
  }

  def process(iter: Iterator[InternalRow], schema: StructType): Unit = {
    val rows : Iterator[Row] = iter.map(internalRow =>
      new GenericRowWithSchema(internalRow.toSeq(schema).toArray, schema))

    val cRows = rows.map(row => row.getValuesMap[AnyRef](schema.fieldNames)).map(x => CassandraRow.fromMap(x))

    val rowWriter = SqlRowWriter.Factory
    val columns = SomeColumns(schema.fieldNames.map(x => x: ColumnRef): _*)

    val writer = TableWriter(connector, keyspaceName, tableName, columns, writeConf)
    writer.write(TaskContext.get(), cRows)
  }

  def close(errorOrNull: Throwable): Unit = {

  }
}
