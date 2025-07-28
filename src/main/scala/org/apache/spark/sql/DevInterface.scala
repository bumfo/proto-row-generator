package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

object DevInterface {
  def internalCreateDataFrame(
                               spark: SparkSession,
                               catalystRows: RDD[InternalRow],
                               schema: StructType,
                               isStreaming: Boolean = false): DataFrame = {
    spark.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }
}
