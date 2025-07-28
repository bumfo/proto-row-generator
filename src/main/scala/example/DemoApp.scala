package example


import com.google.protobuf.Timestamp
import fastproto.{ProtoToRowGenerator, RowConverter}
import org.apache.spark.sql.catalyst.InternalRow
// No need for StructType imports in this example

/**
 * A simple demonstration of the [[ProtoToRowGenerator]] in action.  This
 * program constructs a converter for the built‑in Protobuf `Timestamp` message
 * type and converts an instance directly into an [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]].
 * The converter writes values into an internal buffer using `UnsafeRowWriter`,
 * so no intermediate `GenericInternalRow` or `UnsafeProjection` is needed.
 * The example runs without a SparkSession and can be invoked from a plain
 * `main` method.
 */
object DemoApp {
  def main(args: Array[String]): Unit = {
    // Obtain the descriptor for the Timestamp message directly from the compiled class
    val descriptor = Timestamp.getDescriptor()
    // Generate a converter for Timestamp.  The returned converter writes
    // directly into an UnsafeRow using a BufferHolder and UnsafeRowWriter.
    val converter: RowConverter[Timestamp] =
      ProtoToRowGenerator.generateConverter(descriptor, classOf[Timestamp])
    // Build a sample message
    val ts = Timestamp.newBuilder().setSeconds(1621473001L).setNanos(500000000).build()
    // Convert the message into an UnsafeRow (which also implements InternalRow)
    val row: InternalRow = converter.convert(ts)
    println(s"UnsafeRow = ${row}")
    // Inspect individual fields using the UnsafeRow API
    val structType = converter match {
      case _ => fastproto.ProtoToRowGenerator.getClass // placeholder, schema introspection not exposed
    }
    println(s"Field 0 (seconds) = ${row.getLong(0)}")
    println(s"Field 1 (nanos) = ${row.getInt(1)}")
  }
}