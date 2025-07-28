# Proto Row Generator

This project provides a minimal example of how to bridge compiled
Protocol Buffers messages into Spark SQL's internal row format without
resorting to dynamic reflection.  At its core lies the
`fastproto.ProtoToRowGenerator` object which uses the Janino compiler to
generate Java code at runtime.  The generated class implements the
`fastproto.RowConverter` interface and writes field values directly into a
Spark `UnsafeRow` using `BufferHolder` and `UnsafeRowWriter`.  No
`GenericInternalRow` bridge is required.

The idea is inspired by the `spark-protobuf` project but eliminates the
overhead of `DynamicMessage` and reflection by operating directly on
generated Java classes.  In this enhanced implementation most common
Protobuf patterns are supported: primitive fields, nested messages,
repeated fields (arrays) and recursive types.  Map fields are lowered
to arrays of key/value structs for simplicity.  Support for further
types (e.g. oneof or special wrappers) can be added by extending the
code generation logic.

## Structure

```
proto_row_generator/
  ├── build.sbt                 – project definition with Spark, Protobuf and Janino dependencies
  ├── project/
  │   └── build.properties      – SBT version specification
  └── src/
      └── main/
          └── scala/
              ├── fastproto/
              │   ├── RowConverter.scala
              │   └── ProtoToRowGenerator.scala
              └── example/
                  └── DemoApp.scala
```

### `fastproto.RowConverter`

A simple trait representing a function from a compiled Protobuf message to a
Spark `InternalRow`.  Implementations are generated on the fly and avoid
reflection.

### `fastproto.ProtoToRowGenerator`

Given a Protobuf `Descriptor` and the corresponding compiled message class,
this object builds a small Java class as a string, compiles it with
Janino, and returns an instance of `RowConverter`.  The generated code
extracts field values using the message's accessor methods and writes them
directly into an `UnsafeRow` via a `BufferHolder` and `UnsafeRowWriter`.
Primitive numeric types, booleans, strings, byte strings and enums are
supported.  Nested messages are handled recursively by invoking a nested
converter, and repeated fields are materialised into `ArrayData` and
written through the writer.  Map fields are represented as arrays of
structs (key/value pairs) for simplicity.  Additional types can be
handled by extending the code generation logic.

### `example.DemoApp`

A small program demonstrating how to use the generator with the built‑in
`com.google.protobuf.Timestamp` message.  It constructs a `RowConverter`
for `Timestamp`, converts a sample message into an `UnsafeRow`, and
prints the contents.

```scala
// Obtain the descriptor and generate a converter
val descriptor = Timestamp.getDescriptor()
val converter = ProtoToRowGenerator.generateConverter(descriptor, classOf[Timestamp])

// Build and convert a message
val ts = Timestamp.newBuilder().setSeconds(1621473001L).setNanos(500000000).build()
val unsafeRow = converter.convert(ts)

// Access individual fields directly from the UnsafeRow
val seconds = unsafeRow.getLong(0)
val nanos   = unsafeRow.getInt(1)
println(s"seconds = $seconds, nanos = $nanos")
```

Note that Spark 3.2.1 is used in this build, so this project aligns with
the user's environment.  To build or run the example, install SBT and
execute `sbt run` from the `proto_row_generator` directory.  Network
connectivity is required on first run to download dependencies.