# Proto Row Generator (Spark Protobuf Janino Example)

This project demonstrates how to generate **Janino‑compiled** Scala/Java classes that map compiled [Protocol Buffers](https://developers.google.com/protocol-buffers) messages directly into Spark SQL’s `UnsafeRow` format without relying on reflection.  The goal is to avoid the dynamic, slower `DynamicMessage`/`GenericRow` path in Spark’s built‑in `from_protobuf` support and instead emit bytecode that extracts each field from the compiled message class and writes it into an `UnsafeRow` buffer.

## Background

- **Spark SQL internal representation.** Spark stores rows in a binary format called `UnsafeRow`.  To populate an `UnsafeRow` you must use `UnsafeRowWriter` or `UnsafeArrayWriter` – these classes write fixed‑width values directly into a buffer and update offsets for variable‑length data.  There is no overload on `UnsafeRowWriter` for `ArrayData`, so array fields must be written using `UnsafeArrayWriter`【626752585479751†L160-L187】.
- **Protobuf descriptors.** The project reads a Protobuf `Descriptor` either from the compiled Java class (via `getDescriptor()`) or from a descriptor file.  It then builds a Spark `StructType` for the message, converting primitive types, nested messages, repeated fields and map entries appropriately.
- **Janino.** Janino is a lightweight Java compiler used at runtime to generate classes.  The project synthesises a Java class implementing a `fastproto.RowConverter[T]` interface.  The generated class extracts each field using the compiled message’s getters and writes them into an `UnsafeRow` via `UnsafeRowWriter` or `UnsafeArrayWriter`.
- **CamelCase accessors.** Protobuf field names use snake_case in `.proto` definitions, but compiled Java classes use camelCase methods.  For example, a field `source_context` generates `getSourceContext()` and `hasSourceContext()` methods.  The generator uses a helper to convert snake_case names into CamelCase to derive accessor names correctly; without this, reflection attempts like `getSource_context()` will fail【626752585479751†L160-L187】.

## Project structure

```
proto_row_generator/
├── build.sbt                  # SBT build definition (Scala 2.12, Spark 3.2.1)
├── project/
│   └── build.properties       # sets sbt version
├── src/main/scala/
│   ├── fastproto/
│   │   ├── RowConverter.scala # trait with `convert()` and `schema()`
│   │   └── ProtoToRowGenerator.scala # generates converters via Janino
│   └── example/
│       └── DemoApp.scala      # example usage of the generator
└── README.md                 # (generated) description and instructions
```

### Key classes

- **fastproto.RowConverter[T]** – a small interface with two methods:
  - `convert(message: T): InternalRow` converts a compiled `com.google.protobuf.Message` to an `InternalRow` (an `UnsafeRow` instance).  This method must be implemented by generated classes.
  - `schema: StructType` returns the Spark schema used by the converter.
- **fastproto.ProtoToRowGenerator** – factory that builds a `StructType` from a Protobuf descriptor and then generates a Java class implementing `RowConverter[T]`.  The generator:
  1. Recursively converts the descriptor to a `StructType`, mapping primitive and nested message fields.
  2. Precomputes nested `RowConverter`s for message fields.
  3. Writes a Java source string that declares fields for the schema, nested converters and an `UnsafeRowWriter`.
  4. Emits code for each field to call the appropriate accessor on the message and write the value:
     - Primitive singletons use `writer.write(int, value)`.
     - Nested messages call the nested converter and cast the result to `UnsafeRow` before writing.
     - Repeated fields allocate an `UnsafeArrayWriter` with the correct element size (4 or 8 bytes for primitives, 8 bytes for variable‑length types and nested messages) and loop over elements.  After writing the array, the converter calls `writer.setOffsetAndSizeFromPreviousCursor(ordinal, offsetBeforeArray)` to update the row’s offset and size【626752585479751†L160-L187】.
  5. Adds a sugar method `convert(Object obj)` to satisfy the erased Java signature of the Scala trait.  Janino only sees the erased method, so a bridge method is required; the typed method should **not** be annotated with `@Override` to avoid compilation errors.
  6. Implements `schema()` to return the stored `StructType`.
- **example.DemoApp** – demonstrates loading a descriptor and compiled class, generating a converter, and converting a sample message.

## Building from scratch

1. **Prerequisites**:
   - Java 8 or 11.
   - [SBT](https://www.scala-sbt.org/) version 1.11.3.  You can install it via a package manager or download from the official site.
   - Scala 2.12 is used implicitly via the Spark dependency.
   - Apache Spark 3.2.1 (only the Spark SQL and Catalyst jars are pulled as dependencies via SBT).

2. **Unpack the project**:

   ```sh
   tar -xzvf proto_row_generator_patch14.tar.gz
   cd proto_row_generator
   ```

   (Replace the tarball name with the file you downloaded.)

3. **Compile and run**:

   ```sh
   sbt compile   # compiles the sources and resolves dependencies
   sbt run       # runs the DemoApp example
   ```

   The demo prints the input Protobuf message, the resulting `UnsafeRow`, and its schema.

4. **Using the generator in your code**:

   ```scala
   import com.google.protobuf.Descriptors
   import fastproto.ProtoToRowGenerator

   val descriptor: Descriptors.Descriptor = MyMessage.getDescriptor
   val converter  = ProtoToRowGenerator.generateConverter(descriptor, classOf[MyMessage])
   val row: org.apache.spark.sql.catalyst.InternalRow = converter.convert(myMessageInstance)
   val schema: org.apache.spark.sql.types.StructType = converter.schema
   ```

   You can then wrap the `InternalRow` in an `UnsafeRow` or use it directly in Spark SQL operations.

## Guidelines and pitfalls

- **Accessor names must be CamelCase** – always derive getter names by converting each `snake_case` field name to CamelCase.  Do not simply capitalise the first letter; otherwise, field names such as `source_context` will lead to reflection errors like `NoSuchMethodException: getSource_context()`.
- **Repeated fields require `UnsafeArrayWriter`** – there is **no** `write(int, ArrayData, DataType)` overload on `UnsafeRowWriter`【626752585479751†L160-L187】.  You must allocate an `UnsafeArrayWriter` with the correct element size, write each element via the appropriate `write` method, and then call `writer.setOffsetAndSizeFromPreviousCursor`.
- **Nested messages** – call the nested converter, cast its result to `UnsafeRow` and use `writer.write(int, UnsafeRow)`【626752585479751†L160-L187】.  For repeated nested messages, treat them as arrays as above.
- **Avoid `@Override` on the typed `convert` method** – the Scala trait’s erased method signature is what Janino sees.  Annotating the typed method with `@Override` can lead to compile errors because Janino does not recognise it as overriding the erased method.  Instead, implement a bridge method `convert(Object)` with `@Override` that casts and delegates to the typed method.
- **Do not reference `BufferHolder`** – it is package‑private in Spark and cannot be imported.  Use `UnsafeRowWriter.reset()` and `UnsafeRowWriter.zeroOutNullBytes()` to prepare the buffer; `UnsafeRowWriter.getRow()` will set the total size and return the row【626752585479751†L160-L187】.

## Extending the project

- **Map fields** – Spark’s Catalyst uses `UnsafeMapData` for maps.  This example treats Protobuf maps as an array of key/value structs to avoid dealing with `UnsafeMapData`.  You can extend the generator to write true map fields using `UnsafeMapWriter` if needed.
- **Custom type mappings** – The mapping of Protobuf types to Spark types is simplified.  You may want to support 64‑bit timestamps, durations, or wrapper types explicitly.  Extend `fieldToDataType` accordingly.

## Conclusion

This project provides a starting point for high‑performance Protobuf deserialization into Spark SQL.  By generating Janino‑compiled converters, it avoids reflection and dynamic dispatch, producing code that writes directly into `UnsafeRow` buffers.  The example is intentionally minimal; you can build on it to support more Protobuf types, integrate with DataFrames/Datasets, or generate converters ahead of time.
