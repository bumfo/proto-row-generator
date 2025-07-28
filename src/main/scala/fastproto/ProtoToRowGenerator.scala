package fastproto

// Use JavaConverters for Scala 2.12 compatibility
import scala.collection.JavaConverters._

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}

import org.codehaus.janino.SimpleCompiler

import org.apache.spark.sql.types._

/**
 * Factory object for generating [[RowConverter]] instances on the fly.  Given a
 * Protobuf [[Descriptor]] and the corresponding compiled message class, this
 * object synthesises a small Java class that extracts each field from the
 * message and writes it into a new array.  The generated class implements
 * [[RowConverter]] for the provided message type.  Janino is used to
 * compile the generated Java code at runtime.  Only a subset of types is
 * currently supported: primitive numeric types, booleans, strings, byte
 * strings and enums.  Nested messages and repeated fields are emitted as
 * `null` placeholders and can be extended with recursive conversion logic.
 */
object ProtoToRowGenerator {

  /**
   * Recursively build a Spark SQL [[StructType]] corresponding to the
   * structure of a Protobuf message.  Primitive fields are mapped to
   * appropriate Catalyst types; repeated fields become [[ArrayType]] and
   * Protobuf map entries become [[MapType]].  Nested message types are
   * converted into nested [[StructType]]s.
   *
   * @param descriptor the root Protobuf descriptor
   * @return a [[StructType]] representing the schema
   */
  private def buildStructType(descriptor: Descriptor): StructType = {
    val fields = descriptor.getFields.asScala.map { fd =>
      val dt = fieldToDataType(fd)
      // Proto3 fields are optional by default; mark field nullable unless explicitly required
      val nullable = !fd.isRequired
      StructField(fd.getName, dt, nullable)
    }
    StructType(fields.toArray)
  }

  /**
   * Convert a Protobuf field descriptor into a Spark SQL [[DataType]].
   * Nested messages are handled recursively.  Repeated fields become
   * [[ArrayType]] and Protobuf map entry types are translated into
   * [[MapType]].  Primitive wrapper types (e.g. IntValue) are treated
   * according to their contained primitive.
   */
  private def fieldToDataType(fd: FieldDescriptor): DataType = {
    import FieldDescriptor.JavaType._
    // Handle Protobuf map entries: repeated message types with mapEntry option.  In this
    // implementation we do not emit a Spark MapType because writing MapData into
    // an UnsafeRow requires more complex handling.  Instead, treat map entries
    // as an array of structs with two fields (key and value).  This approach
    // still captures all information from the map and avoids the need to build
    // MapData at runtime.
    if (fd.isRepeated && fd.getType == FieldDescriptor.Type.MESSAGE && fd.getMessageType.getOptions.hasMapEntry) {
      // Build a StructType for the map entry (with key and value fields) and wrap it in an ArrayType.
      val entryType = buildStructType(fd.getMessageType)
      ArrayType(entryType, containsNull = false)
    } else if (fd.isRepeated) {
      // Repeated (array) field
      val elementType = fd.getJavaType match {
        case INT => IntegerType
        case LONG => LongType
        case FLOAT => FloatType
        case DOUBLE => DoubleType
        case BOOLEAN => BooleanType
        case STRING => StringType
        case BYTE_STRING => BinaryType
        case ENUM => StringType
        case MESSAGE => buildStructType(fd.getMessageType)
      }
      ArrayType(elementType, containsNull = false)
    } else {
      fd.getJavaType match {
        case INT => IntegerType
        case LONG => LongType
        case FLOAT => FloatType
        case DOUBLE => DoubleType
        case BOOLEAN => BooleanType
        case STRING => StringType
        case BYTE_STRING => BinaryType
        case ENUM => StringType
        case MESSAGE => buildStructType(fd.getMessageType)
      }
    }
  }


  /**
   * Generate a concrete [[RowConverter]] for the given Protobuf message type.
   *
   * @param descriptor the Protobuf descriptor describing the message schema
   * @param messageClass the compiled Protobuf Java class
   * @tparam T the concrete type of the message
   * @return a [[RowConverter]] capable of converting the message into an
   *         [[org.apache.spark.sql.catalyst.InternalRow]]
   */
  def generateConverter[T <: com.google.protobuf.Message](descriptor: Descriptor,
                                                          messageClass: Class[T]): RowConverter[T] = {
    // Build the Spark SQL schema corresponding to this descriptor
    val schema: StructType = buildStructType(descriptor)

    // Precompute nested converters for message fields (both single and repeated) and map fields
    case class NestedInfo(field: FieldDescriptor, converter: RowConverter[_ <: com.google.protobuf.Message])
    val nestedInfos = scala.collection.mutable.ArrayBuffer[NestedInfo]()

    // Inspect each field to detect nested message types that require their own converter
    descriptor.getFields.asScala.foreach { fd =>
      if (fd.getJavaType == FieldDescriptor.JavaType.MESSAGE) {
        // Determine the compiled Java class for the nested message.  Protobuf
        // compiles nested messages as inner classes of the outer message.  However,
        // the runtime Java class name may differ from the proto descriptor name
        // due to reserved keywords or custom java_outer_classname options.  To
        // resolve this, we first search among the declared inner classes of the
        // outer message class for a class whose descriptor's full name matches the
        // field descriptor's full name.  If none is found, we fall back to
        // matching by simple name and then try loading by full name.
        val accessorName = fd.getName.substring(0, 1).toUpperCase + fd.getName.substring(1)
        val getterName = if (fd.isRepeated) s"get${accessorName}List" else s"get${accessorName}"
        val method = messageClass.getMethod(getterName)
        // The full name of the nested message descriptor
        val targetFullName = fd.getMessageType.getFullName
        // Attempt 1: find a declared inner class whose getDescriptor().getFullName matches
        val candidateByDescriptor: Option[Class[_ <: com.google.protobuf.Message]] =
          messageClass.getDeclaredClasses.collectFirst {
            case cls if classOf[com.google.protobuf.Message].isAssignableFrom(cls) =>
              try {
                val descMethod = cls.getMethod("getDescriptor")
                val nestedDesc = descMethod.invoke(null).asInstanceOf[Descriptor]
                if (nestedDesc.getFullName == targetFullName) {
                  cls.asInstanceOf[Class[_ <: com.google.protobuf.Message]]
                } else null
              } catch {
                case _: Exception => null
              }
          }.filter(_ != null)
        // Attempt 2: match by simple name
        val simpleName = fd.getMessageType.getName
        val candidateBySimple: Option[Class[_ <: com.google.protobuf.Message]] =
          messageClass.getDeclaredClasses.find(_.getSimpleName == simpleName).map(_.asInstanceOf[Class[_ <: com.google.protobuf.Message]])
        // Attempt 3: try to load class by full name or full name with $ separators
        val candidateByFullName: Option[Class[_ <: com.google.protobuf.Message]] = {
          val fullName = targetFullName
          val candidates = List(fullName, fullName.replace('.', '$'))
          candidates.iterator.map { name =>
            try {
              Some(Class.forName(name).asInstanceOf[Class[_ <: com.google.protobuf.Message]])
            } catch {
              case _: ClassNotFoundException => None
            }
          }.collectFirst { case Some(cls) => cls }
        }
        val nestedClass: Class[_ <: com.google.protobuf.Message] =
          candidateByDescriptor
            .orElse(candidateBySimple)
            .orElse(candidateByFullName)
            .getOrElse {
              // As a last resort, use the return type of the getter for non‑repeated fields
              if (!fd.isRepeated) {
                method.getReturnType.asInstanceOf[Class[_ <: com.google.protobuf.Message]]
              } else {
                throw new RuntimeException(s"Unable to resolve class for nested message ${fd.getFullName}")
              }
            }
        // Recursively generate a converter for the nested message type
        val nestedConverter = generateConverter(fd.getMessageType, nestedClass)
        nestedInfos += NestedInfo(fd, nestedConverter)
      }
    }

    // Assign variable names for nested converters in the generated code
    val nestedNames: Map[FieldDescriptor, String] = nestedInfos.zipWithIndex.map { case (info, idx) =>
      info.field -> s"nestedConv${idx}"
    }.toMap

    // Create a unique class name to avoid collisions when multiple converters are generated
    val className = s"GeneratedConverter_${descriptor.getName}_${System.nanoTime()}"
    val code = new StringBuilder
    // Imports required by the generated Java source
    code ++= "import org.apache.spark.sql.catalyst.expressions.UnsafeRow;\n"
    // We avoid importing BufferHolder because it is package‑private and cannot be referenced
    // directly from outside its package.  UnsafeRowWriter handles BufferHolder internally.
    code ++= "import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;\n"
    code ++= "import org.apache.spark.sql.catalyst.util.GenericArrayData;\n"
    code ++= "import org.apache.spark.sql.catalyst.util.ArrayData;\n"
    code ++= "import org.apache.spark.sql.types.StructType;\n"
    code ++= "import org.apache.spark.sql.types.ArrayType;\n"
    code ++= "import org.apache.spark.sql.types.StructField;\n"
    code ++= "import org.apache.spark.sql.types.DataType;\n"
    code ++= "import org.apache.spark.unsafe.types.UTF8String;\n"
    code ++= "import fastproto.RowConverter;\n"
    // Begin class declaration
    code ++= s"public final class ${className} implements fastproto.RowConverter<${messageClass.getName}> {\n"
    // Declare fields: schema, writer, and nested converters.  We avoid referencing
    // BufferHolder or UnsafeRow directly because BufferHolder is package‑private.
    code ++= "  private final StructType schema;\n"
    code ++= "  private final UnsafeRowWriter writer;\n"
    nestedNames.values.foreach { name =>
      code ++= s"  private final fastproto.RowConverter ${name};\n"
    }
    // Constructor signature
    code ++= s"  public ${className}(StructType schema"
    nestedNames.values.foreach { name =>
      code ++= s", fastproto.RowConverter ${name}"
    }
    code ++= ") {\n"
    // Assign constructor parameters and initialise writer
    code ++= "    this.schema = schema;\n"
    code ++= "    int numFields = schema.length();\n"
    code ++= "    this.writer = new UnsafeRowWriter(numFields);\n"
    nestedNames.values.foreach { name =>
      code ++= s"    this.${name} = ${name};\n"
    }
    code ++= "  }\n"
    // Helper to capitalise field names for accessor methods
    def accessorName(fd: FieldDescriptor): String = {
      val name = fd.getName
      name.substring(0, 1).toUpperCase + name.substring(1)
    }
    // Generate the typed convert method.  We intentionally omit the @Override
    // annotation here because the Scala trait's erased bridge method is what
    // Janino sees.  Adding @Override on this generic method can lead to
    // spurious errors about missing supertype methods.  The bridge method
    // defined below will carry the override annotation.
    code ++= "  public UnsafeRow convert(" + messageClass.getName + " msg) {\n"
    // Reset the writer for each conversion.  Calling reset() clears the buffer and
    // zeroOutNullBytes() clears the null bitset.  This prepares the writer for
    // writing a new row.
    code ++= "    writer.reset();\n"
    code ++= "    writer.zeroOutNullBytes();\n"
    // Generate per‑field extraction and writing logic
    descriptor.getFields.asScala.zipWithIndex.foreach { case (fd, idx) =>
      val getterName = if (fd.isRepeated) {
        s"get${accessorName(fd)}List"
      } else {
        s"get${accessorName(fd)}"
      }
      val hasMethodName = if (fd.getJavaType == FieldDescriptor.JavaType.MESSAGE && !fd.isRepeated && !(fd.getType == FieldDescriptor.Type.MESSAGE && fd.getMessageType.getOptions.hasMapEntry)) {
        // For singular message fields there is a hasX() method
        Some(s"has${accessorName(fd)}")
      } else {
        None
      }
      fd.getJavaType match {
        case FieldDescriptor.JavaType.INT =>
          if (fd.isRepeated) {
            // Repeated int32: build primitive array and write as UnsafeArrayData
            code ++= s"    java.util.List<Integer> list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    int[] arr${idx} = new int[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arr${idx}[i] = list${idx}.get(i); }\n"
            code ++= s"    ArrayData data${idx} = org.apache.spark.sql.catalyst.util.UnsafeArrayData.fromPrimitiveArray(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.LONG =>
          if (fd.isRepeated) {
            code ++= s"    java.util.List<Long> list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    long[] arr${idx} = new long[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arr${idx}[i] = list${idx}.get(i); }\n"
            code ++= s"    ArrayData data${idx} = org.apache.spark.sql.catalyst.util.UnsafeArrayData.fromPrimitiveArray(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.FLOAT =>
          if (fd.isRepeated) {
            code ++= s"    java.util.List<Float> list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    float[] arr${idx} = new float[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arr${idx}[i] = list${idx}.get(i); }\n"
            code ++= s"    ArrayData data${idx} = org.apache.spark.sql.catalyst.util.UnsafeArrayData.fromPrimitiveArray(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.DOUBLE =>
          if (fd.isRepeated) {
            code ++= s"    java.util.List<Double> list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    double[] arr${idx} = new double[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arr${idx}[i] = list${idx}.get(i); }\n"
            code ++= s"    ArrayData data${idx} = org.apache.spark.sql.catalyst.util.UnsafeArrayData.fromPrimitiveArray(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.BOOLEAN =>
          if (fd.isRepeated) {
            code ++= s"    java.util.List<Boolean> list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    Object[] arr${idx} = new Object[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arr${idx}[i] = list${idx}.get(i); }\n"
            code ++= s"    ArrayData data${idx} = new GenericArrayData(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            code ++= s"    writer.write($idx, msg.${getterName}());\n"
          }
        case FieldDescriptor.JavaType.STRING =>
          if (fd.isRepeated) {
            code ++= s"    java.util.List<String> list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    Object[] arr${idx} = new Object[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { String s = list${idx}.get(i); arr${idx}[i] = (s == null ? null : UTF8String.fromString(s)); }\n"
            code ++= s"    ArrayData data${idx} = new GenericArrayData(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            code ++= s"    String v${idx} = msg.${getterName}();\n"
            code ++= s"    if (v${idx} == null) {\n"
            code ++= s"      writer.setNullAt($idx);\n"
            code ++= s"    } else {\n"
            code ++= s"      writer.write($idx, UTF8String.fromString(v${idx}));\n"
            code ++= s"    }\n"
          }
        case FieldDescriptor.JavaType.BYTE_STRING =>
          if (fd.isRepeated) {
            code ++= s"    java.util.List<com.google.protobuf.ByteString> list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    Object[] arr${idx} = new Object[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arr${idx}[i] = list${idx}.get(i).toByteArray(); }\n"
            code ++= s"    ArrayData data${idx} = new GenericArrayData(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            code ++= s"    com.google.protobuf.ByteString b${idx} = msg.${getterName}();\n"
            code ++= s"    if (b${idx} == null) {\n"
            code ++= s"      writer.setNullAt($idx);\n"
            code ++= s"    } else {\n"
            code ++= s"      writer.write($idx, b${idx}.toByteArray());\n"
            code ++= s"    }\n"
          }
        case FieldDescriptor.JavaType.ENUM =>
          if (fd.isRepeated) {
            code ++= s"    java.util.List<? extends com.google.protobuf.ProtocolMessageEnum> list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    Object[] arr${idx} = new Object[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arr${idx}[i] = UTF8String.fromString(list${idx}.get(i).toString()); }\n"
            code ++= s"    ArrayData data${idx} = new GenericArrayData(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            code ++= s"    com.google.protobuf.ProtocolMessageEnum e${idx} = msg.${getterName}();\n"
            code ++= s"    if (e${idx} == null) {\n"
            code ++= s"      writer.setNullAt($idx);\n"
            code ++= s"    } else {\n"
            code ++= s"      writer.write($idx, UTF8String.fromString(e${idx}.toString()));\n"
            code ++= s"    }\n"
          }
        case FieldDescriptor.JavaType.MESSAGE =>
          if (fd.isRepeated) {
            // Repeated message: use nested converter for element type (map entries are treated as repeated message)
            val nestedName = nestedNames(fd)
            code ++= s"    java.util.List list${idx} = msg.${getterName}();\n"
            code ++= s"    int size${idx} = list${idx}.size();\n"
            code ++= s"    Object[] arr${idx} = new Object[size${idx}];\n"
            code ++= s"    for (int i = 0; i < size${idx}; i++) { arr${idx}[i] = ${nestedName}.convert((com.google.protobuf.Message) list${idx}.get(i)); }\n"
            code ++= s"    ArrayData data${idx} = new GenericArrayData(arr${idx});\n"
            code ++= s"    writer.write($idx, data${idx}, ((ArrayType) schema.apply($idx).dataType()).elementType());\n"
          } else {
            // Singular message: use nested converter, handle nullability
            val nestedName = nestedNames(fd)
            hasMethodName match {
              case Some(method) =>
                code ++= s"    if (!msg.${method}()) {\n"
                code ++= s"      writer.setNullAt($idx);\n"
                code ++= s"    } else {\n"
                code ++= s"      com.google.protobuf.Message v${idx} = (com.google.protobuf.Message) msg.${getterName}();\n"
                code ++= s"      writer.write($idx, ${nestedName}.convert(v${idx}), (StructType) schema.apply($idx).dataType());\n"
                code ++= s"    }\n"
              case None =>
                code ++= s"    com.google.protobuf.Message v${idx} = (com.google.protobuf.Message) msg.${getterName}();\n"
                code ++= s"    if (v${idx} == null) {\n"
                code ++= s"      writer.setNullAt($idx);\n"
                code ++= s"    } else {\n"
                code ++= s"      writer.write($idx, ${nestedName}.convert(v${idx}), (StructType) schema.apply($idx).dataType());\n"
                code ++= s"    }\n"
            }
          }
      }
    }
    // After all fields have been written, finalise row size and return
    // the UnsafeRow.  Calling writer.getRow() will set the total size and
    // return the row object.  We avoid directly manipulating BufferHolder.
    code ++= "    return writer.getRow();\n"
    code ++= "  }\n" // End of convert(T) method
    // Add bridge method to satisfy the generic RowConverter interface.  This
    // method simply casts the object to the expected message type and
    // delegates to the typed convert method.  It overrides the erased
    // signature defined on the Scala trait.
    code ++= "  @Override\n"
    code ++= "  public org.apache.spark.sql.catalyst.InternalRow convert(Object obj) {\n"
    code ++= s"    return this.convert((${messageClass.getName}) obj);\n"
    code ++= "  }\n"
    code ++= "}\n" // End of class

    // Compile the generated Java code using Janino
    val compiler = new SimpleCompiler()
    compiler.setParentClassLoader(this.getClass.getClassLoader)
    compiler.cook(code.toString)
    val generatedClass = compiler.getClassLoader.loadClass(className)

    // Collect nested converter instances in the order they appear in nestedNames
    val nestedInstances: Seq[RowConverter[_ <: com.google.protobuf.Message]] = nestedInfos.map(_.converter).toSeq

    // Build constructor argument list: schema and nested converters (no projection needed now)
    val constructorArgs: Array[AnyRef] = {
      val base: Seq[AnyRef] = Seq(schema)
      base ++ nestedInstances map (_.asInstanceOf[AnyRef]) toArray
    }
    val constructor = generatedClass.getConstructors.head
    constructor.newInstance(constructorArgs: _*).asInstanceOf[RowConverter[T]]
  }
}