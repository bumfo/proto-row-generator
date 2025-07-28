package example

import com.google.protobuf.{Field, Type}
import fastproto.ProtoToRowGenerator
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DevInterface, Row, SparkSession}

import java.io.LineNumberReader
import java.util

object Demo {
  // ----- helper to build a Field in a single call --------------------------
  private def f(
                 name: String,
                 number: Int,
                 kind: Field.Kind = Field.Kind.TYPE_STRING,
                 card: Field.Cardinality = Field.Cardinality.CARDINALITY_OPTIONAL
               ): Field =
    Field
      .newBuilder()
      .setName(name)
      .setNumber(number)
      .setKind(kind)
      .setCardinality(card)
      .build()

  def main(args: Array[String]): Unit = {

    // // ----- build an Enum -------------------------------
    // val phoneTypeEnum = com.google.protobuf.Enum.newBuilder()
    //   .setName("PhoneType")
    //   .addEnumvalue(
    //     com.google.protobuf.EnumValue
    //       .newBuilder()
    //       .setName("MOBILE")
    //       .setNumber(0))
    //   .addEnumvalue(
    //     com.google.protobuf.EnumValue
    //       .newBuilder()
    //       .setName("WORK")
    //       .setNumber(1))
    //   .build()

    // ----- build the Type (i.e. schema) -----------------
    val personSchema: Type = Type
      .newBuilder()
      .setName("tutorial.Person")
      .addFields(f("id", 1, Field.Kind.TYPE_INT32))
      .addFields(f("name", 2))
      .addFields(f("email", 3))
      .addFields(f("phones", 4,
        kind = Field.Kind.TYPE_MESSAGE,
        card = Field.Cardinality.CARDINALITY_REPEATED)
        .toBuilder
        .setTypeUrl("type.googleapis.com/tutorial.Phone") // refer to another message
        .build())
      // .addEnumType(phoneTypeEnum)
      .build()

    // com.google.protobuf.Field

    // println(personSchema)

    val conv = ProtoToRowGenerator.generateConverter(Type.getDescriptor, classOf[Type])
    val row = conv.convert(personSchema)

    println(s"$row")

    val spark = SparkSession.builder()
      .appName("Runtime CodeGen Demo")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Seq(row))
    val df = DevInterface.internalCreateDataFrame(spark, rdd, conv.schema)

    df.select(explode(col("fields"))).select("col.*").show()
  }
}
