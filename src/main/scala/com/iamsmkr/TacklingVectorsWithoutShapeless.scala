package com.iamsmkr

import org.apache.arrow.memory._
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo._

import java.nio.charset.StandardCharsets
import java.util
import scala.jdk.CollectionConverters._

object TacklingVectorsWithoutShapeless extends App {

  private val allocator: BufferAllocator = new RootAllocator()

  private val schema: Schema =
    new Schema(List(
      new Field("superStep",  FieldType.notNullable(new ArrowType.Int(32, true)), null),
      new Field("dstVertexId", FieldType.notNullable(new ArrowType.Int(64, true)), null),
      new Field("vertexId", FieldType.notNullable(new ArrowType.Int(64, true)), null),
      new Field("label", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
      new Field("flag", FieldType.notNullable(ArrowType.Bool.INSTANCE), null),
      new Field("list", FieldType.notNullable(ArrowType.List.INSTANCE),
        util.Arrays.asList(new Field("elem1",  FieldType.notNullable(new ArrowType.Int(32, true)), null)))
    ).asJava)

  private val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)

  val superSteps = vectorSchemaRoot.getVector("superStep").asInstanceOf[IntVector]
  val dstVertexIds = vectorSchemaRoot.getVector("dstVertexId").asInstanceOf[BigIntVector]
  val vertexIds = vectorSchemaRoot.getVector("vertexId").asInstanceOf[BigIntVector]
  val labels = vectorSchemaRoot.getVector("label").asInstanceOf[VarCharVector]
  val flags = vectorSchemaRoot.getVector("flag").asInstanceOf[BitVector]
  val lists = vectorSchemaRoot.getVector("list").asInstanceOf[ListVector]

  // allocate new buffers
  superSteps.allocateNew()
  dstVertexIds.allocateNew()
  vertexIds.allocateNew()
  labels.allocateNew()
  flags.allocateNew()
  lists.allocateNew()

  // set values to vectors
  superSteps.setSafe(0, 1)
  dstVertexIds.setSafe(0, 1000L)
  vertexIds.setSafe(0, 1000L)
  labels.setSafe(0, "One".getBytes)
  flags.setSafe(0, 1)

  val writer = lists.getWriter
  writer.startList()
  writer.setPosition(0)
  for (j <- 0 until 5) writer.writeInt(j)
  writer.setValueCount(5)
  writer.endList()

  writer.startList()
  writer.setPosition(1)
  for (j <- 5 until 10) writer.writeInt(j)
  writer.setValueCount(5)
  writer.endList()

  // set value count
  superSteps.setValueCount(1)
  dstVertexIds.setValueCount(1)
  vertexIds.setValueCount(1)
  labels.setValueCount(1)
  flags.setValueCount(1)
  lists.setValueCount(2)

  // check if values are set against a given row
  assert(superSteps.isSet(0) == 1)
  assert(dstVertexIds.isSet(0) == 1)
  assert(vertexIds.isSet(0) == 1)
  assert(labels.isSet(0) == 1)
  assert(flags.isSet(0) == 1)
  assert(lists.isSet(0) == 1)

  // get values against a row
  assert(superSteps.get(0) == 1)
  assert(dstVertexIds.get(0) == 1000L)
  assert(vertexIds.get(0) == 1000L)
  assert(new String(labels.get(0), StandardCharsets.UTF_8) == "One")
  assert(flags.get(0) == 1)
  assert(lists.getObject(0).toArray sameElements Array(0, 1, 2, 3, 4))
  assert(lists.getObject(1).toArray sameElements Array(5, 6, 7, 8, 9))
//  for (i <- 0 until lists.getValueCount)
//    println(lists.getObject(i).toArray.mkString("Array(", ", ", ")"))

  // close resources
  superSteps.close()
  dstVertexIds.close()
  vertexIds.close()
  labels.close()
  flags.close()
  lists.close()
  vectorSchemaRoot.close()
}
