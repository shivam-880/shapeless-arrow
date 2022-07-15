package com.iamsmkr

import org.apache.arrow.memory._
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

object TacklingVectorsWithoutShapeless extends App {

  private val allocator: BufferAllocator = new RootAllocator()

  private val schema: Schema =
    new Schema(List(
      new Field("superStep", new FieldType(false, new ArrowType.Int(32, true), null), null),
      new Field("dstVertexId", new FieldType(false, new ArrowType.Int(64, true), null), null),
      new Field("vertexId", new FieldType(false, new ArrowType.Int(64, true), null), null),
      new Field("label", new FieldType(false, new ArrowType.Utf8(), null), null),
      new Field("flag", new FieldType(false, new ArrowType.Bool(), null), null)
    ).asJava)

  private val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)

  val superSteps = vectorSchemaRoot.getVector("superStep").asInstanceOf[IntVector]
  val dstVertexIds = vectorSchemaRoot.getVector("dstVertexId").asInstanceOf[BigIntVector]
  val vertexIds = vectorSchemaRoot.getVector("vertexId").asInstanceOf[BigIntVector]
  val labels = vectorSchemaRoot.getVector("label").asInstanceOf[VarCharVector]
  val flags = vectorSchemaRoot.getVector("flag").asInstanceOf[BitVector]

  // allocate new buffers
  superSteps.allocateNew()
  dstVertexIds.allocateNew()
  vertexIds.allocateNew()
  labels.allocateNew()
  flags.allocateNew()

  // set values to vectors
  superSteps.setSafe(0, 1)
  dstVertexIds.setSafe(0, 1000L)
  vertexIds.setSafe(0, 1000L)
  labels.setSafe(0, "One".getBytes)
  flags.setSafe(0, 1)

  // set value count
  superSteps.setValueCount(1)
  dstVertexIds.setValueCount(1)
  vertexIds.setValueCount(1)
  labels.setValueCount(1)
  flags.setValueCount(1)

  // check if values are set against a given row
  assert(superSteps.isSet(0) == 1)
  assert(dstVertexIds.isSet(0) == 1)
  assert(vertexIds.isSet(0) == 1)
  assert(labels.isSet(0) == 1)
  assert(flags.isSet(0) == 1)

  // get values against a row
  assert(superSteps.get(0) == 1)
  assert(dstVertexIds.get(0) == 1000L)
  assert(vertexIds.get(0) == 1000L)
  assert(new String(labels.get(0), StandardCharsets.UTF_8) == "One")
  assert(flags.get(0) == 1)

  // close resources
  superSteps.close()
  dstVertexIds.close()
  vertexIds.close()
  labels.close()
  flags.close()
  vectorSchemaRoot.close()
}
