package com.iamsmkr

import com.iamsmkr.shapelessarrow._
import org.apache.arrow.memory._
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._

import scala.jdk.CollectionConverters._

object TacklingVectorsWithShapeless extends App {

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


  case class MixArrowFlightMessage(
                                    superStep: Int = 0,
                                    dstVertexId: Long = 0L,
                                    vertexId: Long = 0L,
                                    label: String = "",
                                    flag: Boolean = false
                                  )

  case class MixArrowFlightMessageVectors(
                                           superSteps: IntVector,
                                           dstVertexIds: BigIntVector,
                                           vertexIds: BigIntVector,
                                           labels: VarCharVector,
                                           flags: BitVector
                                         )

  private val vectors =
    MixArrowFlightMessageVectors(
      superSteps,
      dstVertexIds,
      vertexIds,
      labels,
      flags
    )

  private val mixMessage =
    MixArrowFlightMessage(
      900,
      2000L,
      3L,
      "One",
      true
    )

  // allocate new buffers
  AllocateNew[MixArrowFlightMessageVectors].allocateNew(vectors)

  // set values to vectors
  SetSafe[MixArrowFlightMessageVectors, MixArrowFlightMessage].setSafe(vectors, 0, mixMessage)

  // set value count
  SetValueCount[MixArrowFlightMessageVectors].setValueCount(vectors, 1)

  // check if values are set against a given row
  assert(IsSet[MixArrowFlightMessageVectors].isSet(vectors, 0).forall(_ == 1))

  // get values against a row
  val encoded = Get[MixArrowFlightMessageVectors, MixArrowFlightMessage].invokeGet(vectors, 0)
  assert(encoded == mixMessage)

  // close resources
  Close[MixArrowFlightMessageVectors].close(vectors)
  vectorSchemaRoot.close()

}
