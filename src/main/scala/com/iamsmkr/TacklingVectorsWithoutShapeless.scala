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
      new Field("ints",  FieldType.notNullable(new ArrowType.Int(32, true)), null),
      new Field("longs", FieldType.notNullable(new ArrowType.Int(64, true)), null),
      new Field("strs", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
      new Field("bools", FieldType.notNullable(ArrowType.Bool.INSTANCE), null),
      new Field("list", FieldType.notNullable(ArrowType.List.INSTANCE),
        util.Arrays.asList(new Field("intelems",  FieldType.notNullable(new ArrowType.Int(32, true)), null)))
    ).asJava)

  private val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)

  val ints = vectorSchemaRoot.getVector("ints").asInstanceOf[IntVector]
  val longs = vectorSchemaRoot.getVector("longs").asInstanceOf[BigIntVector]
  val strs = vectorSchemaRoot.getVector("strs").asInstanceOf[VarCharVector]
  val bools = vectorSchemaRoot.getVector("bools").asInstanceOf[BitVector]
  val lists = vectorSchemaRoot.getVector("list").asInstanceOf[ListVector]

  // allocate new buffers
  ints.allocateNew()
  longs.allocateNew()
  strs.allocateNew()
  bools.allocateNew()
  lists.allocateNew()

  // set values to vectors
  ints.setSafe(0, 1)
  longs.setSafe(0, 1000L)
  strs.setSafe(0, "One".getBytes)
  bools.setSafe(0, 1)

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
  ints.setValueCount(1)
  longs.setValueCount(1)
  strs.setValueCount(1)
  bools.setValueCount(1)
  lists.setValueCount(2)

  // check if values are set against a given row
  assert(ints.isSet(0) == 1)
  assert(longs.isSet(0) == 1)
  assert(strs.isSet(0) == 1)
  assert(bools.isSet(0) == 1)
  assert(lists.isSet(0) == 1)

  // get values against a row
  assert(ints.get(0) == 1)
  assert(longs.get(0) == 1000L)
  assert(new String(strs.get(0), StandardCharsets.UTF_8) == "One")
  assert(bools.get(0) == 1)
  assert(lists.getObject(0).toArray sameElements Array(0, 1, 2, 3, 4))
  assert(lists.getObject(1).toArray sameElements Array(5, 6, 7, 8, 9))
//  for (i <- 0 until lists.getValueCount)
//    println(lists.getObject(i).toArray.mkString("Array(", ", ", ")"))

  // close resources
  ints.close()
  longs.close()
  strs.close()
  bools.close()
  lists.close()
  vectorSchemaRoot.close()
}
