package com.iamsmkr

import com.esotericsoftware.kryo._
import com.twitter.chill._
import org.apache.arrow.memory._
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo._

import java.nio.charset.StandardCharsets
import java.util
import scala.jdk.CollectionConverters._

object TacklingVectorsWithoutShapeless extends App {

  private val kryo = new Kryo()

  def serialise[T](value: T): Array[Byte] = kryo.toBytesWithClass(value)

  /** deserialise byte array to object
   * @param bytes byte array to de-serialise
   */
  def deserialise[T](bytes: Array[Byte]): T =
    kryo.fromBytes(bytes).asInstanceOf[T]

  private val allocator: BufferAllocator = new RootAllocator()

  private val schema: Schema =
    new Schema(List(
      new Field("ints",  FieldType.notNullable(new ArrowType.Int(32, true)), null),
      new Field("longs", FieldType.notNullable(new ArrowType.Int(64, true)), null),
      new Field("strs", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
      new Field("bools", FieldType.notNullable(ArrowType.Bool.INSTANCE), null),
      new Field("list", FieldType.notNullable(ArrowType.List.INSTANCE),
        java.util.Arrays.asList(new Field("intelems",  FieldType.notNullable(new ArrowType.Int(32, true)), null))),
      new Field("bytes", FieldType.notNullable(ArrowType.Binary.INSTANCE), null)
    ).asJava)

  private val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)

  val ints = vectorSchemaRoot.getVector("ints").asInstanceOf[IntVector]
  val longs = vectorSchemaRoot.getVector("longs").asInstanceOf[BigIntVector]
  val strs = vectorSchemaRoot.getVector("strs").asInstanceOf[VarCharVector]
  val bools = vectorSchemaRoot.getVector("bools").asInstanceOf[BitVector]
  val lists = vectorSchemaRoot.getVector("list").asInstanceOf[ListVector]
  val bytes = vectorSchemaRoot.getVector("bytes").asInstanceOf[VarBinaryVector]

  // allocate new buffers
  ints.allocateNew()
  longs.allocateNew()
  strs.allocateNew()
  bools.allocateNew()
  lists.allocateNew()
  bytes.allocateNew()

  // set values to vectors
  ints.setSafe(0, 1)
  longs.setSafe(0, 1000L)
  strs.setSafe(0, "One".getBytes)
  bools.setSafe(0, 1)

  val writer = lists.getWriter
  writer.startList()
  writer.setPosition(0)
  for (j <- 0 until 5) writer.writeInt(j)
//  writer.setValueCount(5)
  writer.endList()

  // IMPORTANT! You must use the same writer to add another list to subsequent rows in a given ListVector
  writer.startList()
  writer.setPosition(1)
  for (j <- 5 until 20) writer.writeInt(j)
//  writer.setValueCount(15)
  writer.endList()

  bytes.set(0, "Pometry".getBytes)

  // set value count
  ints.setValueCount(1)
  longs.setValueCount(1)
  strs.setValueCount(1)
  bools.setValueCount(1)
  lists.setValueCount(2)
  bytes.setValueCount(1)

  // check if values are set against a given row
  assert(ints.isSet(0) == 1)
  assert(longs.isSet(0) == 1)
  assert(strs.isSet(0) == 1)
  assert(bools.isSet(0) == 1)
  assert(lists.isSet(0) == 1)
  assert(bytes.isSet(0) == 1)

  // get values against a row
  assert(ints.get(0) == 1)
  assert(longs.get(0) == 1000L)
  assert(new String(strs.get(0), StandardCharsets.UTF_8) == "One")
  assert(bools.get(0) == 1)
  assert(lists.getObject(0).toArray sameElements Array(0, 1, 2, 3, 4))
  assert(lists.getObject(1).toArray sameElements Array(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19))
  println(lists.getObject(0).asScala.toSet.asInstanceOf[Set[Int]])
//  for (i <- 0 until lists.getValueCount)
//    println(lists.getObject(i).toArray.mkString("Array(", ", ", ")"))
  assert(new String(bytes.get(0), StandardCharsets.UTF_8) == "Pometry")

  // close resources
  ints.close()
  longs.close()
  strs.close()
  bools.close()
  lists.close()
  bytes.close()
  vectorSchemaRoot.close()
}

class KryoSerialiser {
  private val kryo: KryoPool = ScalaKryoMaker.defaultPool

  /** serialise value to byte array
   * @param value value to serialise
   */
  def serialise[T](value: T): Array[Byte] = kryo.toBytesWithClass(value)

  /** deserialise byte array to object
   * @param bytes byte array to de-serialise
   */
  def deserialise[T](bytes: Array[Byte]): T =
    kryo.fromBytes(bytes).asInstanceOf[T]
}

object KryoSerialiser {

  def apply(): KryoSerialiser =
    new KryoSerialiser()
}

private object ScalaKryoMaker extends Serializable {
  private val mutex                      = new AnyRef with Serializable // some serializable object
  @transient private var kpool: KryoPool = null

  /**
   * Return a KryoPool that uses the ScalaKryoInstantiator
   */
  def defaultPool: KryoPool =
    mutex.synchronized {
      if (null == kpool)
        kpool = KryoPool.withByteArrayOutputStream(guessThreads, new KryoInstantiator)
      kpool
    }

  private def guessThreads: Int = {
    val cores                  = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }
}
