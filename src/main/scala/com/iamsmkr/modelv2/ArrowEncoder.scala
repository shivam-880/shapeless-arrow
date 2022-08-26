package com.iamsmkr.modelv2

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{FieldVector, IntVector}

import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._

@implicitNotFound("Could not find an implicit for ArrowEncoder[${T}]")
trait ArrowEncoder[T] {

  def encode(ts: Iterable[T], name: Option[String] = None)(implicit
      A: BufferAllocator
  ): Column[T]

}

object ArrowEncoder {
  def apply[T](implicit A: ArrowEncoder[T]) = A

  implicit val intArrowEncoder: ArrowEncoder[Int] = new ArrowEncoder[Int] {

    override def encode(
        ts: Iterable[Int],
        name: Option[String] = None
    )(implicit A: BufferAllocator): Column[Int] = {
      val field = new Field(
        name.getOrElse("value"),
        new FieldType(false, new ArrowType.Int(32, true), null),
        null
      )
      val vec = new IntVector(field, A)

      var i = 0

      val iter = ts.iterator
      while (iter.hasNext) {
        vec.setSafe(i, iter.next())
        i += 1
      }

      vec.setValueCount(i)

      IntColumn(vec, field, i)
    }

  }

  implicit def tuple2ArrowEncoder[A, B](
      implicit
      @implicitNotFound("Cannot find ArrowEncoder of ${A} or ${B}")
      A: ArrowEncoder[A],
      B: ArrowEncoder[B]): ArrowEncoder[(A, B)] =

    new ArrowEncoder[(A, B)] {

      override def encode(ts: Iterable[(A, B)], name: Option[String] = None)(
          implicit BA: BufferAllocator
      ): Column[(A, B)] = {

        val as = ts.map(_._1)
        val bs = ts.map(_._2)

        val colA: Column[A] = A.encode(as, Some("_1"))
        val colB: Column[B] = B.encode(bs, Some("_2"))

        assert(colA.valueCount == colB.valueCount)

        val schema: Schema =
          new Schema(
            List(
              new Field("_1", colA.fieldType, null),
              new Field("_2", colA.fieldType, null)
            ).asJava
          )

        val vec: StructVector = new StructVector(
          name.getOrElse("value"),
          BA,
          new FieldType(false, new ArrowType.Struct, null),
          null
        )
        // VERY IMPORTANT APPARENTLY
        vec.setValueCount(colA.valueCount)
        (0 until colA.valueCount).foreach(vec.setIndexDefined)

        val destColumn = vec.addOrGet(
          "_1",
          colA.fieldType,
          colA.classOf
        )
        val srcColumnA: FieldVector = colA.fieldColumn
        srcColumnA.makeTransferPair(destColumn).transfer()
        colA.fieldColumn.close()

        val colBDest = vec.addOrGet(
          "_2",
          colB.fieldType,
          colB.classOf
        )
        val srcColumnB: FieldVector = colB.fieldColumn
        srcColumnB.makeTransferPair(colBDest).transfer()
        colB.fieldColumn.close()

        StructColumn(vec, schema, colA.valueCount)

      }

    }
}
