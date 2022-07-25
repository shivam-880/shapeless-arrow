package com.iamsmkr.shapelessarrow

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import shapeless.{::, Generic, HList, HNil}

trait Close[T] {
  def close(vector: T): Unit
}

object Close {
  def apply[T](implicit derivative: Close[T]): Close[T] =
    derivative

  def instance[T](func: T => Unit): Close[T] =
    new Close[T] {
      override def close(vector: T): Unit = func(vector)
    }

  implicit val intVectorGet: Close[IntVector] =
    Close.instance[IntVector](_.close())

  implicit val bigIntVectorGet: Close[BigIntVector] =
    Close.instance[BigIntVector](_.close())

  implicit val varCharVectorGet: Close[VarCharVector] =
    Close.instance[VarCharVector](_.close())

  implicit val bitVectorGet: Close[BitVector] =
    Close.instance[BitVector](_.close())

  implicit val listVectorGet: Close[ListVector] =
    Close.instance[ListVector](_.close())

  implicit def hNilClose: Close[HNil] = Close.instance[HNil](_ => ())

  implicit def hListClose[H, T <: HList](
                                          implicit
                                          hClose: Close[H],
                                          tClose: Close[T]
                                        ): Close[H :: T] =
    Close.instance { case h :: t =>
      hClose.close(h)
      tClose.close(t)
    }

  implicit def genericClose[A, R](
                                   implicit
                                   gen: Generic.Aux[A, R],
                                   derivative: Close[R]
                                 ): Close[A] = {
    Close.instance { adt =>
      derivative.close(gen.to(adt))
    }
  }
}
