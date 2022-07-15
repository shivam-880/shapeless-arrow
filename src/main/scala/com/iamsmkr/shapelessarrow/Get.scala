package com.iamsmkr.shapelessarrow

import org.apache.arrow.vector._
import shapeless.{::, Generic, HList, HNil}

import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag

trait Get[T, R] {
  def get(vector: T, row: Int, value: R): R

  def invokeGet(vector: T, row: Int)(implicit ct: ClassTag[R]): R = {
    def newDefault[A](implicit t: reflect.ClassTag[A]): A = {
      import reflect.runtime.{universe => ru, currentMirror => cm}

      val clazz = cm.classSymbol(t.runtimeClass)
      val mod = clazz.companion.asModule
      val im = cm.reflect(cm.reflectModule(mod).instance)
      val ts = im.symbol.typeSignature
      val mApply = ts.member(ru.TermName("apply")).asMethod
      val syms = mApply.paramLists.flatten
      val args = syms.zipWithIndex.map { case (p, i) =>
        val mDef = ts.member(ru.TermName(s"apply$$default$$${i + 1}")).asMethod
        im.reflectMethod(mDef)()
      }
      im.reflectMethod(mApply)(args: _*).asInstanceOf[A]
    }

    get(vector, row, newDefault[R])
  }
}

object Get {
  def apply[T, R](implicit derivative: Get[T, R]): Get[T, R] =
    derivative

  def instance[T, R](func: (T, Int, R) => R): Get[T, R] =
    new Get[T, R] {
      override def get(vector: T, row: Int, value: R): R =
        func(vector, row, value)
    }

  implicit val intVectorGet: Get[IntVector, Int] =
    Get.instance[IntVector, Int] { case (vector, row, value) => vector.get(row) }

  implicit val bigIntVectorGet: Get[BigIntVector, Long] =
    Get.instance[BigIntVector, Long] { case (vector, row, value) => vector.get(row) }

  implicit val varCharVectorGet: Get[VarCharVector, String] =
    Get.instance[VarCharVector, String] {
      case (vector, row, value) =>
        new String(vector.get(row), StandardCharsets.UTF_8)
    }

  implicit val bitVectorGet: Get[BitVector, Boolean] =
    Get.instance[BitVector, Boolean] { case (vector, row, value) => if (vector.get(row) == 1) true else false }

  implicit def hNilGet: Get[HNil, HNil] =
    Get.instance[HNil, HNil] { case (vector, row, value) => HNil }

  implicit def hListGet[H, T <: HList, P, Q <: HList](
                                                       implicit
                                                       hGet: Get[H, P],
                                                       tGet: Get[T, Q]
                                                     ): Get[H :: T, P :: Q] =
    Get.instance { case (h :: t, row, p :: q) =>
      hGet.get(h, row, p) :: tGet.get(t, row, q)
    }

  implicit def genericGet[A, ARepr <: HList, B, BRepr <: HList](
                                                                 implicit
                                                                 genA: Generic.Aux[A, ARepr],
                                                                 genB: Generic.Aux[B, BRepr],
                                                                 derivative: Get[ARepr, BRepr]
                                                               ): Get[A, B] = {
    Get.instance { case (vectors, row, values) =>
      genB.from(derivative.get(genA.to(vectors), row, genB.to(values)))
    }
  }
}
