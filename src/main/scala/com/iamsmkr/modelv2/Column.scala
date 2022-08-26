package com.iamsmkr.modelv2

import org.apache.arrow.vector.{FieldVector, IntVector}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import scala.jdk.CollectionConverters._

sealed trait Column[T] {
  def schema: Schema
  def fieldColumn: FieldVector
  def classOf: Class[_ <: FieldVector]
  def valueCount: Int
  def fieldType: FieldType
}

case class IntColumn(c: IntVector, field: Field, valueCount: Int)
    extends Column[Int] {

  override def fieldColumn: FieldVector = c

  def classOf: Class[_ <: FieldVector] = c.getClass

  override def schema: Schema = new Schema(List(field).asJava)

  override def fieldType: FieldType = field.getFieldType
}

case class StructColumn[T](c: StructVector, schema: Schema, valueCount: Int)
    extends Column[T] {

  override def fieldColumn: FieldVector = c

  def classOf: Class[_ <: FieldVector] = c.getClass

  override def fieldType: FieldType =
    new FieldType(false, new ArrowType.Struct, null)
}
