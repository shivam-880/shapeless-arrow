//package com.iamsmkr.modelv2
//
//import org.apache.arrow.memory.RootAllocator
//import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
//
//import scala.jdk.CollectionConverters._
//
//class ArrowEncoderTest extends munit.FunSuite {
//
//  test("can encode an Iterable of Int into IntVector") {
//
//    implicit val allocator = new RootAllocator
//
//    val ints = Vector(1, 2, 3, 4)
//
//    val IntColumn(arrCol, _, count) = ArrowEncoder[Int].encode(ints)
//
//    val schema = new Schema(
//      List(
//        new Field(
//          "value",
//          new FieldType(false, new ArrowType.Int(32, true), null),
//          null
//        )
//      ).asJava
//    )
//
//    assertEquals(count, 4)
//    ints.zipWithIndex.foreach { case (t, i) =>
//      assertEquals(arrCol.get(i), t)
//    }
//  }
//
//
//  test("can encode a tuple (Int, Int) into a Struct column with _1 and _2 as field names") {
//
//    import ArrowEncoder._
//
//    implicit val allocator = new RootAllocator
//
//    val ints = Vector(1 -> 4, 2 -> 3, 3 -> 2, 4 -> 1)
//
//    val col: Column[(Int, Int)] = ArrowEncoder[(Int, Int)].encode(ints)
//
//    println(col)
//  }
//
//}
