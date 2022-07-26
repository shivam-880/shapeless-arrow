package com.iamsmkr

import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionListWriter

import java.util.concurrent.ConcurrentHashMap

package object shapelessarrow {
  val listVectorToWriter = new ConcurrentHashMap[ListVector, UnionListWriter]()
}
