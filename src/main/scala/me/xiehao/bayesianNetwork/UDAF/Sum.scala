package me.xiehao.bayesianNetwork.UDAF

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class Sum(val len: Int) extends UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    new StructType().add("values", ArrayType(DoubleType))

  override def bufferSchema: StructType =
    new StructType().add("sums", ArrayType(DoubleType))

  override def dataType: DataType = ArrayType(DoubleType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer)
  : Unit = buffer(0) = Seq.fill(len)(0.0)

  override def update(
    buffer: MutableAggregationBuffer,
    input: Row
  ): Unit = {
    val values = input.getAs[Seq[Double]](0)
    buffer(0) = (0 until values.length).
      foldLeft(buffer.getAs[Seq[Double]](0)){
        case (v, index) =>
          v.updated(index, v(index) + values(index))
      }
  }

  override def merge(
    buffer1: MutableAggregationBuffer,
    buffer2: Row
  ): Unit = {
    val values = buffer2.getAs[Seq[Double]](0)
    buffer1(0) = (0 until values.length).foldLeft(
      buffer1.getAs[Seq[Double]](0).toVector
    ){
      case (v, index) =>
        v.updated(index, v(index) + values(index))
    }
  }

  override def evaluate(buffer: Row): Any =
    buffer.getAs[Seq[Double]](0)
}
