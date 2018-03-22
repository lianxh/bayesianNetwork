package me.xiehao.bayesianNetwork.UDAF

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * ATTENTION: only support string key
  * @param keys
  * @param len
  */
class aggregate(
  val keys: Seq[String],
  override val len: Int
) extends Sum(len) {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    new StructType().add("key", StringType).
      add("values", ArrayType(DoubleType))

  override def update(buffer: MutableAggregationBuffer, input: Row)
  : Unit = {
    val key = input.getAs[String](0)
    buffer(0) =
      if (keys.contains(key)) {
        val values = input.getAs[Seq[Double]](1)
        (0 until values.length).
          foldLeft(buffer.getAs[Seq[Double]](0)){
            case (v, index) =>
              v.updated(index, v(index) + values(index))
          }
      } else buffer(0)
  }
}
