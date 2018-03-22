package me.xiehao.bayesianNetwork

import org.apache.spark.sql.DataFrame

object Check {
  def skewness(
     dataFrame: DataFrame,
     fraction: Double = 0.01,
     num: Int = 100
  ): Unit = {
    val sample = dataFrame.sample(false, fraction)
    val lens = sample.rdd.
      mapPartitions(ite => Seq(ite.length).toIterator).
      collect()
    lens.sorted.reverse.take(num).foreach(println)
  }

  def flat(
    dataFrame: DataFrame,
    columnName: String,
    columnLen: Int,
    num: Int = 20
  ): Unit = {
    (0 to columnLen - 1).foldLeft(dataFrame){
      case (df, i) =>
        df.withColumn(
          s"${columnName}-${i}",
          df.col(columnName)(i)
        )
    }.drop(columnName).show(num)
  }
}
