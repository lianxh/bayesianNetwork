package me.xiehao.bayesianNetwork

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{abs, row_number, sum}
import org.apache.spark.sql.expressions.Window

object Utility {
  /**
    * 计算占比
    * @param dataFrame
    * @param overColumnNames
    * @param columnName
    * @return
    */
  def percent(
    dataFrame: DataFrame,
    overColumnNames: Seq[String],
    columnName: String
  ): DataFrame = {
    val sumColumnName = s"${columnName}Sum"
    val sumDataFrame = dataFrame.
      withColumn(
        sumColumnName,
        sum(columnName).over(
          Window.partitionBy(overColumnNames.map(dataFrame.col): _*)
        )
      )

    sumDataFrame.withColumn(
      columnName,
      sumDataFrame.col(columnName) / sumDataFrame.col(sumColumnName)
    ).drop(sumColumnName)
  }

  /**
    * 计算期望
    * @param dataFrame
    * @param overColumnNames
    * @param columnName
    * @param columnNameOfPb
    * @return
    */
  def expectation(
    dataFrame: DataFrame,
    overColumnNames: Seq[String],
    columnName: String,
    columnNameOfPb: String
  ): DataFrame = {
    val overColumns = overColumnNames.map(dataFrame.col)
    val expColumnName = s"${columnName}Exp"
    val expDataFrame = dataFrame.
      withColumn(
        expColumnName,
        (dataFrame.col(columnName) * dataFrame.col(columnNameOfPb)).
          over(
            Window.partitionBy(overColumns: _*)
          )
      )

    val diffColumnName = s"${columnName}Diff"
    val diffDataFrame = expDataFrame.
      withColumn(
        diffColumnName,
        abs(
          expDataFrame.col(expColumnName) -
          expDataFrame.col(columnName)
        )
      )

    val orderColumnName = s"${columnName}Order"
    diffDataFrame.withColumn(
      orderColumnName,
      row_number.over(
        Window.partitionBy(overColumns: _*).orderBy(
          diffDataFrame.col(diffColumnName).asc
        )
      )
    ).where(diffDataFrame.col(diffColumnName) === 1).
      drop(
        expColumnName,
        diffColumnName,
        orderColumnName
      )
  }

  /**
    * 计算交集
    * @param s1
    * @param s2
    * @tparam T
    * @return
    */
  def intersection[T](s1: Seq[T], s2: Seq[T]): Seq[T] =
    s1.filter(elem => s2.contains(elem))

  /**
    * 计算并集
    * @param s1
    * @param s2
    * @tparam T
    * @return
    */
  def union[T](s1: Seq[T], s2: Seq[T]): Seq[T] =
    (s1 ++ s2).distinct

  def matrix(columns: Seq[Seq[Int]])
  : Seq[Seq[Int]] = columns.foldLeft(Seq(Seq[Int]())){
    case (rows, column) => {
      rows.flatMap(row => column.map(value => row :+ value))
    }
  }
  def index(bases: Seq[(Int, Int)], row: Seq[Int])
  : Int = bases.foldLeft(0){
    case (index, (i, base)) => {
      val value = row(i)
      index + value * base
    }
  }
}
