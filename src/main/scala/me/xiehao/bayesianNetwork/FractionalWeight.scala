package me.xiehao.bayesianNetwork

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.expressions.Window
import com.baidu.mapdata.bayesianNetwork.Utility.percent
import me.xiehao.bayesianNetwork.UDAF.Sum

class FractionalWeight(
  val randomVariable: RandomVariable,
  val dataFrame: DataFrame
) {
  val name = randomVariable.name
  val columnNamesOfNormal = randomVariable.columnNamesOfNormal
  val unNormalColumn = randomVariable.unNormalColumn
  val sampleFractionOfConvergence = randomVariable.sampleFractionOfConvergence
  val isAllUnNormal = randomVariable.isAllUnNormal
  val namesOfUnNormal = randomVariable.namesOfUnNormal
  val columnName: String =
    if (unNormalColumn.isEmpty) FractionalWeight.buildColumnName(name)
    else unNormalColumn.get.name

  lazy val len =
    if (unNormalColumn.isEmpty) dataFrame.count()
    else dataFrame.count() * unNormalColumn.get.len
  lazy val probability: Probability = toProbability.cache

  def unCache: Unit = {
    dataFrame.unpersist()
    probability.unCache
  }

  /**
    * 计算概率
    * @return
    */
  def toProbability: Probability = {
    new Probability(
      if (unNormalColumn.isEmpty) {
        percent(
          dataFrame,
          columnNamesOfNormal.filter(_ != name),
          columnName
        )
      } else if (columnNamesOfNormal.contains(name)) {
        val sumOfUnNormalColumn = new Sum(unNormalColumn.get.len)
        val columnNameOfSum = "columnNameOfSum"
        val dataFrameWithSum =
          if (columnNamesOfNormal.length == 1) {
            dataFrame.crossJoin(
              dataFrame.agg(
                sumOfUnNormalColumn(dataFrame.col(columnName)).
                  as(columnNameOfSum)
              )
            )
          } else {
            dataFrame.withColumn(
              columnNameOfSum,
              sumOfUnNormalColumn(dataFrame.col(columnName)).over(
                Window.partitionBy(
                  columnNamesOfNormal.filter(_ != name).map(dataFrame.col): _*
                )
              )
            )
          }

        dataFrameWithSum.withColumn(
          columnName,
          UnNormalColumn.divide(
            dataFrameWithSum.col(columnName),
            dataFrameWithSum.col(columnNameOfSum)
          )
        ).drop(columnNameOfSum)
      } else {
        dataFrame.withColumn(
          columnName,
          UnNormalColumn.fractionate(
            dataFrame.col(columnName),
            unNormalColumn.get.groupBy(
              unNormalColumn.get.names.filter(_ != name)
            )
          )
        )
      },
      columnNamesOfNormal,
      unNormalColumn
    )
  }

  def take(fractionalWeight: FractionalWeight): FractionalWeight = {
    /**
      * 暂不考虑UnNormal变化的情况
      */
    val columnNameToTake = s"${columnName}ToTake"
    val dataFrameToTake = fractionalWeight.dataFrame.
      withColumnRenamed(
        columnName,
        columnNameToTake
      )
    val dataFrameAfterJoined = if (isAllUnNormal) {
      dataFrame.crossJoin(dataFrameToTake)
    } else {
      dataFrame.join(
        dataFrameToTake,
        columnNamesOfNormal,
        "left_outer"
      )
    }

    val columnNameOfTmp = s"${columnName}Tmp"
    val dataFrameAfterTaken = dataFrameAfterJoined.withColumn(
      columnNameOfTmp,
      coalesce(
        dataFrameToTake.col(columnNameToTake),
        dataFrame.col(columnName)
      )
    ).drop(dataFrame.col(columnName)).
      drop(dataFrameToTake.col(columnNameToTake)).
      withColumnRenamed(columnNameOfTmp, columnName)

    new FractionalWeight(
      randomVariable,
      dataFrameAfterTaken
    )
  }
}

object FractionalWeight {
  def buildColumnName(name: String): String =
    s"${name}FractionalWeight"
}
