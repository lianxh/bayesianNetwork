package me.xiehao.bayesianNetwork

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.expressions.Window
import com.baidu.mapdata.bayesianNetwork.Utility.intersection
import me.xiehao.bayesianNetwork.UDAF.Sum

class Probability(
  val dataFrame: DataFrame,
  val columnNamesOfNormal: Seq[String],
  val unNormalColumn: Option[UnNormalColumn]
) {
  val isAllUnNormal = columnNamesOfNormal.length == 0
  val columnNameOfProbability = if (unNormalColumn.isEmpty) {
    dataFrame.columns.find(
      columnName => !columnNamesOfNormal.contains(columnName)
    ).get
  } else unNormalColumn.get.name

  def cache: Probability = new Probability(
    dataFrame.cache(),
    columnNamesOfNormal,
    unNormalColumn
  )
  def unCache: Unit = dataFrame.unpersist()

  def join(probability: Probability): Probability = {
    val columnNamesToJoin: Seq[String] = intersection(
      columnNamesOfNormal,
      probability.columnNamesOfNormal
    )

    val dataFrameAfterJoined =
      if (columnNamesToJoin.length == 0)
        dataFrame.crossJoin(probability.dataFrame)
      else {
        dataFrame.join(
          probability.dataFrame,
          columnNamesToJoin
        )
      }

    val (
      dataFrameAfterMulti,
      unNormalColumnAfterMulti,
      columnNameOfProbabilityAfterMulti
    ) = if (unNormalColumn.isEmpty && probability.unNormalColumn.isEmpty) {
      val _columnNameOfProbabilityAfterMulti =
        s"${columnNameOfProbability}${probability.columnNameOfProbability}"

      (
        dataFrameAfterJoined.withColumn(
          _columnNameOfProbabilityAfterMulti,
          dataFrame.col(columnNameOfProbability) *
            probability.dataFrame.col(probability.columnNameOfProbability)
        ).drop(
          dataFrame.col(columnNameOfProbability)
        ).drop(
          probability.dataFrame.col(probability.columnNameOfProbability)
        ),
        None,
        _columnNameOfProbabilityAfterMulti
      )
    } else if (unNormalColumn.isEmpty) {
      (
        dataFrameAfterJoined.withColumn(
          probability.columnNameOfProbability,
          UnNormalColumn.multi(
            dataFrame.col(columnNameOfProbability),
            probability.dataFrame.col(
              probability.columnNameOfProbability
            )
          )
        ).drop(dataFrame.col(columnNameOfProbability)),
        probability.unNormalColumn,
        probability.columnNameOfProbability
      )
    } else if (probability.unNormalColumn.isEmpty) {
      (
        dataFrameAfterJoined.withColumn(
          columnNameOfProbability,
          UnNormalColumn.multi(
            probability.dataFrame.col(
              probability.columnNameOfProbability
            ),
            dataFrame.col(columnNameOfProbability)
          )
        ).drop(
          probability.dataFrame.col(probability.columnNameOfProbability)
        ),
        unNormalColumn,
        columnNameOfProbability
      )
    } else {
      val (unNormalColumnAfterJoined, columnOfJoinMap) =
        unNormalColumn.get.join(probability.unNormalColumn.get)

      val columnNameOfTmp = s"${unNormalColumnAfterJoined.get.name}Tmp"

      (
        dataFrameAfterJoined.withColumn(
          columnNameOfTmp,
          UnNormalColumn.join(
            dataFrame.col(columnNameOfProbability),
            probability.dataFrame.col(probability.columnNameOfProbability),
            columnOfJoinMap
          )
        ).drop(dataFrame.col(columnNameOfProbability)).
          drop(probability.dataFrame.col(probability.columnNameOfProbability)).
          withColumnRenamed(
            columnNameOfTmp,
            unNormalColumnAfterJoined.get.name
          ),
        unNormalColumnAfterJoined,
        unNormalColumnAfterJoined.get.name
      )
    }

    new Probability(
      dataFrameAfterMulti,
      dataFrameAfterMulti.columns.
        filter(_ != columnNameOfProbabilityAfterMulti),
      unNormalColumnAfterMulti
    )
  }

  private def coalesceByNormal(
    columnNamesOfNormalToCoalesce: Seq[String]
  ): Probability = {
    val columnOfSum =
      if (unNormalColumn.isEmpty) sum(columnNameOfProbability)
      else new Sum(unNormalColumn.get.len)(
        dataFrame.col(columnNameOfProbability)
      )

    val dataFrameAfterCoalesced =
      if (columnNamesOfNormalToCoalesce.length == 0) {
        dataFrame.select(columnOfSum.as(columnNameOfProbability))
      } else {
        dataFrame.groupBy(
          columnNamesOfNormalToCoalesce.map(dataFrame.col): _*
        ).agg(columnOfSum.as(columnNameOfProbability))
      }

    new Probability(
      dataFrameAfterCoalesced,
      columnNamesOfNormalToCoalesce,
      unNormalColumn
    )
  }

  private def coalesceByUnNormal(
    unNormalColumnToCoalesce: Option[UnNormalColumn]
  ): Probability = {
    val dataFrameAfterCoalesced =
      if (unNormalColumn.isEmpty) {
        dataFrame
      } else if (unNormalColumnToCoalesce.isEmpty) {
        dataFrame.withColumn(
          columnNameOfProbability,
          UnNormalColumn.aggregate(
            dataFrame.col(columnNameOfProbability)
          )
        )
      } else {
        dataFrame.withColumn(
          unNormalColumnToCoalesce.get.name,
          UnNormalColumn.coalesce(
            dataFrame.col(columnNameOfProbability),
            unNormalColumn.get.coalesce(
              unNormalColumnToCoalesce.get
            )
          )
        ).drop(dataFrame.col(columnNameOfProbability))
      }

    new Probability(
      dataFrameAfterCoalesced,
      columnNamesOfNormal,
      unNormalColumnToCoalesce
    )
  }

  def coalesce(randomVariables: Seq[RandomVariable])
  : Probability = {
    val (
      randomVariablesOfNormal,
      randomVariablesOfUnNormal
    ) = randomVariables.partition(_.innerType == RandomVariable.NORMAL)

    val unNormalColumnToCoalesce =
      if (randomVariablesOfUnNormal.length == 0) None
      else Option(new UnNormalColumn(randomVariablesOfUnNormal))
    val columnNamesOfNormalToCoalesce = randomVariablesOfNormal.map(_.name)

    coalesceByUnNormal(unNormalColumnToCoalesce).
      coalesceByNormal(columnNamesOfNormalToCoalesce)
  }

  def divideByNormal(columnNamesOfNormalToDivide: Seq[String])
  : Probability = {
    val columnNameOfSum = s"${columnNameOfProbability}Sum"

    val dataFrameWithSum = if (unNormalColumn.isEmpty) {
      dataFrame.withColumn(
        columnNameOfSum,
        sum(dataFrame.col(columnNameOfProbability)).over(
          Window.partitionBy(
            columnNamesOfNormalToDivide.map(dataFrame.col): _*
          )
        )
      )
    } else {
      val sumOfUnNormalColumn = new Sum(1)
      dataFrame.withColumn(
        columnNameOfSum,
        sumOfUnNormalColumn(
          UnNormalColumn.all(dataFrame.col(columnNameOfProbability))
        ).over(
          Window.partitionBy(
            columnNamesOfNormalToDivide.map(dataFrame.col): _*
          )
        )(0)
      )
    }

    val dataFrameAfterDivided = dataFrameWithSum.withColumn(
      columnNameOfProbability,
      if (unNormalColumn.isEmpty) {
        dataFrameWithSum.col(columnNameOfProbability) /
          dataFrameWithSum.col(columnNameOfSum)
      } else {
        UnNormalColumn.factor(
          dataFrameWithSum.col(columnNameOfProbability),
          dataFrameWithSum.col(columnNameOfSum)
        )
      }
    ).drop(dataFrameWithSum.col(columnNameOfSum))

    new Probability(
      dataFrameAfterDivided,
      columnNamesOfNormal,
      unNormalColumn
    )
  }

  def divideByUnNormal(randomVariables: Seq[RandomVariable])
  : Probability = {
    val unNormalColumnToDivide = Option(new UnNormalColumn(randomVariables))

    val columnNameOfTmp = s"${columnNameOfProbability}Tmp"
    val sumOfUnNormalColumn = new Sum(unNormalColumn.get.len)

    val dataFrameWithTmp = dataFrame.crossJoin(
      dataFrame.agg(
        sumOfUnNormalColumn(
          dataFrame.col(columnNameOfProbability)
        ).as(columnNameOfTmp)
      )
    )

    val dataFrameAfterDivided =
      if (unNormalColumn.get == unNormalColumnToDivide.get) {
        dataFrameWithTmp.withColumn(
          columnNameOfProbability,
          UnNormalColumn.divide(
            dataFrameWithTmp.col(columnNameOfProbability),
            dataFrameWithTmp.col(columnNameOfTmp)
          )
        ).drop(columnNameOfTmp)
      } else {
        dataFrameWithTmp.withColumn(
          columnNameOfProbability,
          UnNormalColumn.fractionate(
            dataFrameWithTmp.col(columnNameOfTmp),
            unNormalColumn.get.groupBy(
              unNormalColumnToDivide.get.names
            )
          )
        ).drop(columnNameOfTmp)
      }

    new Probability(
      dataFrameAfterDivided,
      columnNamesOfNormal,
      unNormalColumn
    )
  }

  /**
    * ATTENTION: only support string column
    * @param name
    * @param values
    * @return
    */
  def aggregateByNormal(name: String, values: Seq[String])
  : Probability = {

    this
  }

  /**
    * ATTENTION: only support unNormalColumn with single name
    */
  def toExpectation(
    columnNameOfExpectation: String,
    columnNameOfProbability: String
  ): DataFrame = {
    val columnNameOfTmp = s"${columnNameOfExpectation}Tmp"

    val dataFrameWithExpectation = dataFrame.withColumn(
      columnNameOfTmp,
      UnNormalColumn.expect(dataFrame.col(this.columnNameOfProbability))
    )

    dataFrameWithExpectation.withColumn(
      columnNameOfExpectation,
      dataFrameWithExpectation.col(columnNameOfTmp)(0)
    ).withColumn(
      columnNameOfProbability,
      dataFrameWithExpectation.col(columnNameOfTmp)(1)
    ).drop(columnNameOfTmp)
  }
}
