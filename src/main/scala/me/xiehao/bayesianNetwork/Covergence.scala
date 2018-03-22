package me.xiehao.bayesianNetwork

import scala.math.max
import org.apache.spark.sql.functions.{sum, abs, when}

class Convergence(
  val differences: Seq[(String, Boolean, Double, Double)],
  /**
    * 收敛系数描绘的是两数相差的百分数以内则判断收敛，
    * 例如收敛系数为 0.2，数 100 和 81 相差20%以内，
    * 此时可判断为收敛
    */
  val coefficientOfConvergence: Double
) {
  val isConverged: Boolean = differences.forall{
    case (_, converged, _, _) => converged
  }

  private def diff(
    fractionalWeight: FractionalWeight,
    lastFractionalWeight: FractionalWeight
  ): Double = {
    val sampleFractionOfConvergence = fractionalWeight.sampleFractionOfConvergence
    val dataFrame = if (sampleFractionOfConvergence == 1.0) {
      fractionalWeight.dataFrame
    } else {
      fractionalWeight.dataFrame.sample(false, sampleFractionOfConvergence)
    }

    val columnName =
      if (fractionalWeight.unNormalColumn.isEmpty)
        FractionalWeight.buildColumnName(
          fractionalWeight.name
        )
      else fractionalWeight.unNormalColumn.get.name
    val lastColumnName = s"last${columnName}"
    val lastDataFrame = lastFractionalWeight.dataFrame.
      withColumnRenamed(
        columnName,
        lastColumnName
      )

    val dataFrameAfterJoined =
      if (fractionalWeight.isAllUnNormal)
        dataFrame.crossJoin(lastDataFrame)
      else {
        dataFrame.join(
          lastDataFrame,
          fractionalWeight.columnNamesOfNormal
        )
      }

    val columnNameOfDifference = "columnNameOfDifference"

    val dataFrameWithDifference = if (fractionalWeight.unNormalColumn.isEmpty) {
      dataFrameAfterJoined.withColumn(
        columnNameOfDifference,
        abs(dataFrame.col(columnName) - lastDataFrame.col(lastColumnName))
        /*when(
          dataFrame.col(columnName) > lastDataFrame.col(lastColumnName),
          (dataFrame.col(columnName) - lastDataFrame.col(lastColumnName)) /
            dataFrame.col(columnName)
        ).when(
          lastDataFrame.col(lastColumnName) > dataFrame.col(columnName),
          (lastDataFrame.col(lastColumnName) - dataFrame.col(columnName)) /
            lastDataFrame.col(lastColumnName)
        ).otherwise(0.0)*/
      ).select(sum(columnNameOfDifference))
    } else {
      dataFrameAfterJoined.withColumn(
        columnNameOfDifference,
        UnNormalColumn.diff(
          dataFrame.col(columnName),
          lastDataFrame.col(lastColumnName)
        )
      ).select(sum(columnNameOfDifference))
    }

    dataFrameWithDifference.first().getAs[Double](0)
  }

  def compare(model: Model, lastModel: Model): Convergence = {
    var isStop: Boolean = false

    new Convergence(
      differences.map{
        case (name, converged, lastDifference, lastPercent) => {
          if (converged || isStop) (name, converged, lastDifference, lastPercent)
          else {
            val fractionalWeight = model.getFractionalWeightByName(name)

            val difference: Double = diff(
              fractionalWeight,
              lastModel.getFractionalWeightByName(name)
            )

            if (difference == 0) (name, true, 0.0, lastPercent)
            else if (difference == lastDifference)
              (name, true, difference, lastPercent)
            /*else if (
              difference <
                (
                  coefficientOfConvergence * fractionalWeight.len *
                    fractionalWeight.sampleFractionOfConvergence
                )
            ) (name, true, difference, lastPercent)*/
            else {
              val percent = scala.math.abs(difference - lastDifference) /
                max(difference, lastDifference)

              if (percent == 1) (name, false, difference, 0.0)
              else if (percent < coefficientOfConvergence)
                (name, true, difference, percent)
              else {
                val differenceOfPercent= scala.math.abs(percent - lastPercent) /
                  max(percent, lastPercent)

                if (differenceOfPercent == 1) (name, false, difference, percent)
                else if (differenceOfPercent < coefficientOfConvergence)
                  (name, true, difference, percent)
                else {
                  //isStop = true
                  (name, false, difference, percent)
                }
              }
            }
          }
        }
      },
      coefficientOfConvergence
    )
  }
}

object Convergence {
  def apply(
    randomVariables: Seq[RandomVariable],
    coefficientOfConvergence: Double
  ): Convergence = {
    new Convergence(
      randomVariables.map(
        randomVariable => (randomVariable.name, false, 0.0, 0.0)
      ),
      coefficientOfConvergence
    )
  }
}
