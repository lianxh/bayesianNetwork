package me.xiehao.bayesianNetwork

import me.xiehao.bayesianNetwork.UDAF.Sum
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, rand, sum}

class RandomVariable(
  val name: String,
  val piRandomVariables: Seq[RandomVariable],
  val innerType: String,
  val defaultValues: Seq[Int],
  val partitionNum: Int,
  val sampleFractionOfConvergence: Double
) {
  val randomVariables = (this +: piRandomVariables)
  val unNormalRandomVariables = randomVariables.
    filter(_.innerType != RandomVariable.NORMAL)
  val defaultValuesLen: Int = defaultValues.length
  val isAllUnNormal: Boolean =
    randomVariables.forall(_.innerType != RandomVariable.NORMAL)

  val namesOfMissing: Seq[String] = randomVariables.
    filter(_.innerType == RandomVariable.MISSING).map(_.name)
  val namesOfUnNormal: Seq[String] = unNormalRandomVariables.map(_.name)

  val columnNamesOfNormal: Seq[String] = randomVariables.
    filter(_.innerType == RandomVariable.NORMAL).map(_.name)
  val unNormalColumn: Option[UnNormalColumn] =
    if (randomVariables.forall(_.innerType == RandomVariable.NORMAL)) None
    else Option(new UnNormalColumn(unNormalRandomVariables))

  def addPi(randomVariable: RandomVariable): RandomVariable = {
    new RandomVariable(
      name,
      piRandomVariables :+ randomVariable,
      innerType,
      defaultValues,
      partitionNum,
      sampleFractionOfConvergence
    )
  }
  /**
    * 根据dataFrame随机生成FractionalWeight
    * @param dataFrame
    * @return
    */
  def toFractionalWeight(
    dataFrame: DataFrame,
    columnNameOfDuplicate: String
  ): FractionalWeight = {
    val dataFrameWithFractionalWeight = if (unNormalColumn.isEmpty) {
      dataFrame.groupBy(
        columnNamesOfNormal.map(dataFrame.col): _*
      ).agg(
        sum(columnNameOfDuplicate).as(FractionalWeight.buildColumnName(name))
      )
    } else if (isAllUnNormal) {
      val spark = SparkSession.builder().getOrCreate()

      spark.range(1).toDF(unNormalColumn.get.name).withColumn(
        unNormalColumn.get.name,
        array(
          (0 until  unNormalColumn.get.len).
            map(_ => rand()): _*
        )
      )
    } else {
      val dataFrameWithCount =
        dataFrame.groupBy(
          columnNamesOfNormal.map(dataFrame.col): _*
        ).agg(sum(columnNameOfDuplicate).alias(columnNameOfDuplicate))

      dataFrameWithCount.withColumn(
        unNormalColumn.get.name,
        UnNormalColumn.add(
          dataFrameWithCount.col(columnNameOfDuplicate),
          array(
            (0 until  unNormalColumn.get.len).
              map(_ => rand()): _*
          )
        )
      ).drop(columnNameOfDuplicate)
    }

    val columnOfRepartition =
      if (isAllUnNormal) dataFrameWithFractionalWeight.col(unNormalColumn.get.name)
      else dataFrameWithFractionalWeight.col(name)

    new FractionalWeight(
      this,
      dataFrameWithFractionalWeight.repartition(
        partitionNum,
        columnOfRepartition
      )
    )
  }

  def toFractionalWeight(dataSet: DataSet): FractionalWeight = {
    val dataFrameWithFractionalWeight = if (unNormalColumn.isEmpty) {
      dataSet.dataFrame.groupBy(
        columnNamesOfNormal.
          map(dataSet.dataFrame.col): _*
      ).agg(
        sum(dataSet.columnNameOfDuplicate).
          as(FractionalWeight.buildColumnName(name))
      )
    } else {
      val dataFrameAfterManipulated = dataSet.randomization(unNormalColumn.get)

      val sumOfUnNormalColumn = new Sum(
        unNormalColumn.get.len
      )

      if (isAllUnNormal)
        dataFrameAfterManipulated.agg(
          sumOfUnNormalColumn(
            dataFrameAfterManipulated.col(unNormalColumn.get.name)
          ).as(unNormalColumn.get.name)
        )
      else {
        dataFrameAfterManipulated.groupBy(
          columnNamesOfNormal.map(dataFrameAfterManipulated.col): _*
        ).agg(
          sumOfUnNormalColumn(
            dataFrameAfterManipulated.col(unNormalColumn.get.name)
          ).as(unNormalColumn.get.name)
        )
      }
    }

    new FractionalWeight(
      this,
      dataFrameWithFractionalWeight.repartition(
        partitionNum,
        RandomVariable.getColumnOfRepartition(
          unNormalColumn,
          dataFrameWithFractionalWeight,
          name
        )
      )
    )
  }

  /**
    * 从本地生成FractionalWeight
    * @param spark
    * @param filePath
    * @return
    */
  def toFractionalWeight(
    spark: SparkSession,
    filePath: String,
    fraction: Double
  ): FractionalWeight = {
    val dataFrameFromFiles =
      if (fraction == 1) spark.read.load(filePath)
      else spark.read.load(filePath).sample(false, fraction)
    val columnName =
      if (unNormalColumn.isEmpty) FractionalWeight.buildColumnName(name)
      else unNormalColumn.get.name

    val dataFrameWithFractionalWeight = if (unNormalColumn.isEmpty) {
      dataFrameFromFiles.groupBy(
        columnNamesOfNormal.map(dataFrameFromFiles.col): _*
      ).agg(sum(columnName).as(columnName))
    } else {
      val sumOfUnNormalColumn = new Sum(unNormalColumn.get.len)

      if (isAllUnNormal) {
        dataFrameFromFiles.select(
          sumOfUnNormalColumn(
            dataFrameFromFiles.col(columnName)
          ).as(columnName)
        )
      } else {
        dataFrameFromFiles.groupBy(
          columnNamesOfNormal.map(dataFrameFromFiles.col): _*
        ).agg(
          sumOfUnNormalColumn(
            dataFrameFromFiles.col(columnName)
          ).as(columnName)
        )
      }
    }

    new FractionalWeight(
      this,
      dataFrameWithFractionalWeight.repartition(
        partitionNum,
        RandomVariable.getColumnOfRepartition(
          unNormalColumn,
          dataFrameWithFractionalWeight,
          name
        )
      )
    )
  }

  def toFractionalWeight(dataFrame: DataFrame): FractionalWeight = {
    val columnName =
      if (unNormalColumn.isEmpty) FractionalWeight.buildColumnName(name)
      else unNormalColumn.get.name

    val dataFrameWithFractionalWeight = if (unNormalColumn.isEmpty) {
      dataFrame.groupBy(
        columnNamesOfNormal.map(dataFrame.col): _*
      ).agg(sum(columnName).as(columnName))
    } else {
      val sumOfUnNormalColumn = new Sum(unNormalColumn.get.len)

      if (isAllUnNormal) {
        dataFrame.select(
          sumOfUnNormalColumn(
            dataFrame.col(columnName)
          ).as(columnName)
        )
      } else {
        dataFrame.groupBy(
          columnNamesOfNormal.map(dataFrame.col): _*
        ).agg(
          sumOfUnNormalColumn(
            dataFrame.col(columnName)
          ).as(columnName)
        )
      }
    }

    new FractionalWeight(
      this,
      dataFrameWithFractionalWeight.repartition(
        partitionNum,
        RandomVariable.getColumnOfRepartition(
          unNormalColumn,
          dataFrameWithFractionalWeight,
          name
        )
      )
    )
  }
}

object RandomVariable {
  val NORMAL = "normal"
  val MISSING = "missing"
  val HIDDEN = "hidden"

  private def getColumnOfRepartition(
    unNormalColumn: Option[UnNormalColumn],
    dataFrame: DataFrame,
    name: String
  ): Column = {
    if (unNormalColumn.isEmpty) dataFrame.col(name)
    else if (unNormalColumn.get.names.contains(name))
      dataFrame.col(unNormalColumn.get.name)
    else dataFrame.col(name)
  }

  def normalRandomVariable(
    name: String,
    piRandomVariables: Seq[RandomVariable] = Seq(),
    sampleFractionOfConvergence: Double = 1.0
  ): RandomVariable = new RandomVariable(
      name,
      piRandomVariables,
      RandomVariable.NORMAL,
      Seq(),
      1,
      sampleFractionOfConvergence
    )
  def missingRandomVariable(
    name: String,
    defaultValues: Seq[Int],
    piRandomVariables: Seq[RandomVariable] = Seq(),
    sampleFractionOfConvergence: Double = 1.0
  ): RandomVariable = new RandomVariable(
    name,
    piRandomVariables,
    RandomVariable.MISSING,
    defaultValues,
    1,
    sampleFractionOfConvergence
  )
  def hiddenRandomVariable(
    name: String,
    defaultValues: Seq[Int],
    piRandomVariables: Seq[RandomVariable] = Seq(),
    sampleFractionOfConvergence: Double = 1.0
  ): RandomVariable = new RandomVariable(
    name,
    piRandomVariables,
    RandomVariable.HIDDEN,
    defaultValues,
    1,
    sampleFractionOfConvergence
  )
}