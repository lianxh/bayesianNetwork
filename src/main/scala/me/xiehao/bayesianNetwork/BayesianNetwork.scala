/**
  *
  * @author xh92@hotmail.com
  *
  */

package me.xiehao.bayesianNetwork

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, lit}

class BayesianNetwork(
  val randomVariables: Seq[RandomVariable] = Seq(),
  val columnNameOfDuplicate: String = "dataFrameDuplicates",
  val columnNameOfFractionalWeight: String = "dataFrameFractionalWeight"
) {
  val hasUnNormalVariable: Boolean =
    !randomVariables.forall(_.innerType == RandomVariable.NORMAL)
  val namesOfNormalRandomVariables: Seq[String] =
    randomVariables.filter(_.innerType == RandomVariable.NORMAL).map(_.name)
  val namesOfNoHiddenRandomVariables: Seq[String] =
    randomVariables.filter(_.innerType != RandomVariable.HIDDEN).map(_.name)

  def apply(name: String): Option[RandomVariable] =
    randomVariables.find(_.name == name)

  /**
    * 添加RandomVariable
    * @param name
    * @param piNames
    * @param innerType
    * @param defaultValues
    * @return
    */
  def addRandomVariable(
    name: String,
    piNames: Seq[String] = Seq(),
    innerType: String = RandomVariable.NORMAL,
    defaultValues: Seq[Int] = Seq(),
    partitionNum: Int = 1,
    sampleFractionOfConvergence: Double = 1.0
  ): BayesianNetwork = {
    val piRandomVariables = piNames.map(apply).map(_.get)
    new BayesianNetwork(
      new RandomVariable(
        name,
        piRandomVariables,
        innerType,
        defaultValues,
        partitionNum,
        sampleFractionOfConvergence
      ) +: randomVariables
    )
  }

  def fit(
    dataFrame: DataFrame,
    manipulate: Manipulate,
    checkpointDir: String = null,
    lastModel: Model = null,
    maxTurn: Int = 10,
    namesToBroadcast: Seq[String] = Seq(),
    debug: Boolean = false,
    checkConvergence: Boolean = true,
    coefficientOfConvergence: Double = 0.05,
    showConvergence: Boolean = false
  ): Model = {
    /**
      * 初始化并缓存训练集
      */
    val dataSet = new DataSet(
      dataFrame.groupBy(
        dataFrame.columns.map(dataFrame.col): _*
      ).agg(count(lit(1.0)).as(columnNameOfDuplicate)).
        withColumn(columnNameOfFractionalWeight, lit(1.0)),
      None,
      columnNameOfDuplicate,
      columnNameOfFractionalWeight
    ).checkpointAndCache(checkpointDir)

    /**
      * EM迭代
      */
    val (modelAfterEM, _) = (1 to maxTurn).foldLeft((
      new Model(
        randomVariables.map(
          _.toFractionalWeight(
            dataSet.dataFrame,
            columnNameOfDuplicate
          )
        )
      ).take(lastModel).checkpointAndCache(checkpointDir),
      Convergence(
        randomVariables,
        coefficientOfConvergence
      )
    )){
      case ((model, convergence), turn) => {
        if (convergence.isConverged) (model, convergence)
        else {
          println(s"第${turn}轮")

          /**
            * 计算新model
            */
          val nextModel = manipulate(
            model,
            dataSet,
            namesToBroadcast,
            debug,
            checkpointDir
          ).reset(randomVariables)

          /**
            * 比较model是否收敛
            */
          val nextConvergence =
            if (checkConvergence && (turn != maxTurn))
              convergence.compare(nextModel, model)
            else convergence

          if (showConvergence) nextConvergence.differences.foreach(println)

          model.unCache

          (nextModel, nextConvergence)
        }
      }
    }

    dataSet.unCache

    modelAfterEM
  }

  def load(
    spark: SparkSession,
    filePath: String,
    names: Seq[String] = randomVariables.map(_.name)
  ): Model = {
    new Model(
      randomVariables.filter(
        randomVariable =>
          names.contains(randomVariable.name)
      ).map(randomVariable => {
          randomVariable.toFractionalWeight(
            spark.read.load(s"${filePath}/${randomVariable.name}")
          )
        })
    )
  }
}
