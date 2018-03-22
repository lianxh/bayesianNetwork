package me.xiehao.bayesianNetwork

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, lit, when}

class DataSet(
  val dataFrame: DataFrame,
  val unNormalColumn: Option[UnNormalColumn],
  val columnNameOfDuplicate: String,
  val columnNameOfFractionalWeight: String
) {
  val columnNamesOfNormal: Seq[String] = if (unNormalColumn.isEmpty) {
    dataFrame.columns.filter(_ != columnNameOfFractionalWeight).
      filter(_ != columnNameOfDuplicate)
  } else {
    dataFrame.columns.filter(_ != columnNameOfFractionalWeight).
      filter(_ != columnNameOfDuplicate).
      filter(_ != unNormalColumn.get.name)
  }

  def checkpointAndCache(checkpointDir: String): DataSet = new DataSet(
    if (checkpointDir == null) dataFrame.cache()
    else {
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.setCheckpointDir(checkpointDir)

      dataFrame.checkpoint().cache()
    },
    unNormalColumn,
    columnNameOfDuplicate,
    columnNameOfFractionalWeight
  )
  def unCache: Unit = dataFrame.unpersist()

  def calculate(
    fractionalWeight: FractionalWeight,
    broadcastHint: Boolean,
    debug: Boolean
  ): DataSet = {
    val probability = fractionalWeight.probability

    val checkSeq =
      if (fractionalWeight.unNormalColumn.isEmpty) Seq()
      else fractionalWeight.unNormalColumn.get.check(this)

    if (debug) {
      println(s"RandomVariable:${fractionalWeight.name}")
      println("dataFrame:")
      dataFrame.show()
      println("probability:")
      probability.dataFrame.show()
      println("checkSeq:")
      checkSeq.foreach(println)
    }

    /**
      * join阶段
      */
    val dataFrameAfterJoined = if (fractionalWeight.isAllUnNormal) {
      dataFrame.crossJoin(
        if (broadcastHint) broadcast(probability.dataFrame)
        else probability.dataFrame
      )
    } else {
      val namesPairs: Seq[(String, String)] =
        fractionalWeight.columnNamesOfNormal.map(
          columnName => (columnName, s"${columnName}Ext")
        )
      val dataFrameAfterRename =
        namesPairs.foldLeft(probability.dataFrame){
          case (pb, (columnName, columnNameOfExt)) =>
            pb.withColumnRenamed(
              columnName,
              columnNameOfExt
            )
        }

      namesPairs.foldLeft(
        dataFrame.join(
          if (broadcastHint) broadcast(dataFrameAfterRename)
          else dataFrameAfterRename,
          namesPairs.map{
            case (columnName, columnNameOfExt) =>
              dataFrame.col(columnName) ===
                dataFrameAfterRename.col(columnNameOfExt)
          }.reduceLeft(_ && _)
        )
      ){
        case (dataFrameToDrop, (_, columnNameExt)) =>
          dataFrameToDrop.drop(dataFrameAfterRename.col(columnNameExt))
      }
    }

    if (debug) {
      println("dataFrameAfterJoined:")
      dataFrameAfterJoined.show()
    }

    /**
      * merge阶段
      */
    val dataFrameAfterMerged = if (checkSeq.length == 0) {
      dataFrameAfterJoined
    } else {
      val isEqual: Column = lit(
        checkSeq.length == fractionalWeight.namesOfUnNormal.length
      )

      val columnNameOfTmp = s"${
        fractionalWeight.unNormalColumn.get.name
      }Tmp"

      checkSeq.foldLeft(dataFrameAfterJoined){
        case (dataFrameToMerge, (columnName, indexesMap)) =>
          dataFrameToMerge.withColumn(
            columnNameOfTmp,
            when(
              dataFrame.col(columnName).isNotNull,
              UnNormalColumn.check(
                dataFrame.col(columnName),
                probability.dataFrame.col(fractionalWeight.unNormalColumn.get.name),
                indexesMap,
                isEqual
              )
            ).otherwise(
              probability.dataFrame.col(fractionalWeight.unNormalColumn.get.name)
            )
          ).drop(columnName).
            drop(fractionalWeight.unNormalColumn.get.name).
            withColumnRenamed(
              columnNameOfTmp,
              fractionalWeight.unNormalColumn.get.name
            )
      }
    }

    if (debug) {
      println("dataFrameAfterMerged:")
      dataFrameAfterMerged.show()
    }

    /**
      * calculate阶段
      */
    if (unNormalColumn.isEmpty && fractionalWeight.unNormalColumn.isEmpty) {

      if (debug) println("Calculate: All Empty")

      new DataSet(
        dataFrameAfterMerged.withColumn(
          columnNameOfFractionalWeight,
          dataFrame.col(columnNameOfFractionalWeight) *
            probability.dataFrame.col(
              FractionalWeight.buildColumnName(
                fractionalWeight.name
              )
            )
        ).drop(FractionalWeight.buildColumnName(fractionalWeight.name)),
        None,
        columnNameOfDuplicate,
        columnNameOfFractionalWeight
      )
    } else if (fractionalWeight.unNormalColumn.isEmpty) {

      if (debug) println("Calculate: fractionalWeight.unNormalColumn Empty")

      new DataSet(
        dataFrameAfterMerged.withColumn(
          unNormalColumn.get.name,
          UnNormalColumn.multi(
            probability.dataFrame.col(
              FractionalWeight.buildColumnName(
                fractionalWeight.name
              )
            ),
            dataFrame.col(unNormalColumn.get.name)
          )
        ).drop(FractionalWeight.buildColumnName(fractionalWeight.name)),
        unNormalColumn,
        columnNameOfDuplicate,
        columnNameOfFractionalWeight
      )
    } else if (unNormalColumn.isEmpty) {

      if (debug) println("Calculate: unNormalColumn Empty")

      new DataSet(
        dataFrameAfterMerged.withColumn(
          fractionalWeight.unNormalColumn.get.name,
          UnNormalColumn.multi(
            dataFrame.col(columnNameOfFractionalWeight),
            dataFrameAfterMerged.col(fractionalWeight.unNormalColumn.get.name)
          )
        ).drop(columnNameOfFractionalWeight),
        fractionalWeight.unNormalColumn,
        columnNameOfDuplicate,
        columnNameOfFractionalWeight
      )
    } else {

      if (debug) println("Calculate: No Empty")

      val (unNormalColumnAfterJoined, columnOfJoinMap) =
        unNormalColumn.get.join(fractionalWeight.unNormalColumn.get)

      val columnNameOfTmp = s"${unNormalColumnAfterJoined.get.name}Tmp"

      new DataSet(
        dataFrameAfterMerged.withColumn(
          columnNameOfTmp,
          UnNormalColumn.join(
            dataFrame.col(unNormalColumn.get.name),
            if (checkSeq.length == 0)
              probability.dataFrame.col(fractionalWeight.unNormalColumn.get.name)
            else
              dataFrameAfterMerged.col(fractionalWeight.unNormalColumn.get.name),
            columnOfJoinMap
          )
        ).drop(dataFrame.col(unNormalColumn.get.name)).
          drop(
            if (checkSeq.length == 0)
              probability.dataFrame.col(fractionalWeight.unNormalColumn.get.name)
            else
              dataFrameAfterMerged.col(fractionalWeight.unNormalColumn.get.name)
          ).withColumnRenamed(columnNameOfTmp, unNormalColumnAfterJoined.get.name),
        unNormalColumnAfterJoined,
        columnNameOfDuplicate,
        columnNameOfFractionalWeight
      )
    }
  }

  def coalesceByUnNormalRandomVariables(
    randomVariables: Seq[RandomVariable],
    debug: Boolean
  ): DataSet = {
    if (debug) {
      println("namesToCoalesce:")
      randomVariables.map(_.name).foreach(println)
      println("beforeCoalesced:")
      dataFrame.show()
    }

    val unNormalColumnToCoalesce =
      if (randomVariables.length == 0) None
      else Option(new UnNormalColumn(randomVariables))

    val dataFrameAfterCoalesced =
      if (unNormalColumnToCoalesce.isEmpty) {
        dataFrame.withColumn(
          columnNameOfFractionalWeight,
          UnNormalColumn.aggregate(
            dataFrame.col(columnNameOfFractionalWeight)
          )
        )
      } else if (
        unNormalColumn.get.name == unNormalColumnToCoalesce.get.name
      ) dataFrame
      else {
        dataFrame.withColumn(
          unNormalColumnToCoalesce.get.name,
          UnNormalColumn.coalesce(
            dataFrame.col(unNormalColumn.get.name),
            unNormalColumn.get.coalesce(
              unNormalColumnToCoalesce.get
            )
          )
        ).drop(dataFrame.col(unNormalColumn.get.name))
      }

    if (debug) {
      println("AfterCoalesced:")
      dataFrameAfterCoalesced.show()
    }

    new DataSet(
      dataFrameAfterCoalesced,
      unNormalColumnToCoalesce,
      columnNameOfDuplicate,
      columnNameOfFractionalWeight
    )
  }

  def randomization(unNormalColumnToReset: UnNormalColumn): DataFrame = {
    dataFrame.withColumn(
      unNormalColumnToReset.name,
      UnNormalColumn.reset(
        UnNormalColumn.multi(
          dataFrame.col(columnNameOfDuplicate),
          UnNormalColumn.percent(
            dataFrame.col(
              unNormalColumn.get.name
            )
          )
        ),
        unNormalColumn.get.reset(unNormalColumnToReset)
      )
    ).drop(dataFrame.col(unNormalColumn.get.name))
  }
}
